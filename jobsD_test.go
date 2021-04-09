package jobsd

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpleframeworks/logc"
	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func setupLogging(level logrus.Level) logc.Logger {
	log := logrus.New()
	log.SetLevel(level)
	return logc.NewLogrus(log)
}

func setupDB(logger logc.Logger) *gorm.DB {
	dbToUse := strings.ToLower(strings.TrimSpace(os.Getenv("JOBSD_DB")))

	if dbToUse == "" || dbToUse == "sqllite" {
		return setupSQLLite(logger)
	}
	if dbToUse == "postgres" {
		return setupPostgreSQL(logger)
	}
	if dbToUse == "mysql" {
		return setupMySQL(logger)
	}

	return nil
}

func setupSQLLite(logger logc.Logger) *gorm.DB {
	db, err0 := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		// Logger: logc.NewGormLogger(logger),
	})

	sqlDB, err := db.DB()
	checkError(err)

	// SQLLite does not work well with concurrent connections
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetMaxOpenConns(1)

	checkError(err0)
	return db
}

func setupPostgreSQL(logger logc.Logger) *gorm.DB {
	host := os.Getenv("JOBSD_PG_HOST")
	port := os.Getenv("JOBSD_PG_PORT")
	dbname := os.Getenv("JOBSD_PG_DB")
	user := os.Getenv("JOBSD_PG_USER")
	password := os.Getenv("JOBSD_PG_PASSWORD")
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable", host, user, password, dbname, port)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logc.NewGormLogger(logger),
	})
	checkError(err)

	return db
}

func setupMySQL(logger logc.Logger) *gorm.DB {
	host := os.Getenv("JOBSD_MY_HOST")
	port := os.Getenv("JOBSD_MY_PORT")
	dbname := os.Getenv("JOBSD_MY_DB")
	user := os.Getenv("JOBSD_MY_USER")
	password := os.Getenv("JOBSD_MY_PASSWORD")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", user, password, host, port, dbname)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logc.NewGormLogger(logger),
	})
	checkError(err)

	return db
}

func ciDuration(localVal, ciVal time.Duration) time.Duration {
	ci := os.Getenv("JOBSD_CI_TEST") != ""
	if ci {
		return ciVal
	}
	return localVal
}

func TestJobsDRun(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestJobsDRun" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("a Job that increments a counter")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		atomic.AddUint32(&runCounter, 1)
		defer wait.Done()
		return nil
	}

	t.Given("we register the job to the JobsD instance")
	jd.RegisterJob(jobName, jobFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we run the job once")
	startTime := time.Now()
	wait.Add(1)
	_, err := jd.CreateRun(jobName).Run()
	t.Assert.NoError(err)

	t.Then("the job should have run once")
	t.WaitTimeout(&wait, ciDuration(500*time.Millisecond, 10*time.Second))
	t.Assert.Equal(1, int(runCounter))

	t.Then("the job run should have completed within 1 second")
	t.Assert.WithinDuration(time.Now(), startTime, 1*time.Second)
	// If it takes longer it means it ran after being recovered from the DB

	// Cleanup
	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestJobsDRunMulti(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestJobsDRunMulti" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("a Job that increments a counter")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		atomic.AddUint32(&runCounter, 1)
		defer wait.Done()
		return nil
	}

	t.Given("we register the job to the JobsD instance")
	jd.RegisterJob(jobName, jobFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	runNum := 20
	t.Whenf("we run the job %d times", runNum)
	startTime := time.Now()
	for i := 0; i < runNum; i++ {
		wait.Add(1)
		_, errR := jd.CreateRun(jobName).Run()
		t.Assert.NoError(errR)
	}

	t.Thenf("the job should have run %d times", runNum)
	t.WaitTimeout(&wait, ciDuration(5000*time.Millisecond, 10*time.Second))
	t.Assert.Equal(runNum, int(runCounter))

	t.Then("the all job runs should have completed within 3 second")
	t.Assert.WithinDuration(time.Now(), startTime, 3*time.Second)
	// If it takes longer it means it ran after being recovered from the DB

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestQueuedRunErrRetry(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestQueuedRunErrRetry" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("a Job that errors out on the first run")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func(i int) (rtn error) {
		defer wait.Done()
		currentCount := atomic.AddUint32(&runCounter, 1)
		if currentCount == 1 {
			rtn = errors.New("an error")
		}
		return rtn
	}

	t.Given("we register the job and set it to retry on error once")
	jd.RegisterJob(jobName, jobFunc).RetryErrorLimit(1)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we run the job")
	wait.Add(2)
	startTime := time.Now()
	_, err := jd.CreateRun(jobName, 0).Run()
	t.Assert.NoError(err)

	t.Then("the job should have run twice")
	t.WaitTimeout(&wait, ciDuration(500*time.Millisecond, 10*time.Second))
	t.Assert.Equal(2, int(runCounter))

	t.Then("the job runs should have completed within 1 second")
	t.Assert.WithinDuration(time.Now(), startTime, 1*time.Second)
	// If it takes longer it means it ran after being recovered from the DB

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestQueuedRunTimeoutRetry(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestQueuedRunTimeoutRetry" // Must be unique otherwise tests may collide

	retryCheck := 50 * time.Millisecond
	retryTimeout := 200 * time.Millisecond
	firstRunTime := 1000 * time.Millisecond

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("the instance checks for jobs that timeout every " + retryCheck.String())
	jd.TimeoutCheck(retryCheck)

	t.Given("a Job that times out on the first run")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		currentCount := atomic.AddUint32(&runCounter, 1)
		if currentCount == 1 {
			<-time.After(firstRunTime)
		}
		return nil
	}

	t.Given("we register the job and set it to retry once on a " + retryTimeout.String() + " timeout")
	jd.RegisterJob(jobName, jobFunc).RetriesTimeoutLimit(1).RunTimeout(retryTimeout)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we run the job")
	wait.Add(2)
	_, err := jd.CreateRun(jobName).Run()
	t.Assert.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, ciDuration(5*time.Second, 10*time.Second))

	t.Then("the job should have run twice")
	t.Assert.Equal(2, int(runCounter))

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestJobsDScheduledRun(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestJobsDScheduledRun" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("a Job that increments a counter")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		atomic.AddUint32(&runCounter, 1)
		defer wait.Done()
		return nil
	}

	timer := 150 * time.Millisecond
	t.Given("a Schedule that runs every " + timer.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(timer)
	}

	t.Given("we register the job to the JobsD instance")
	jd.RegisterJob(jobName, jobFunc)

	t.Given("we register the schedule to the JobsD instance")
	jd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we run the job with the schedule")
	wait.Add(1)
	startTime := time.Now()
	_, errR := jd.CreateRun(jobName).Schedule("theSchedule").Limit(1).Run()
	t.Assert.NoError(errR)

	t.Then("the job should have run")
	t.WaitTimeout(&wait, ciDuration(500*timer, 10*time.Second))
	finishTime := time.Now()

	t.Then("the job should have run within " + timer.String() + " with a tolerance of 150ms")
	t.Assert.WithinDuration(finishTime, startTime.Add(timer), time.Duration(150*time.Millisecond))

	t.Then("the job should only run once even if we wait for another 500ms")
	<-time.After(500 * time.Millisecond)
	t.Assert.Equal(1, int(runCounter))

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestJobsDScheduledRunRecurrent(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestJobsDScheduledRunRecurrent" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("a Job that increments a counter")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		atomic.AddUint32(&runCounter, 1)
		defer wait.Done()
		return nil
	}

	timer := 1000 * time.Millisecond
	t.Given("a Schedule that runs every " + timer.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(timer)
	}

	t.Given("we register the job to the JobsD instance")
	jd.RegisterJob(jobName, jobFunc)

	t.Given("we register the schedule to the JobsD instance")
	jd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we run the job with the schedule")
	wait.Add(3)
	startTime := time.Now()
	_, errR := jd.CreateRun(jobName).Schedule("theSchedule").Limit(3).Run()
	t.Assert.NoError(errR)

	t.Then("the job should have run 3 times")
	t.WaitTimeout(&wait, ciDuration(5000*time.Millisecond, 10*time.Second))
	finishTime := time.Now()

	t.Then("the job should only run three times even if we wait for another 500ms")
	<-time.After(500 * time.Millisecond)
	t.Assert.Equal(3, int(runCounter))

	timerFor3 := time.Duration(3 * timer)
	tolerance := time.Duration(1000 * time.Millisecond)
	t.Thenf("3 jobs should have run within %s with a tolerance of %s", timerFor3.String(), tolerance.String())
	t.Assert.WithinDuration(finishTime, startTime.Add(timerFor3), tolerance)

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestJobsDScheduledRunMulti(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.DebugLevel)
	db := setupDB(logger)
	jobName := "TestJobsDScheduledRunMulti" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	workers := 1
	jd := New(db).Logger(logger).WorkerNum(workers)

	t.Given("a Job that increments a counter")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		atomic.AddUint32(&runCounter, 1)
		defer wait.Done()
		return nil
	}

	timer := 200 * time.Millisecond
	t.Given("a Schedule that runs every " + timer.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(timer)
	}

	t.Given("we register the job to the JobsD instance")
	jd.RegisterJob(jobName, jobFunc)

	t.Given("we register the schedule to the JobsD instance")
	jd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	runNum := 10
	times := 2
	t.Whenf("we schedule the job to run %d times with a limit of %d runs", runNum, times)
	startTime := time.Now()
	for i := 0; i < runNum; i++ {
		wait.Add(2)
		_, errR := jd.CreateRun(jobName).Schedule("theSchedule").Limit(times).Run()
		t.Assert.NoError(errR)
	}

	t.Thenf("the job should have run %d times", runNum*times)
	t.WaitTimeout(&wait, ciDuration(5*time.Second, 20*time.Second))
	finishTime := time.Now()
	t.Assert.Equal(runNum*times, int(runCounter))

	expectedRunTime := timer * time.Duration(times+runNum/workers)
	tolerance := 100 * time.Millisecond * time.Duration(times+runNum/workers)
	t.Thenf("the jobs should have run within %s with a tolerance of %s", expectedRunTime.String(), tolerance.String())
	t.Assert.WithinDuration(startTime.Add(expectedRunTime), finishTime, tolerance)

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestJobsDClusterWorkSharing(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestJobsDClusterWorkSharing" // Must be unique otherwise tests may collide

	nodes := 20
	t.Given("a " + strconv.Itoa(nodes) + " JobsD instance cluster with one worker each")
	jdInstances := []*JobsD{}
	for i := 0; i < nodes; i++ {
		jdInstances = append(jdInstances, New(db).Logger(logger).WorkerNum(1).PollInterval(100*time.Millisecond))
	}

	runTime := 150 * time.Millisecond
	t.Given("a job that increments a counter and takes " + runTime.String())
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		atomic.AddUint32(&runCounter, 1)
		<-time.After(runTime)
		return nil
	}

	t.Given("the cluster can run the job")
	for _, qInst := range jdInstances {
		qInst.RegisterJob(jobName, jobFunc)
	}

	t.When("we bring up the JobsD instances")
	for _, qInst := range jdInstances {
		t.Assert.NoError(qInst.Up())
	}

	runs := nodes * 3
	runIDs := []int64{}
	t.When("we run a job " + strconv.Itoa(runs) + " times from the first instance in the cluster")
	for i := 0; i < runs; i++ {
		wait.Add(1)
		runID, err := jdInstances[0].CreateRun(jobName).Run()
		t.Assert.NoError(err)
		runIDs = append(runIDs, runID)
	}

	t.Then("the job should have run " + strconv.Itoa(runs) + " times")
	t.WaitTimeout(&wait, ciDuration(10*runTime, 10*time.Second))
	t.Assert.Equal(runs, int(runCounter))

	t.Then("the job runs should have been distributed across the cluster and run")
	nonLocalRuns := 0
	for _, runID := range runIDs {
		theState := jdInstances[0].GetRunState(runID)
		if theState.RunStartedBy != nil && *theState.RunStartedBy != jdInstances[0].instance.ID {
			nonLocalRuns++
		}
	}
	t.Assert.Greater(nonLocalRuns, 1)

	for _, qInst := range jdInstances {
		t.Assert.NoError(qInst.Down())
	}
}
func ExampleJobsD() {
	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "ExampleJobsD" // Must be unique otherwise tests may collide

	wait := make(chan struct{})

	job1Func := func(txt string) error {
		defer func() { wait <- struct{}{} }()
		fmt.Printf("Hello %s!", txt)
		return nil
	}
	schedule1Func := func(now time.Time) time.Time {
		return now.Add(500 * time.Millisecond)
	}

	jd := New(db).Logger(logger)

	jd.RegisterJob(jobName, job1Func)
	jd.RegisterSchedule("schedule1", schedule1Func)

	err0 := jd.Up()
	checkError(err0)

	_, err1 := jd.CreateRun(jobName, "World").Schedule("schedule1").Limit(1).Run()
	checkError(err1)

	<-wait
	err2 := jd.Down()
	checkError(err2)

	// Output: Hello World!
}

func BenchmarkQueueRun(b *testing.B) {

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "BenchmarkQueueRun" // Must be unique otherwise tests may collide

	wait := sync.WaitGroup{}
	out := []int{}

	job1Func := func(i int) error {
		defer wait.Done()
		out = append(out, i)
		return nil
	}

	jd := New(db).Logger(logger)

	jd.RegisterJob(jobName, job1Func)

	err0 := jd.Up()
	checkError(err0)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		wait.Add(1)
		_, err1 := jd.CreateRun(jobName, n).Run()
		checkError(err1)
	}

	wait.Wait()
	b.StopTimer()

	err2 := jd.Down()
	checkError(err2)
}
