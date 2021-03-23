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
	"gorm.io/driver/sqlserver"
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
	if dbToUse == "mssql" {
		return setupMSSQL(logger)
	}

	return nil
}

func setupSQLLite(logger logc.Logger) *gorm.DB {
	db, err0 := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		Logger: logc.NewGormLogger(logger),
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

func setupMSSQL(logger logc.Logger) *gorm.DB {
	host := os.Getenv("JOBSD_MS_HOST")
	port := os.Getenv("JOBSD_MS_PORT")
	dbname := os.Getenv("JOBSD_MS_DB")
	user := os.Getenv("JOBSD_MS_USER")
	password := os.Getenv("JOBSD_MS_PASSWORD")
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s", user, password, host, port, dbname)

	db, err := gorm.Open(sqlserver.Open(dsn), &gorm.Config{
		Logger: logc.NewGormLogger(logger),
	})
	checkError(err)

	return db
}

func TestJobsDJobRun(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestJobsDJobRun" // Must be unique otherwise tests may collide

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
	t.NoError(jd.Up())

	t.When("we run the job once")
	startTime := time.Now()
	wait.Add(1)
	_, err := jd.CreateRun(jobName).Run()
	t.NoError(err)

	t.Then("the job should have run once")
	t.WaitTimeout(&wait, 500*time.Millisecond)
	t.Equal(1, int(runCounter))

	t.Then("the job run should have completed within 1 second")
	t.WithinDuration(time.Now(), startTime, 1*time.Second)
	// If it takes longer it means it ran after being recovered from the DB

	// Cleanup
	t.NoError(jd.Down()) // Cleanup
}

func TestJobsDJobRunMulti(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestJobsDJobRunMulti" // Must be unique otherwise tests may collide

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
	t.NoError(jd.Up())

	runNum := 20
	t.Whenf("we run the job %d times", runNum)
	startTime := time.Now()
	for i := 0; i < runNum; i++ {
		wait.Add(1)
		_, errR := jd.CreateRun(jobName).Run()
		t.NoError(errR)
	}

	t.Thenf("the job should have run %d times", runNum)
	t.WaitTimeout(&wait, 5000*time.Millisecond)
	t.Equal(runNum, int(runCounter))

	t.Then("the all job runs should have completed within 3 second")
	t.WithinDuration(time.Now(), startTime, 3*time.Second)
	// If it takes longer it means it ran after being recovered from the DB

	t.NoError(jd.Down()) // Cleanup
}

func TestQueuedJobRunErrRetry(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestQueuedJobRunErrRetry" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("a Job that errors out on the first run")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func(i int) (rtn error) {
		defer wait.Done()
		if runCounter == 0 {
			rtn = errors.New("an error")
		}
		atomic.AddUint32(&runCounter, 1)
		return rtn
	}

	t.Given("we register the job and set it to retry on error once")
	jd.RegisterJob(jobName, jobFunc).RetryErrorLimit(1)

	t.When("we bring up the JobsD instance")
	t.NoError(jd.Up())

	t.When("we run the job")
	wait.Add(2)
	startTime := time.Now()
	_, err := jd.CreateRun(jobName, 0).Run()
	t.NoError(err)

	t.Then("the job should have run twice")
	t.WaitTimeout(&wait, 500*time.Millisecond)
	t.Equal(2, int(runCounter))

	t.Then("the job runs should have completed within 1 second")
	t.WithinDuration(time.Now(), startTime, 1*time.Second)
	// If it takes longer it means it ran after being recovered from the DB

	t.NoError(jd.Down()) // Cleanup
}

func TestQueuedJobRunTimeoutRetry(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestQueuedJobRunTimeoutRetry" // Must be unique otherwise tests may collide

	retryCheck := 50 * time.Millisecond
	retryTimeout := 200 * time.Millisecond
	firstJobRunTime := 1000 * time.Millisecond

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("the instance checks for jobs that timeout every " + retryCheck.String())
	jd.JobRetryTimeoutCheck(retryCheck)

	t.Given("a Job that times out on the first run")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		if atomic.LoadUint32(&runCounter) == 0 {
			<-time.After(firstJobRunTime)
		}
		atomic.AddUint32(&runCounter, 1)
		return nil
	}

	t.Given("we register the job and set it to retry once on a " + retryTimeout.String() + " timeout")
	jd.RegisterJob(jobName, jobFunc).RetryTimeoutLimit(1).RetryTimeout(retryTimeout)

	t.When("we bring up the JobsD instance")
	t.NoError(jd.Up())

	t.When("we run the job")
	wait.Add(2)
	_, err := jd.CreateRun(jobName).Run()
	t.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, 5*time.Second)

	t.Then("the job should have run twice")
	t.Equal(2, int(runCounter))

	t.NoError(jd.Down()) // Cleanup
}

func TestJobsDScheduledJobRun(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestJobsDScheduledJobRun" // Must be unique otherwise tests may collide

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
	t.NoError(jd.Up())

	t.When("we run the job with the schedule")
	wait.Add(1)
	startTime := time.Now()
	_, errR := jd.CreateRun(jobName).Schedule("theSchedule").Limit(1).Run()
	t.NoError(errR)

	t.Then("the job should have run")
	t.WaitTimeout(&wait, 500*timer)
	finishTime := time.Now()

	t.Then("the job should have run within " + timer.String() + " with a tolerance of 150ms")
	t.WithinDuration(finishTime, startTime.Add(timer), time.Duration(150*time.Millisecond))

	t.Then("the job should only run once even if we wait for another 500ms")
	<-time.After(500 * time.Millisecond)
	t.Equal(1, int(runCounter))

	t.NoError(jd.Down()) // Cleanup
}

func TestJobsDScheduledJobRunRecurrent(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestJobsDScheduledJobRunRecurrent" // Must be unique otherwise tests may collide

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
	t.NoError(jd.Up())

	t.When("we run the job with the schedule")
	wait.Add(3)
	startTime := time.Now()
	_, errR := jd.CreateRun(jobName).Schedule("theSchedule").Limit(3).Run()
	t.NoError(errR)

	t.Then("the job should have run 3 times")
	t.WaitTimeout(&wait, 5000*time.Millisecond)
	finishTime := time.Now()

	t.Then("the job should only run three times even if we wait for another 500ms")
	<-time.After(500 * time.Millisecond)
	t.Equal(3, int(runCounter))

	timerFor3 := time.Duration(3 * timer)
	tolerance := time.Duration(1000 * time.Millisecond)
	t.Thenf("3 jobs should have run within %s with a tolerance of %s", timerFor3.String(), tolerance.String())
	t.WithinDuration(finishTime, startTime.Add(timerFor3), tolerance)

	t.NoError(jd.Down()) // Cleanup
}

func TestJobsDScheduledJobRunMulti(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestJobsDScheduledJobRunMulti" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger).WorkerNum(1)

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
	t.NoError(jd.Up())

	t.When("we schedule the job to run 10 times with a limit of 2 runs")
	startTime := time.Now()
	for i := 0; i < 10; i++ {
		wait.Add(2)
		_, errR := jd.CreateRun(jobName).Schedule("theSchedule").Limit(2).Run()
		t.NoError(errR)
	}

	t.Then("the job should have run 20 times")
	t.WaitTimeout(&wait, 2000*time.Millisecond)
	finishTime := time.Now()
	t.Equal(20, int(runCounter))

	expectedRunTime := 2 * timer
	t.Then("the jobs should have run within " + expectedRunTime.String() + " with a tolerance of 300ms")
	t.WithinDuration(finishTime, startTime.Add(expectedRunTime), time.Duration(300*time.Millisecond))

	t.NoError(jd.Down()) // Cleanup
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
		jdInstances = append(jdInstances, New(db).Logger(logger).WorkerNum(1).JobPollInterval(100*time.Millisecond))
	}

	runTime := 150 * time.Millisecond
	t.Given("a job that increments a counter and takes " + runTime.String())
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		<-time.After(runTime)
		atomic.AddUint32(&runCounter, 1)
		return nil
	}

	t.Given("the cluster can run the job")
	for _, qInst := range jdInstances {
		qInst.RegisterJob(jobName, jobFunc)
	}

	t.When("we bring up the JobsD instances")
	for _, qInst := range jdInstances {
		t.NoError(qInst.Up())
	}

	runs := nodes * 3
	runIDs := []int64{}
	t.When("we run a job " + strconv.Itoa(runs) + " times from the first instance in the cluster")
	for i := 0; i < runs; i++ {
		wait.Add(1)
		runID, err := jdInstances[0].CreateRun(jobName).Run()
		t.NoError(err)
		runIDs = append(runIDs, runID)
	}

	t.Then("the job should have run " + strconv.Itoa(runs) + " times")
	t.WaitTimeout(&wait, 10*runTime)
	t.Equal(runs, int(runCounter))

	t.Then("the job runs should have been distributed across the cluster and run")
	nonLocalJobRuns := 0
	for _, runID := range runIDs {
		theState := jdInstances[0].GetJobRunState(runID)
		if theState.RunStartedBy != nil && *theState.RunStartedBy != jdInstances[0].instance.ID {
			nonLocalJobRuns++
		}
	}
	t.Greater(nonLocalJobRuns, 1)

	for _, qInst := range jdInstances {
		t.NoError(qInst.Down())
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

func BenchmarkQueueJobRun(b *testing.B) {

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "BenchmarkQueueJobRun" // Must be unique otherwise tests may collide

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
