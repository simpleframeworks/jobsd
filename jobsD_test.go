package jobsd

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/simpleframeworks/logc"
	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
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

func TestJobsDJobRun(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger)

	t.Given("a Job that increments a counter")
	wait := sync.WaitGroup{}
	runNum := 0
	jobFunc := func() error {
		runNum++
		defer wait.Done()
		return nil
	}

	t.Given("we register the job to the JobsD instance")
	qd.RegisterJob("theJob", jobFunc)

	t.When("we bring up the JobsD instance")
	t.NoError(qd.Up())

	t.When("we run the job once")
	startTime := time.Now()
	wait.Add(1)
	_, err := qd.CreateRun("theJob").Run()
	t.NoError(err)

	t.Then("the job should have run once")
	t.WaitTimeout(&wait, 500*time.Millisecond)
	t.Equal(1, runNum)

	t.Then("the job run should have completed within 1 second")
	t.WithinDuration(time.Now(), startTime, 1*time.Second)
	// If it takes longer it means it ran after being recovered from the DB
}

func TestJobsDJobRunMulti(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger)

	t.Given("a Job that increments a counter")
	wait := sync.WaitGroup{}
	runNum := 0
	jobFunc := func() error {
		runNum++
		defer wait.Done()
		return nil
	}

	t.Given("we register the job to the JobsD instance")
	qd.RegisterJob("theJob", jobFunc)

	t.When("we bring up the JobsD instance")
	t.NoError(qd.Up())

	t.When("we run the job 20 times")
	startTime := time.Now()
	for i := 0; i < 20; i++ {
		wait.Add(1)
		_, errR := qd.CreateRun("theJob").Run()
		t.NoError(errR)
	}

	t.Then("the job should have run 20 times")
	t.WaitTimeout(&wait, 500*time.Millisecond)
	t.Equal(20, runNum)

	t.Then("the all job runs should have completed within 3 second")
	t.WithinDuration(time.Now(), startTime, 3*time.Second)
	// If it takes longer it means it ran after being recovered from the DB
}

func TestQueuedJobRunErrRetry(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger)

	t.Given("a Job that errors out on the first run")
	wait := sync.WaitGroup{}
	runNum := 0
	jobFunc := func(i int) (rtn error) {
		defer wait.Done()
		if runNum == 0 {
			rtn = errors.New("an error")
		}
		runNum++
		return rtn
	}

	t.Given("we register the job and set it to retry on error once")
	qd.RegisterJob("theJob", jobFunc).RetryOnError(1)

	t.When("we bring up the JobsD instance")
	t.NoError(qd.Up())

	t.When("we run the job")
	wait.Add(2)
	startTime := time.Now()
	_, err := qd.CreateRun("theJob", 0).Run()
	t.NoError(err)

	t.Then("the job should have run twice")
	t.WaitTimeout(&wait, 500*time.Millisecond)
	t.Equal(2, runNum)

	t.Then("the job runs should have completed within 1 second")
	t.WithinDuration(time.Now(), startTime, 1*time.Second)
	// If it takes longer it means it ran after being recovered from the DB

	t.NoError(qd.Down())
}

func TestQueuedJobRunTimeoutRetry(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	retryCheck := 50 * time.Millisecond
	retryTimeout := 150 * time.Millisecond

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger)

	t.Given("the instance checks for jobs that timeout every " + retryCheck.String())
	qd.JobRetryTimeoutCheck(50 * time.Millisecond)

	t.Given("a Job that times out on the first run")
	wait := sync.WaitGroup{}
	runNum := 0
	jobFunc := func() error {
		defer wait.Done()
		if runNum == 0 {
			<-time.After(2 * retryTimeout)
		}
		runNum++
		return nil
	}

	t.Given("we register the job and set it to retry once on a " + retryCheck.String() + " timeout")
	qd.RegisterJob("theJob", jobFunc).RetryOnTimeout(1).RetryTimeout(retryTimeout)

	t.When("we bring up the JobsD instance")
	t.NoError(qd.Up())

	t.When("we run the job")
	wait.Add(2)
	_, err := qd.CreateRun("theJob").Run()
	t.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, 10*retryTimeout)

	t.Then("the job should have run twice")
	t.Equal(2, runNum)

	t.NoError(qd.Down())
}

func TestJobsDScheduledJobRun(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger)

	t.Given("a Job that increments a counter")
	wait := sync.WaitGroup{}
	runNum := 0
	jobFunc := func() error {
		runNum++
		defer wait.Done()
		return nil
	}

	timer := 150 * time.Millisecond
	t.Given("a Schedule that runs every " + timer.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(timer)
	}

	t.Given("we register the job to the JobsD instance")
	qd.RegisterJob("theJob", jobFunc)

	t.Given("we register the schedule to the JobsD instance")
	qd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.NoError(qd.Up())

	t.When("we run the job with the schedule")
	wait.Add(1)
	startTime := time.Now()
	_, errR := qd.CreateRun("theJob").Schedule("theSchedule").Limit(1).Run()
	t.NoError(errR)

	t.Then("the job should have run")
	t.WaitTimeout(&wait, 500*timer)
	finishTime := time.Now()

	t.Then("the job should have run within " + timer.String() + " with a tolerance of 150ms")
	t.WithinDuration(finishTime, startTime.Add(timer), time.Duration(150*time.Millisecond))

	t.Then("the job should only run once even if we wait for another 500ms")
	<-time.After(500 * time.Millisecond)
	t.Equal(1, runNum)

	t.NoError(qd.Down())
}

func TestJobsDScheduledJobRunRecurrent(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger)

	t.Given("a Job that increments a counter")
	wait := sync.WaitGroup{}
	runNum := 0
	jobFunc := func() error {
		runNum++
		defer wait.Done()
		return nil
	}

	timer := 200 * time.Millisecond
	t.Given("a Schedule that runs every " + timer.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(timer)
	}

	t.Given("we register the job to the JobsD instance")
	qd.RegisterJob("theJob", jobFunc)

	t.Given("we register the schedule to the JobsD instance")
	qd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.NoError(qd.Up())

	t.When("we run the job with the schedule")
	wait.Add(3)
	startTime := time.Now()
	_, errR := qd.CreateRun("theJob").Schedule("theSchedule").Limit(3).Run()
	t.NoError(errR)

	t.Then("the job should have run 3 times")
	t.WaitTimeout(&wait, 100000*time.Millisecond)
	finishTime := time.Now()

	t.Then("the job should only run three times even if we wait for another 500ms")
	<-time.After(500 * time.Millisecond)
	t.Equal(3, runNum)

	timerFor3 := time.Duration(2 * timer) //There are only two scheduled wait periods 1-2 and 2-3
	t.Then("3 jobs should have run within " + timerFor3.String() + " with a tolerance of 150ms")
	t.WithinDuration(finishTime, startTime.Add(timerFor3), time.Duration(150*time.Millisecond))

	t.NoError(qd.Down())
}

func TestJobsDScheduledJobRunMulti(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger).WorkerNum(1)

	t.Given("a Job that increments a counter")
	wait := sync.WaitGroup{}
	runNum := 0
	jobFunc := func() error {
		runNum++
		defer wait.Done()
		return nil
	}

	timer := 200 * time.Millisecond
	t.Given("a Schedule that runs every " + timer.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(timer)
	}

	t.Given("we register the job to the JobsD instance")
	qd.RegisterJob("theJob", jobFunc)

	t.Given("we register the schedule to the JobsD instance")
	qd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.NoError(qd.Up())

	t.When("we schedule the job to run 10 times with a limit of 2 runs")
	startTime := time.Now()
	for i := 0; i < 10; i++ {
		wait.Add(2)
		_, errR := qd.CreateRun("theJob").Schedule("theSchedule").Limit(2).Run()
		t.NoError(errR)
	}

	t.Then("the job should have run 20 times")
	t.WaitTimeout(&wait, 2000*time.Millisecond)
	finishTime := time.Now()
	t.Equal(20, runNum)

	t.Then("the jobs should have run within " + timer.String() + " with a tolerance of 150ms")
	t.WithinDuration(finishTime, startTime.Add(timer), time.Duration(150*time.Millisecond))

	t.NoError(qd.Down())
}

func TestJobsDClusterWorkSharing(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	nodes := 20
	t.Given("a " + strconv.Itoa(nodes) + " JobsD instance cluster with one worker each")
	qdInstances := []*JobsD{}
	for i := 0; i < nodes; i++ {
		qdInstances = append(qdInstances, New(db).Logger(logger).WorkerNum(1).JobPollInterval(100*time.Millisecond))
	}

	runTime := 150 * time.Millisecond
	t.Given("a job that increments a counter and takes " + runTime.String())
	wait := sync.WaitGroup{}
	runCounter := 0
	jobFunc := func() error {
		defer wait.Done()
		<-time.After(runTime)
		runCounter++
		return nil
	}

	t.Given("the cluster can run the job")
	for _, qInst := range qdInstances {
		qInst.RegisterJob("jobName", jobFunc)
	}

	t.When("we bring up the JobsD instances")
	for _, qInst := range qdInstances {
		t.NoError(qInst.Up())
	}

	runs := nodes * 3
	runIDs := []int64{}
	t.When("we run a job " + strconv.Itoa(runs) + " times from the first instance in the cluster")
	for i := 0; i < runs; i++ {
		wait.Add(1)
		runID, err := qdInstances[0].CreateRun("jobName").Run()
		t.NoError(err)
		runIDs = append(runIDs, runID)
	}

	t.Then("the job should have run " + strconv.Itoa(runs) + " times")
	t.WaitTimeout(&wait, 10*runTime)
	t.Equal(runs, runCounter)

	t.Then("the job runs should have been distributed across the cluster and run")
	nonLocalJobRuns := 0
	for _, runID := range runIDs {
		theState := qdInstances[0].GetJobRunState(runID)
		if theState.RunStartedBy != nil && *theState.RunStartedBy != qdInstances[0].Instance.ID {
			nonLocalJobRuns++
		}
	}
	t.Greater(nonLocalJobRuns, 1)

	for _, qInst := range qdInstances {
		t.NoError(qInst.Down())
	}
}
func ExampleJobsD() {
	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	wait := make(chan struct{})

	job1Func := func(txt string) error {
		defer func() { wait <- struct{}{} }()
		fmt.Printf("Hello %s!", txt)
		return nil
	}
	schedule1Func := func(now time.Time) time.Time {
		return now.Add(500 * time.Millisecond)
	}

	qd := New(db).Logger(logger)

	qd.RegisterJob("job1", job1Func)
	qd.RegisterSchedule("schedule1", schedule1Func)

	err0 := qd.Up()
	checkError(err0)

	_, err1 := qd.CreateRun("job1", "World").Schedule("schedule1").Limit(1).Run()
	checkError(err1)

	<-wait
	err2 := qd.Down()
	checkError(err2)

	// Output: Hello World!
}

func BenchmarkQueueJobRun(b *testing.B) {

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	wait := sync.WaitGroup{}
	out := []int{}

	job1Func := func(i int) error {
		defer wait.Done()
		out = append(out, i)
		return nil
	}

	qd := New(db).Logger(logger)

	qd.RegisterJob("job1", job1Func)

	err0 := qd.Up()
	checkError(err0)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		wait.Add(1)
		_, err1 := qd.CreateRun("job1", n).Run()
		checkError(err1)
	}

	wait.Wait()
	b.StopTimer()

	err2 := qd.Down()
	checkError(err2)
}

func TestJobsD_basic(test *testing.T) {

	// t := testc.New(test)

	// t.Given("a database connection")
	// db, err0 := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	// t.NoError(err0)

	/*
		job1Func := func(txt string) error {
			fmt.Printf("Hello %s!", txt)
			return nil
		}
		schedule1Func := func(now time.Time) time.Time {
			return now.Add(time.Second)
		}

		qd := New(db)

		qd.AddJob("job1", job1Func)
		qd.AddJob("job2", job1Func).Timeout(time.Duration(10)*time.Second)

		qd.AddSchedule("schedule1", schedule1Func)

		qd.Up()

		qd.CreateRun("job1", "World A").Run()
		qd.CreateRun("job1", "World B").RunDelayed(time.Second)
		qd.CreateRun("job1", "World C").Unique("SharedUniqueJob").Run()

		qd.CreateRun("job1", "World D").Schedule("schedule1").Limit(2).Run()
		qd.CreateRun("job1", "World E").Schedule("schedule1").Limit(2).RunDelayed(time.Second)
		qd.CreateRun("job1", "World F").Unique("SharedUniqueJob").Schedule("schedule1").Limit(2).Run()

		<-time.After(time.Duration(3) * time.Second)
		qd.Down()

	*/
}
