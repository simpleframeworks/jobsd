package jobsd

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
)

func TestRunOnceCreatorUnique(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	nodes := 10
	t.Given(strconv.Itoa(nodes) + " JobsD instances to form a cluster")
	qdInstances := []*JobsD{}
	for i := 0; i < nodes; i++ {
		qdInstances = append(qdInstances, New(db).Logger(logger).WorkerNum(2))
	}

	runTime := 200 * time.Millisecond
	t.Given("a job that increments a counter and takes " + runTime.String())
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		<-time.After(runTime)
		atomic.AddUint32(&runCounter, 1)
		return nil
	}

	t.Given("the instances can run the job")
	for _, qInst := range qdInstances {
		qInst.RegisterJob("jobName", jobFunc)
	}

	t.When("we bring up the JobsD instances")
	for _, qInst := range qdInstances {
		t.NoError(qInst.Up())
	}

	t.When("we run the same unique job run on each of the instances in the cluster")
	wait.Add(1) //we only expect it to run once
	for _, qInst := range qdInstances {
		_, err := qInst.CreateRun("jobName").Unique("UniqueJobName").Run()
		t.NoError(err)
	}

	t.When("we wait until it finishes")
	t.WaitTimeout(&wait, 5*runTime)

	t.Then("the job should have run only once")
	t.Equal(1, int(runCounter))

	waitTime := 500 * time.Millisecond
	t.When("we wait " + waitTime.String())
	<-time.After(waitTime)

	t.Then("the job should have still only run once")
	t.Equal(1, int(runCounter))

	for _, qInst := range qdInstances {
		t.NoError(qInst.Down())
	}
}

func TestRunOnceCreatorRunAfter(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger)

	t.Given("a Job that records the time it started running")
	wait := sync.WaitGroup{}
	var runTime time.Time
	runNum := 0
	jobFunc := func() error {
		runTime = time.Now()
		runNum++
		defer wait.Done()
		return nil
	}

	t.Given("we register the job to the JobsD instance")
	qd.RegisterJob("theJob", jobFunc)

	t.When("we bring up the JobsD instance")
	t.NoError(qd.Up())

	delay := 500 * time.Millisecond
	t.When("we run the job once after ")
	startTime := time.Now()
	wait.Add(1)
	_, err := qd.CreateRun("theJob").RunAfter(delay)
	t.NoError(err)

	t.Then("the job should have run once")
	t.WaitTimeout(&wait, 3*time.Second)
	t.Equal(1, runNum)

	t.Then("the job run should run after the specified delay of " + delay.String())
	t.WithinDuration(startTime.Add(delay), runTime, 250*time.Millisecond)
}
func TestRunOnceCreatorRetryTimeout(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunOnceCreatorRetryTimeout" // Must be unique otherwise tests may collide

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

	t.Given("we register the job with a retry timeout limit of 1")
	jd.RegisterJob(jobName, jobFunc).RetryTimeoutLimit(1)

	t.When("we bring up the JobsD instance")
	t.NoError(jd.Up())

	t.Whenf("we create a job run with a retry timeout of %s", retryTimeout.String())
	jr := jd.CreateRun(jobName).RetryTimeout(retryTimeout)

	t.When("we run the job")
	wait.Add(2)
	_, err := jr.Run()
	t.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, 5*time.Second)

	t.Then("the job should have run twice")
	t.Equal(2, int(runCounter))

	t.NoError(jd.Down()) // Cleanup
}

func TestRunOnceCreatorRetryTimeoutLimit(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunOnceCreatorRetryTimeoutLimit" // Must be unique otherwise tests may collide

	retryCheck := 50 * time.Millisecond
	retryTimeout := 200 * time.Millisecond
	jobRunTime := 500 * time.Millisecond

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("the instance checks for jobs that timeout every " + retryCheck.String())
	jd.JobRetryTimeoutCheck(retryCheck)

	t.Given("a Job that times out consistently (takes too long)")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		<-time.After(jobRunTime)
		atomic.AddUint32(&runCounter, 1)
		return nil
	}

	t.Givenf("we register the job with a retry timeout of %s", retryTimeout.String())
	jd.RegisterJob(jobName, jobFunc).RetryTimeout(retryTimeout)

	t.When("we bring up the JobsD instance")
	t.NoError(jd.Up())

	t.When("we create a job run with a retry timeout limit of 2")
	jr := jd.CreateRun(jobName).RetryTimeoutLimit(2)

	t.When("we run the job")
	wait.Add(3)
	_, err := jr.Run()
	t.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, 5*time.Second)

	t.Then("the job should have run three times (1 + 2 retries)")
	t.Equal(3, int(runCounter))

	t.When("we wait enough time for another job run to complete")
	<-time.After(jobRunTime)
	<-time.After(retryTimeout)
	<-time.After(retryCheck)

	t.Then("the job should have still only run three times (1 + 2 retries)")
	t.Equal(3, int(runCounter))

	t.NoError(jd.Down()) // Cleanup
}

func TestRunOnceCreatorRetryErrorLimit(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunOnceCreatorRetryErrorLimit" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("a Job that errors out consistently")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		atomic.AddUint32(&runCounter, 1)
		return errors.New("some error")
	}

	t.Given("we register the job")
	jd.RegisterJob(jobName, jobFunc)

	t.When("we bring up the JobsD instance")
	t.NoError(jd.Up())

	t.When("we create a job run with a retry error limit of 2")
	jr := jd.CreateRun(jobName).RetryErrorLimit(2)

	t.When("we run the job")
	wait.Add(3)
	_, err := jr.Run()
	t.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, 5*time.Second)

	t.Then("the job should have run three times (1 + 2 retries)")
	t.Equal(3, int(runCounter))

	t.When("we wait enough time for another job run to complete")
	<-time.After(1 * time.Second)

	t.Then("the job should have still only run three times (1 + 2 retries)")
	t.Equal(3, int(runCounter))

	t.NoError(jd.Down()) // Cleanup
}

func TestRunScheduleCreatorUnique(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	nodes := 10
	t.Given(strconv.Itoa(nodes) + " JobsD instances to form a cluster")
	qdInstances := []*JobsD{}
	for i := 0; i < nodes; i++ {
		qdInstances = append(qdInstances, New(db).Logger(logger).WorkerNum(2))
	}

	runTime := 200 * time.Millisecond
	t.Given("a job that increments a counter and takes " + runTime.String())
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		<-time.After(runTime)
		atomic.AddUint32(&runCounter, 1)
		return nil
	}

	t.Given("the instances can run the job")
	for _, qInst := range qdInstances {
		qInst.RegisterJob("jobName", jobFunc)
	}

	interval := 150 * time.Millisecond
	t.Given("a schedule that runs at set uniform interval of " + interval.String())
	triggerTime := time.Now()
	scheduleFunc := func(now time.Time) time.Time {
		triggerTime = triggerTime.Add(interval)
		return triggerTime
	}

	t.Given("the instances can use the schedule")
	for _, qInst := range qdInstances {
		qInst.RegisterSchedule("scheduleName", scheduleFunc)
	}

	t.When("we bring up the JobsD instances")
	for _, qInst := range qdInstances {
		t.NoError(qInst.Up())
	}

	t.When("we run the same unique job run on each of the instances in the cluster")
	wait.Add(2) //we only expect it to run twice
	for _, qInst := range qdInstances {
		_, err := qInst.CreateRun("jobName").Schedule("scheduleName").Unique("UniqueJobName").Limit(2).Run()
		t.NoError(err)
	}

	t.When("we wait until it finishes")
	t.WaitTimeout(&wait, 3*time.Second)

	t.Then("the job should have run twice")
	t.Equal(2, int(runCounter))

	waitTime := 500 * time.Millisecond
	t.When("we wait " + waitTime.String())
	<-time.After(waitTime)

	t.Then("the job should have still only run twice")
	t.Equal(2, int(runCounter))

	for _, qInst := range qdInstances {
		t.NoError(qInst.Down())
	}
}

func TestRunScheduleCreatorRunAfter(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger)

	t.Given("a Job that records the time it started running")
	wait := sync.WaitGroup{}
	var runTime time.Time
	runNum := 0
	jobFunc := func() error {
		runTime = time.Now()
		runNum++
		defer wait.Done()
		return nil
	}

	interval := 200 * time.Millisecond
	t.Given("a Schedule that runs every " + interval.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(interval)
	}

	t.Given("we register the job to the JobsD instance")
	qd.RegisterJob("theJob", jobFunc)

	t.Given("we register the schedule to the JobsD instance")
	qd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.NoError(qd.Up())

	delay := 500 * time.Millisecond
	t.When("we run the job once after ")
	startTime := time.Now()
	wait.Add(1)
	_, err := qd.CreateRun("theJob").Schedule("theSchedule").Limit(1).RunAfter(delay)
	t.NoError(err)

	t.Then("the job should have run once")
	t.WaitTimeout(&wait, 3*time.Second)
	t.Equal(1, runNum)

	t.Then("the job run should run after the specified delay of " + delay.String())
	t.WithinDuration(startTime.Add(delay).Add(interval), runTime, 250*time.Millisecond)
}
