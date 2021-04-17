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
	jdInstances := []*JobsD{}
	for i := 0; i < nodes; i++ {
		jdInstances = append(jdInstances, New(db).Logger(logger).WorkerNum(2))
	}

	runTime := 800 * time.Millisecond
	t.Given("a job that increments a counter and takes " + runTime.String())
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		atomic.AddUint32(&runCounter, 1)
		<-time.After(runTime)
		return nil
	}

	t.Given("the instances can run the job")
	for _, qInst := range jdInstances {
		qInst.RegisterJob("jobName", jobFunc)
	}

	t.When("we bring up the JobsD instances")
	for _, qInst := range jdInstances {
		t.Assert.NoError(qInst.Up())
	}

	t.When("we run the same unique job run on each of the instances in the cluster")
	wait.Add(1) //we only expect it to run once
	for _, qInst := range jdInstances {
		_, err := qInst.CreateRun("jobName").Unique("UniqueJobName").Run()
		t.Assert.NoError(err)
	}

	t.When("we wait until it finishes")
	t.WaitTimeout(&wait, ciDuration(5*runTime, 10*time.Second))

	t.Then("the job should have run only once")
	t.Assert.Equal(1, int(runCounter))

	waitTime := 500 * time.Millisecond
	t.When("we wait " + waitTime.String())
	<-time.After(waitTime)

	t.Then("the job should have still only run once")
	t.Assert.Equal(1, int(runCounter))

	for _, qInst := range jdInstances {
		t.Assert.NoError(qInst.Down())
	}
}

func TestRunOnceCreatorRunAfter(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunOnceCreatorRunAfter" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

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
	jd.RegisterJob(jobName, jobFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	delay := 500 * time.Millisecond
	t.Whenf("we run the job once after %s", delay.String())
	wait.Add(1)
	startTime := time.Now()
	_, err := jd.CreateRun(jobName).RunAfter(delay)
	t.Assert.NoError(err)

	t.Then("the job should have run once")
	t.WaitTimeout(&wait, ciDuration(4*time.Second, 10*time.Second))
	t.Assert.Equal(1, runNum)

	t.Thenf("the job run should run after the specified delay of %s", delay.String())
	t.Assert.WithinDuration(startTime.Add(delay), runTime, 300*time.Millisecond)

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestRunOnceCreatorRunTimeout(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunOnceCreatorRunTimeout" // Must be unique otherwise tests may collide

	retryCheck := 50 * time.Millisecond
	runTimeout := 200 * time.Millisecond
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

	t.Given("we register the job with a retry timeout limit of 1")
	jd.RegisterJob(jobName, jobFunc).RetriesTimeoutLimit(1)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.Whenf("we create a job run with a retry timeout of %s", runTimeout.String())
	jr := jd.CreateRun(jobName).RunTimeout(runTimeout)

	t.When("we run the job")
	wait.Add(2)
	_, err := jr.Run()
	t.Assert.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, ciDuration(500*time.Second, 10*time.Second))

	t.Then("the job should have run twice")
	t.Assert.Equal(2, int(runCounter))

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestRunOnceCreatorRetriesTimeoutLimit(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunOnceCreatorRetriesTimeoutLimit" // Must be unique otherwise tests may collide

	retryCheck := 20 * time.Millisecond
	retryTimeout := 100 * time.Millisecond
	jobRunTime := 300 * time.Millisecond

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("the instance checks for jobs that timeout every " + retryCheck.String())
	jd.TimeoutCheck(retryCheck)

	t.Given("a Job that times out consistently (takes too long)")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		atomic.AddUint32(&runCounter, 1)
		<-time.After(jobRunTime)
		return nil
	}

	t.Givenf("we register the job with a retry timeout of %s", retryTimeout.String())
	jd.RegisterJob(jobName, jobFunc).RunTimeout(retryTimeout)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we create a job run with a retry timeout limit of 2")
	jr := jd.CreateRun(jobName).RetriesTimeoutLimit(2)

	t.When("we run the job")
	wait.Add(3)
	_, err := jr.Run()
	t.Assert.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, ciDuration(8*time.Second, 15*time.Second))

	t.Then("the job should have run three times (1 + 2 retries)")
	t.Assert.Equal(3, int(runCounter))

	t.When("we wait enough time for another job run to complete")
	<-time.After(jobRunTime)
	<-time.After(retryTimeout)
	<-time.After(retryCheck)

	t.Then("the job should have still only run three times (1 + 2 retries)")
	t.Assert.Equal(3, int(runCounter))

	t.Assert.NoError(jd.Down()) // Cleanup
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
	t.Assert.NoError(jd.Up())

	t.When("we create a job run with a retry error limit of 2")
	jr := jd.CreateRun(jobName).RetryErrorLimit(2)

	t.When("we run the job")
	wait.Add(3)
	_, err := jr.Run()
	t.Assert.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, ciDuration(7*time.Second, 15*time.Second))

	t.Then("the job should have run three times (1 + 2 retries)")
	t.Assert.Equal(3, int(runCounter))

	t.When("we wait enough time for another job run to complete")
	<-time.After(1 * time.Second)

	t.Then("the job should have still only run three times (1 + 2 retries)")
	t.Assert.Equal(3, int(runCounter))

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestRunScheduleCreatorUnique(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunScheduleCreatorUnique" // Must be unique otherwise tests may collide

	nodes := 5
	t.Given(strconv.Itoa(nodes) + " JobsD instances to form a cluster")
	jdInstances := []*JobsD{}
	for i := 0; i < nodes; i++ {
		jdInstances = append(jdInstances, New(db).Logger(logger).WorkerNum(2).RunTimeout(time.Second*30))
	}

	runTime := 500 * time.Millisecond
	t.Given("a job that increments a counter and takes " + runTime.String())
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		atomic.AddUint32(&runCounter, 1)
		<-time.After(runTime)
		return nil
	}

	t.Given("the instances can run the job")
	for _, qInst := range jdInstances {
		qInst.RegisterJob(jobName, jobFunc)
	}

	interval := 200 * time.Millisecond
	t.Given("a schedule that runs at set uniform interval of " + interval.String())
	triggerTime := time.Now()
	scheduleFunc := func(now time.Time) time.Time {
		return triggerTime.Add(interval)
	}

	t.Given("the instances can use the schedule")
	for _, qInst := range jdInstances {
		qInst.RegisterSchedule("scheduleName", scheduleFunc)
	}

	t.When("we bring up the JobsD instances")
	for _, qInst := range jdInstances {
		t.Assert.NoError(qInst.Up())
	}

	t.When("we run the same unique job run on each of the instances in the cluster")
	wait.Add(2) //we only expect it to run twice
	for _, qInst := range jdInstances {
		_, err := qInst.CreateRun(jobName).Schedule("scheduleName").Unique(jobName + "UniqueJobName").Limit(2).Run()
		t.Assert.NoError(err)
	}

	t.When("we wait until it finishes")
	t.WaitTimeout(&wait, ciDuration(10*time.Second, 20*time.Second))

	t.Then("the job should have run twice")
	t.Assert.Equal(2, int(runCounter))

	waitTime := 500 * time.Millisecond
	t.When("we wait " + waitTime.String())
	<-time.After(waitTime)

	t.Then("the job should have still only run twice")
	t.Assert.Equal(2, int(runCounter))

	for _, qInst := range jdInstances {
		t.Assert.NoError(qInst.Down())
	}
}

func TestRunScheduledCreatorRunTimeout(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunScheduledCreatorRunTimeout" // Must be unique otherwise tests may collide

	retryCheck := 50 * time.Millisecond
	retryTimeout := 200 * time.Millisecond
	jobRunTimeTO := 1000 * time.Millisecond

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("the instance checks for jobs that timeout every " + retryCheck.String())
	jd.TimeoutCheck(retryCheck)

	t.Given("a Job that times out on the second run")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		currentCount := atomic.AddUint32(&runCounter, 1)
		if currentCount == 2 {
			<-time.After(jobRunTimeTO)
		}
		return nil
	}

	t.Given("we register the job with a retry timeout limit of 1")
	jd.RegisterJob(jobName, jobFunc).RetriesTimeoutLimit(1)

	interval := 200 * time.Millisecond
	t.Given("a Schedule that runs every " + interval.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(interval)
	}

	t.Given("we register the schedule to the JobsD instance")
	jd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.Whenf("we create a job run with a retry timeout of %s and a limit of 2 successful runs", retryTimeout.String())
	jr := jd.CreateRun(jobName).Schedule("theSchedule").RunTimeout(retryTimeout).Limit(2)

	t.When("we run the job")
	wait.Add(3)
	_, err := jr.Run()
	t.Assert.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, ciDuration(5*time.Second, 10*time.Second))

	t.Then("the job should have run three times (2 successful runs + 1 timed out run")
	t.Assert.Equal(3, int(runCounter))

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestRunScheduledCreatorRetriesTimeoutLimit(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunScheduledCreatorRetriesTimeoutLimit" // Must be unique otherwise tests may collide

	retryCheck := 50 * time.Millisecond
	runTimeout := 200 * time.Millisecond
	jobRunTime := 500 * time.Millisecond

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("the instance checks for jobs that timeout every " + retryCheck.String())
	jd.TimeoutCheck(retryCheck)

	t.Given("a Job that times out consistently (takes too long) after a successful run")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		currentCount := atomic.AddUint32(&runCounter, 1)
		if currentCount >= 2 && currentCount <= 3 {
			<-time.After(jobRunTime)
		}
		return nil
	}

	t.Givenf("we register the job with a retry timeout of %s", runTimeout.String())
	jd.RegisterJob(jobName, jobFunc).RunTimeout(runTimeout)

	interval := 200 * time.Millisecond
	t.Given("a Schedule that runs every " + interval.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(interval)
	}

	t.Given("we register the schedule to the JobsD instance")
	jd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we create a scheduled job run with a retry timeout limit of 2")
	jr := jd.CreateRun(jobName).Schedule("theSchedule").Limit(2).RetriesTimeoutLimit(2)

	t.When("we run the job")
	wait.Add(4)
	_, err := jr.Run()
	t.Assert.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, ciDuration(7*time.Second, 10*time.Second))

	t.Then("the job should have run 5 times (1 successful + 2 unsuccessful (2 retries) + 1 successful)")
	t.Assert.Equal(4, int(runCounter))

	t.When("we wait enough time for another job run to complete")
	<-time.After(jobRunTime)
	<-time.After(runTimeout)
	<-time.After(retryCheck)

	t.Then("the job should have still only run 5 times")
	t.Assert.Equal(4, int(runCounter))

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestRunScheduledCreatorRetryErrorLimit(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunScheduledCreatorRetryErrorLimit" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("a Job that errors out consistently")
	wait := sync.WaitGroup{}
	var runCounter uint32
	jobFunc := func() error {
		defer wait.Done()
		currentCount := atomic.AddUint32(&runCounter, 1)
		if currentCount >= 2 && currentCount <= 4 {
			return errors.New("some error")
		}
		return nil
	}

	t.Given("we register the job")
	jd.RegisterJob(jobName, jobFunc)

	interval := 200 * time.Millisecond
	t.Given("a Schedule that runs every " + interval.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(interval)
	}

	t.Given("we register the schedule to the JobsD instance")
	jd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we create a scheduled job run with a retry error limit of 2")
	jr := jd.CreateRun(jobName).Schedule("theSchedule").Limit(2).RetryErrorLimit(2)

	t.When("we run the job")
	wait.Add(5)
	_, err := jr.Run()
	t.Assert.NoError(err)

	t.Then("we wait for the job to finish")
	t.WaitTimeout(&wait, ciDuration(5*time.Second, 10*time.Second))

	t.Then("the job should have run 5 times (1 successful + 3 unsuccessful (2 retries) + 1 successful)")
	t.Assert.Equal(5, int(runCounter))

	t.When("we wait enough time for another job run to complete")
	<-time.After(1 * time.Second)

	t.Then("the job should have still only run 5 times")
	t.Assert.Equal(5, int(runCounter))

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestRunScheduleCreatorRunAfter(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunScheduleCreatorRunAfter" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

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
	jd.RegisterJob(jobName, jobFunc)

	t.Given("we register the schedule to the JobsD instance")
	jd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	delay := 500 * time.Millisecond
	t.Whenf("we run the job once after %s", delay.String())
	startTime := time.Now()
	wait.Add(1)
	_, err := jd.CreateRun(jobName).Schedule("theSchedule").Limit(1).RunAfter(delay)
	t.Assert.NoError(err)

	t.Then("the job should have run once")
	t.WaitTimeout(&wait, ciDuration(3*time.Second, 10*time.Second))
	t.Assert.Equal(1, runNum)

	t.Then("the job run should run after the specified delay of " + delay.String())
	t.Assert.WithinDuration(startTime.Add(delay).Add(interval), runTime, 250*time.Millisecond)

	t.Assert.NoError(jd.Down()) // Cleanup
}

func TestRunScheduleCreatorSimple(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunScheduleCreatorSimple" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := New(db).Logger(logger)

	t.Given("a Job that counts the number of times it runs")
	var runCounter uint32
	jobFunc := func() error {
		atomic.AddUint32(&runCounter, 1)
		return nil
	}

	interval := 200 * time.Millisecond
	t.Given("a Schedule that runs every " + interval.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(interval)
	}

	t.Given("we register the job to the JobsD instance")
	jd.RegisterJob(jobName, jobFunc)

	t.Given("we register the schedule to the JobsD instance")
	jd.RegisterSchedule("theSchedule", scheduleFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we run the job with the schedule")
	_, err := jd.CreateRun(jobName).Schedule("theSchedule").Run()
	t.Assert.NoError(err)

	t.When("we wait enough time for the job to have run twice")
	<-time.After(interval * 3)

	t.Then("the job should at least twice")
	runNum := atomic.LoadUint32(&runCounter)
	t.Assert.GreaterOrEqual(int(runNum), 2)

	t.Assert.NoError(jd.Down()) // Cleanup
}
