package jobsd

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
)

func TestJobsDRun(testT *testing.T) {
	t := testc.New(testT)

	jobName := "TestJobsDRun" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := testSetup(logrus.ErrorLevel)

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
	wait.Wait()
	t.Assert.Equal(1, int(runCounter))

	t.Then("the job run should have completed within 1 second")
	t.Assert.WithinDuration(time.Now(), startTime, 1*time.Second)
	// If it takes longer it means it ran after being recovered from the DB

	testTeardown(jd)
}

func TestJobsDRunMulti(test *testing.T) {
	t := testc.New(test)

	jobName := "TestJobsDRunMulti" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := testSetup(logrus.ErrorLevel)

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
	wait.Wait()
	t.Assert.Equal(runNum, int(runCounter))

	t.Then("the all job runs should have completed within 3 second")
	t.Assert.WithinDuration(time.Now(), startTime, 3*time.Second)
	// If it takes longer it means it ran after being recovered from the DB

	testTeardown(jd)
}

func TestQueuedRunErrRetry(test *testing.T) {
	t := testc.New(test)

	jobName := "TestQueuedRunErrRetry" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := testSetup(logrus.ErrorLevel)

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
	jd.RegisterJob(jobName, jobFunc).RetriesErrorLimit(1)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we run the job")
	wait.Add(2)
	startTime := time.Now()
	_, err := jd.CreateRun(jobName, 0).Run()
	t.Assert.NoError(err)

	t.Then("the job should have run twice")
	wait.Wait()
	t.Assert.Equal(2, int(runCounter))

	t.Then("the job runs should have completed within 1 second")
	t.Assert.WithinDuration(time.Now(), startTime, 1*time.Second)
	// If it takes longer it means it ran after being recovered from the DB

	testTeardown(jd)
}

func TestQueuedRunTimeoutRetry(test *testing.T) {
	t := testc.New(test)

	jobName := "TestQueuedRunTimeoutRetry" // Must be unique otherwise tests may collide

	retryTimeout := 200 * time.Millisecond
	firstRunTime := 1000 * time.Millisecond

	t.Given("a JobsD instance")
	jd := testSetup(logrus.ErrorLevel)

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
	wait.Wait()

	t.Then("the job should have run twice")
	t.Assert.Equal(2, int(runCounter))

	testTeardown(jd)
}

func TestJobsDScheduledRun(test *testing.T) {
	t := testc.New(test)

	jobName := "TestJobsDScheduledRun" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := testSetup(logrus.ErrorLevel)

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
	wait.Wait()
	finishTime := time.Now()

	t.Then("the job should have run within " + timer.String() + " with a tolerance of 150ms")
	t.Assert.WithinDuration(finishTime, startTime.Add(timer), time.Duration(150*time.Millisecond))

	t.Then("the job should only run once even if we wait for another 500ms")
	<-time.After(500 * time.Millisecond)
	t.Assert.Equal(1, int(runCounter))

	testTeardown(jd)
}

func TestJobsDScheduledRunRecurrent(test *testing.T) {
	t := testc.New(test)

	jobName := "TestJobsDScheduledRunRecurrent" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := testSetup(logrus.ErrorLevel)

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
	wait.Wait()
	finishTime := time.Now()

	t.Then("the job should only run three times even if we wait for another 500ms")
	<-time.After(500 * time.Millisecond)
	t.Assert.Equal(3, int(runCounter))

	timerFor3 := time.Duration(3 * timer)
	tolerance := time.Duration(1000 * time.Millisecond)
	t.Thenf("3 jobs should have run within %s with a tolerance of %s", timerFor3.String(), tolerance.String())
	t.Assert.WithinDuration(finishTime, startTime.Add(timerFor3), tolerance)

	testTeardown(jd)
}

func TestJobsDScheduledRunMulti(test *testing.T) {
	t := testc.New(test)

	jobName := "TestJobsDScheduledRunMulti" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	workers := 1
	jd := testSetup(logrus.ErrorLevel).WorkerNum(workers)

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

	runNum := 8
	times := 2
	t.Whenf("we schedule the job to run %d times with a limit of %d runs", runNum, times)
	startTime := time.Now()
	for i := 0; i < runNum; i++ {
		wait.Add(2)
		_, errR := jd.CreateRun(jobName).Schedule("theSchedule").Limit(times).Run()
		t.Assert.NoError(errR)
	}

	t.Thenf("the job should have run %d times", runNum*times)
	wait.Wait()
	finishTime := time.Now()
	t.Assert.Equal(runNum*times, int(runCounter))

	expectedRunTime := timer * time.Duration(times+runNum/workers)
	tolerance := 100 * time.Millisecond * time.Duration(times*runNum/workers)
	t.Thenf("the jobs should have run within %s with a tolerance of %s", expectedRunTime.String(), tolerance.String())
	t.Assert.WithinDuration(startTime.Add(expectedRunTime), finishTime, tolerance)

	testTeardown(jd)
}

func ExampleJobsD() {
	jobName := "ExampleJobsD" // Must be unique otherwise tests may collide

	jd := testSetup(logrus.ErrorLevel)

	wait := make(chan struct{})
	job1Func := func(txt string) error {
		fmt.Printf("Hello %s!", txt)
		wait <- struct{}{}
		return nil
	}
	jd.RegisterJob(jobName, job1Func)

	schedule1Func := func(now time.Time) time.Time {
		return now.Add(500 * time.Millisecond)
	}
	jd.RegisterSchedule("schedule1", schedule1Func)

	err0 := jd.Up()
	testPanicErr(err0)

	_, err1 := jd.CreateRun(jobName, "World").Schedule("schedule1").Limit(1).Run()
	testPanicErr(err1)

	<-wait
	err2 := jd.Down()
	testPanicErr(err2)

	testTeardown(jd)

	// Output: Hello World!
}

func BenchmarkQueueRun(b *testing.B) {

	jobName := "BenchmarkQueueRun" // Must be unique otherwise tests may collide

	jd := testSetup(logrus.ErrorLevel)

	wait := sync.WaitGroup{}
	out := []int{}
	job1Func := func(i int) error {
		defer wait.Done()
		out = append(out, i)
		return nil
	}
	jd.RegisterJob(jobName, job1Func)

	err0 := jd.Up()
	testPanicErr(err0)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		wait.Add(1)
		_, err1 := jd.CreateRun(jobName, n).Run()
		testPanicErr(err1)
	}

	wait.Wait()
	b.StopTimer()

	testTeardown(jd)
}
