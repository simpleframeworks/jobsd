package jobss

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
)

func TestRunOnceCreatorUnique(test *testing.T) {
	t := testc.New(test)

	jobName := "TestRunOnceCreatorUnique"

	t.Given("a JobsS instance")
	jd := testSetup(logrus.ErrorLevel)

	nodes := 10
	t.Given("a " + strconv.Itoa(nodes) + " JobsS instance cluster with 5 workers each")
	jdInstances := []*JobsS{jd}
	for i := 0; i < nodes-1; i++ {
		jdInstances = append(jdInstances, New(jd.GetDB()).Logger(jd.GetLogger()).WorkerNum(5))
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
		qInst.RegisterJob(jobName, jobFunc)
	}

	t.When("we bring up the JobsS instances")
	for _, qInst := range jdInstances {
		t.Assert.NoError(qInst.Up())
	}

	t.When("we run the same unique job run on each of the instances in the cluster")
	wait.Add(1) //we only expect it to run once
	for _, qInst := range jdInstances {
		_, err := qInst.CreateRun(jobName).Unique("UniqueJobName").Run()
		t.Assert.NoError(err)
	}

	t.When("we wait until it finishes")
	wait.Wait()

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
	testTeardown(jd)
}

func TestRunScheduleCreatorUnique(test *testing.T) {
	t := testc.New(test)

	jobName := "TestRunScheduleCreatorUnique"

	t.Given("a JobsS instance")
	jd := testSetup(logrus.ErrorLevel)

	nodes := 5
	t.Given("a " + strconv.Itoa(nodes) + " JobsS instance cluster with 2 workers each")
	jdInstances := []*JobsS{jd}
	for i := 0; i < nodes-1; i++ {
		jdInstances = append(jdInstances, New(jd.GetDB()).Logger(jd.GetLogger()).WorkerNum(2).RunTimeout(time.Second*30))
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

	t.When("we bring up the JobsS instances")
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
	wait.Wait()

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

func TestJobsSClusterWorkSharing(test *testing.T) {
	t := testc.New(test)

	jobName := "TestJobsSClusterWorkSharing" // Must be unique otherwise tests may collide

	t.Given("a JobsS instance")
	jd := testSetup(logrus.ErrorLevel)

	nodes := 10
	t.Given("a " + strconv.Itoa(nodes) + " JobsS instance cluster with one worker each")
	jdInstances := []*JobsS{jd}
	for i := 0; i < nodes-1; i++ {
		jdInstances = append(jdInstances, New(jd.GetDB()).Logger(jd.GetLogger()).WorkerNum(1).PollInterval(100*time.Millisecond))
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

	t.When("we bring up the JobsS instances")
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
	wait.Wait()
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
	testTeardown(jd)
}

func TestRunOnceClusterFailover(test *testing.T) {
	t := testc.New(test)

	jobName := "TestRunOnceClusterFailover"

	t.Given("a JobsS instance")
	jd := testSetup(logrus.ErrorLevel)

	t.Givenf("a cluster with 2 JobsS instances with 1 worker each")
	jdInstances := [2]*JobsS{}
	jdInstances[0] = New(jd.GetDB()).Logger(jd.GetLogger()).WorkerNum(1)
	jdInstances[1] = New(jd.GetDB()).Logger(jd.GetLogger()).WorkerNum(1).PollInterval(200 * time.Millisecond)

	t.Given("we register a job that hangs on the first instance")
	var startCounter uint32
	started := make(chan struct{}, 1)
	runTime := 5 * time.Minute
	jobFunc0 := func() error {
		atomic.AddUint32(&startCounter, 1)
		started <- struct{}{}
		<-time.After(runTime)
		return nil
	}
	jdInstances[0].RegisterJob(jobName, jobFunc0)

	t.Given("we register a job that does NOT hang on the second instance")
	jobFunc1 := func() error {
		atomic.AddUint32(&startCounter, 1)
		return nil
	}
	jdInstances[1].RegisterJob(jobName, jobFunc1)

	t.When("we bring up the JobsS instances")
	t.Assert.NoError(jdInstances[0].Up())
	t.Assert.NoError(jdInstances[1].Up())

	timeout := 500 * time.Millisecond
	t.When("we create 'job run 1' on the first instance in the cluster that times out")
	runID, err0 := jdInstances[0].CreateRun(jobName).RunTimeout(timeout).Run()
	t.Assert.NoError(err0)

	t.When("we shutdown the first cluster instance to simulate a failure,  after the 'job run 1' started")
	<-started
	t.Assert.NoError(jdInstances[0].Down())

	t.When("we wait enough time for the job run to timeout and retry again")
	<-time.After(timeout * 10)

	t.Then("'job run 1' should have timed out and tried to re-run on the second cluster instance")
	t.Assert.Equal(2, int(atomic.LoadUint32(&startCounter)))

	t.Then("'job run 1' should been created on the first instance of the cluster")
	runState := jdInstances[0].GetRunState(runID)
	t.Assert.Equal(jdInstances[0].GetInstance().ID, runState.CreatedBy)

	t.Then("'job run 1' should have run successfully on the second instance of the cluster")
	t.Require.NotNil(runState.RunStartedBy)
	t.Assert.Equal(jdInstances[1].GetInstance().ID, *runState.RunStartedBy)
	t.Assert.NotNil(runState.RunStartedAt)
	t.Assert.NotNil(runState.RunCompletedAt)
	t.Assert.Nil(runState.RunCompletedError)

	// Cleanup
	t.Assert.NoError(jdInstances[1].Down())
	testTeardown(jd)
}
