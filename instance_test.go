package jobspec

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
)

func TestJobBasic1(testT *testing.T) {
	t := testc.New(testT)
	jobName := testFuncName()

	t.Given("a JobsD instance")
	inst := testSetup(logrus.ErrorLevel)
	defer testTeardown(inst)

	t.Given("a job func that increments a counter")
	var counter uint32
	wait := &sync.WaitGroup{}
	jobFunc := func() error {
		atomic.AddUint32(&counter, 1)
		defer wait.Done()
		return nil
	}

	t.Given("a new job that runs the func")
	job, err0 := inst.NewJob(jobName, jobFunc).Register()

	t.Assert.NoError(err0)
	t.Assert.NotNil(job)

	t.When("we start the JobsD instance")
	err1 := inst.Start()

	t.Assert.NoError(err1)

	wait.Add(1)
	t.When("we get and run the job")
	runState, err2 := inst.GetJob(jobName).Run()

	t.Assert.NoError(err2)

	t.When("we wait for it to run")
	t.WaitTimeout(wait, time.Second*5)

	t.Then("the job should have run and incremented the counter")
	count := int(atomic.LoadUint32(&counter))

	t.Assert.Equal(1, count)

	t.When("we refresh the run state that we got when we ran the job")
	err3 := runState.Refresh()
	t.Assert.NoError(err3)

	t.Then("the RunState of the job run should have completed")
	t.Assert.True(runState.Completed())
}

func TestJobBasic2(testT *testing.T) {
	t := testc.New(testT)
	jobName := testFuncName()

	t.Given("a JobsD instance")
	inst := testSetup(logrus.ErrorLevel)
	defer testTeardown(inst)

	t.Given("a job func that increments a counter")
	var counter uint32
	wait := &sync.WaitGroup{}
	jobFunc := func() error {
		atomic.AddUint32(&counter, 1)
		defer wait.Done()
		return nil
	}

	t.Given("a new job that runs the func")
	job, err0 := inst.NewJob(jobName, jobFunc).Register()

	t.Assert.NoError(err0)
	t.Assert.NotNil(job)

	t.When("we start the JobsD instance")
	err1 := inst.Start()

	t.Assert.NoError(err1)

	wait.Add(1)
	t.When("we run the job")
	runState, err2 := job.Run()

	t.Assert.NoError(err2)

	t.When("we wait for it to run")
	t.WaitTimeout(wait, time.Second*5)

	t.Then("the job should have run and incremented the counter")
	count := int(atomic.LoadUint32(&counter))

	t.Assert.Equal(1, count)

	t.When("we refresh the run state that we got when we ran the job")
	err3 := runState.Refresh()
	t.Assert.NoError(err3)

	t.Then("the RunState of the job run should have completed")
	t.Assert.True(runState.Completed())

}

func TestJobSchedule(testT *testing.T) {
	t := testc.New(testT)
	jobName := testFuncName()

	t.Given("a JobsD instance")
	inst := testSetup(logrus.ErrorLevel)
	defer testTeardown(inst)

	t.Given("a job func that increments a counter")
	var counter uint32
	wait := &sync.WaitGroup{}
	jobFunc := func() error {
		atomic.AddUint32(&counter, 1)
		defer wait.Done()
		return nil
	}

	scheduleDur := time.Millisecond * 200
	t.Givenf("a schedule func that schedules every %s", scheduleDur.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(scheduleDur)
	}

	t.Given("we use the job func to create a job")
	creator := inst.NewJob(jobName, jobFunc)

	t.Given("we use the schedule func to create a job")
	creator.Schedule(scheduleFunc)

	runLimit := 3
	t.Given("we limit the job to run %d time(s)", runLimit)
	creator.Limit(runLimit)

	t.When("we register the job")
	job, err0 := creator.Register()

	t.Then("we should get job and no errors")
	t.Assert.NoError(err0)
	t.Assert.NotNil(job)

	t.When("we start the JobsD instance")
	err1 := inst.Start()

	t.Assert.NoError(err1)

	wait.Add(runLimit)
	t.When("run the job")
	runState, err2 := job.Run()

	t.Assert.NoError(err2)

	t.When("we wait for it to run")
	t.WaitTimeout(wait, time.Second*10)

	t.Then("the job should have run and incremented the counter")
	count := int(atomic.LoadUint32(&counter))

	t.Assert.Equal(runLimit, count)

	t.When("we refresh the run state")
	err3 := runState.Refresh()
	t.Assert.NoError(err3)

	t.Then("the RunState of the first job run should have completed")
	t.Assert.True(runState.Completed())

	t.When("we get the RunState history from the job")
	history, err4 := job.History(runLimit * 2)
	t.Assert.NoError(err4)

	t.Thenf("we should get %d record(s)", runLimit)
	t.Assert.Equal(runLimit, len(history))

	t.Then("all RunState history records should show all job runs completed")
	for _, runState := range history {
		t.Assert.True(runState.Completed())
	}

	t.When("we get the RunState history from the JobsD instance")
	history1, err5 := inst.JobHistory(jobName, runLimit*2)
	t.Assert.NoError(err5)

	t.Thenf("we should get %d record(s)", runLimit)
	t.Assert.Equal(runLimit, len(history1))

	t.Then("all RunState history records should show all job runs completed")
	for _, runState := range history1 {
		t.Assert.True(runState.Completed())
	}
}
