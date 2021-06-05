package jobspec

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
)

func TestJobRun(testT *testing.T) {
	t := testc.New(testT)
	jobName := testFuncName()

	t.Given("a JobsD instance")
	inst := testSetup(logrus.TraceLevel)
	err1 := inst.Start()
	t.Require.NoError(err1)

	defer testTeardown(inst)

	t.Given("a job func that increments a counter")
	var counter uint32
	jobFunc := func(helper RunHelper) error {
		atomic.AddUint32(&counter, 1)
		spew.Dump("HELLO WORLD!")
		return nil
	}

	for i := 0; i < 10; i++ {
		jobName1 := jobName + strconv.Itoa(i)

		t.Given("we register a new job that runs the func")
		job, err0 := inst.NewJob(jobName1, jobFunc).Register()
		t.Assert.NoError(err0)
		t.Assert.NotNil(job)

		t.When("we get and run the job")
		job, err2 := inst.GetJob(jobName1)
		t.Assert.NoError(err2)

		runState, err3 := job.RunIt()
		t.Assert.NoError(err3)

		t.When("we wait for the run to complete")
		for i := 0; i < 100; i++ {
			time.Sleep(100 * time.Millisecond)
			t.Require.NoError(runState.Refresh())
			if runState.Completed() {
				break
			}
		}
		t.Require.True(runState.Completed())

		t.Then("the job should have run and incremented the counter")
		count := int(atomic.LoadUint32(&counter))
		t.Assert.Equal(i+1, count)

	}
}

func TestJobSchedule(testT *testing.T) {
	t := testc.New(testT)
	jobName := testFuncName()

	t.Given("a JobsD instance")
	inst := testSetup(logrus.TraceLevel)
	err1 := inst.Start()
	t.Require.NoError(err1)

	defer testTeardown(inst)

	t.Given("a job func that increments a counter")
	var counter uint32
	jobFunc := func(helper RunHelper) error {
		atomic.AddUint32(&counter, 1)
		spew.Dump("HELLO WORLD!")
		return nil
	}

	scheduleDur := time.Millisecond * 200
	t.Givenf("a schedule func that schedules every %s", scheduleDur.String())
	scheduleFunc := func(now time.Time) time.Time {
		return now.Add(scheduleDur)
	}

	t.Given("we create a new job that runs the func")
	jobCreator := inst.NewJob(jobName, jobFunc)

	t.Given("we use the schedule func to create a job")
	jobCreator.SetSchedule(scheduleFunc)

	runLimit := 3
	t.Givenf("we limit the job to run %d time(s) on the schedule", runLimit)
	jobCreator.SetLimit(runLimit)

	t.When("we register the job")
	job, err0 := jobCreator.Register()
	t.Assert.NoError(err0)
	t.Assert.NotNil(job)

	t.When("we schedule the job to run")
	scheduleState, err2 := job.ScheduleIt()
	t.Assert.NoError(err2)

	t.When("the scheduler has scheduled the job runs")
	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		t.Require.NoError(scheduleState.Refresh())
		if scheduleState.ScheduleCount == runLimit {
			break
		}
	}

	t.When("we get the job run states")
	runStates := []*RunState{}
	var err3 error
	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		runStates, err3 = scheduleState.LatestRuns(runLimit * 2)
		t.Require.NoError(err3)
		if len(runStates) >= 3 {
			break
		}
	}

	t.Thenf("there we should have received %d run states", runLimit)
	t.Require.Equal(runLimit, len(runStates))

	t.When("the job runs have completed")
	completed := func() bool {
		for _, runState := range runStates {
			err4 := runState.Refresh()
			t.Require.NoError(err4)
			if !runState.Completed() {
				return false
			}
		}
		return true
	}
	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		if completed() {
			break
		}
	}
	t.Require.True(completed())

	t.Then("the jobs should have run and incremented the counter")
	count := int(atomic.LoadUint32(&counter))
	t.Assert.Equal(runLimit, count)

	t.When("we get the RunState history from the JobsD instance")
	history1, err5 := inst.JobHistory(jobName, runLimit*2)
	t.Assert.NoError(err5)

	t.Thenf("we should get %d record(s)", runLimit)
	t.Assert.Equal(runLimit, len(history1))

	t.Then("all RunState history records should show all job runs completed")
	for _, scheduleState := range history1 {
		t.Assert.True(scheduleState.Completed())
	}
}
