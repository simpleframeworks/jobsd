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

		runState, err3 := job.Run()
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
	creator := inst.NewJob(jobName, jobFunc)

	t.Given("we use the schedule func to create a job")
	creator.Schedule(scheduleFunc)

	runLimit := 3
	t.Given("we limit the job to run %d time(s) on the schedule", runLimit)
	creator.Limit(runLimit)

	t.When("we register the job")
	job, err0 := creator.Register()
	t.Assert.NoError(err0)
	t.Assert.NotNil(job)

	t.When("we schedule the job")
	scheduleState, err2 := job.Schedule()
	t.Assert.NoError(err2)

	t.When("we wait for the run to complete")
	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		t.Require.NoError(scheduleState.Refresh())
		if scheduleState.Completed() {
			break
		}
	}
	t.Require.True(scheduleState.Completed())

	t.Then("the job should have run and incremented the counter")
	count := int(atomic.LoadUint32(&counter))
	t.Assert.Equal(runLimit, count)

	t.When("we refresh the run state")
	err3 := scheduleState.Refresh()
	t.Assert.NoError(err3)

	t.Then("the RunState of the first job run should have completed")
	t.Assert.True(scheduleState.Completed())

	t.When("we get the RunState history from the job")
	history, err4 := job.History(runLimit * 2)
	t.Assert.NoError(err4)

	t.Thenf("we should get %d record(s)", runLimit)
	t.Assert.Equal(runLimit, len(history))

	t.Then("all RunState history records should show all job runs completed")
	for _, scheduleState := range history {
		t.Assert.True(scheduleState.Completed())
	}

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
