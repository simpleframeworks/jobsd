package jobsd

import (
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
)

func TestRunState(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunState" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger)

	t.Given("a Job that starts and then pauses when called")
	jobStarted := make(chan struct{})
	jobContinue := make(chan struct{})
	jobFunc := func() error {
		jobStarted <- struct{}{}
		<-jobContinue
		return nil
	}

	t.Given("we register the job to the JobsD instance")
	qd.RegisterJob(jobName, jobFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(qd.Up())

	delay := 200 * time.Millisecond
	t.When("we run the job after " + delay.String())
	theID, err := qd.CreateRun(jobName).RunAfter(delay)
	t.Assert.NoError(err)

	t.When("we get the job run state")
	theState := qd.GetRunState(theID)

	t.Then("the state should match a job run that has not run")
	t.Assert.Equal(0, int(theState.RunSuccessCount))
	t.Assert.Nil(theState.RunStartedAt)
	t.Assert.Nil(theState.RunStartedBy)
	t.Assert.Nil(theState.RunCompletedAt)
	t.Assert.Nil(theState.RunCompletedError)
	t.Assert.Equal(0, int(theState.RetriesOnErrorCount))
	t.Assert.Equal(0, int(theState.RetriesOnTimeoutCount))
	t.Assert.Nil(theState.Schedule)
	t.Assert.WithinDuration(theState.CreatedAt, time.Now(), 100*time.Millisecond)
	t.Assert.Equal(qd.instance.ID, theState.CreatedBy)

	createdAt := theState.CreatedAt

	t.When("the job has started")
	<-jobStarted

	t.When("we refresh the job run state")
	err = theState.Refresh()
	t.Assert.NoError(err)

	t.Then("the state should match a job run that is running")
	t.Assert.Equal(0, int(theState.RunSuccessCount))
	t.Assert.WithinDuration(*theState.RunStartedAt, time.Now(), 800*time.Millisecond)
	t.Assert.Equal(qd.instance.ID, *theState.RunStartedBy)
	t.Assert.Nil(theState.RunCompletedAt)
	t.Assert.Nil(theState.RunCompletedError)
	t.Assert.Equal(0, int(theState.RetriesOnErrorCount))
	t.Assert.Equal(0, int(theState.RetriesOnTimeoutCount))
	t.Assert.Nil(theState.Schedule)
	t.Assert.Equal(theState.CreatedAt, createdAt)
	t.Assert.Equal(qd.instance.ID, theState.CreatedBy)

	RunStartedAt := *theState.RunStartedAt

	t.When("we let the job run complete")
	jobContinue <- struct{}{}

	t.When("we shutdown the JobsD instance to let everything complete")
	t.Assert.NoError(qd.Down())

	t.When("we refresh the job run state")
	err = theState.Refresh()
	t.Assert.NoError(err)

	t.Then("the state should match a job run that has completed without error")
	t.Assert.Equal(1, int(theState.RunSuccessCount))
	t.Assert.Equal(*theState.RunStartedAt, RunStartedAt)
	t.Assert.Equal(qd.instance.ID, *theState.RunStartedBy)
	t.Require.NotNil(theState.RunCompletedAt)
	t.Assert.WithinDuration(*theState.RunCompletedAt, time.Now(), 100*time.Millisecond)
	t.Assert.Nil(theState.RunCompletedError)
	t.Assert.Equal(0, int(theState.RetriesOnErrorCount))
	t.Assert.Equal(0, int(theState.RetriesOnTimeoutCount))
	t.Assert.Nil(theState.Schedule)
	t.Assert.Equal(theState.CreatedAt, createdAt)
	t.Assert.Equal(qd.instance.ID, theState.CreatedBy)

}
