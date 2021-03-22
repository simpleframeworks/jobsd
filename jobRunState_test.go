package jobsd

import (
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
)

func TestJobRunState(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)

	t.Given("a JobsD instance")
	qd := New(db).Logger(logger)

	t.Given("a Job that signals and then pauses when called")
	jobStarted := make(chan struct{})
	jobContinue := make(chan struct{})
	jobFunc := func() error {
		jobStarted <- struct{}{}
		<-jobContinue
		return nil
	}

	t.Given("we register the job to the JobsD instance")
	qd.RegisterJob("theJob", jobFunc)

	t.When("we bring up the JobsD instance")
	t.NoError(qd.Up())

	delay := 200 * time.Millisecond
	t.When("we run the job after " + delay.String())
	theID, err := qd.CreateRun("theJob").RunAfter(delay)
	t.NoError(err)

	t.When("we get the job run state")
	theState := qd.GetJobRunState(theID)

	t.Then("the state should match a job run that has not run")
	t.Equal(0, int(theState.RunCount))
	t.Nil(theState.RunStartedAt)
	t.Nil(theState.RunStartedBy)
	t.Nil(theState.RunCompletedAt)
	t.Nil(theState.RunCompletedError)
	t.Equal(0, int(theState.RetriesOnErrorCount))
	t.Equal(0, int(theState.RetriesOnTimeoutCount))
	t.Nil(theState.Schedule)
	t.Nil(theState.ClosedAt)
	t.Nil(theState.ClosedBy)
	t.WithinDuration(theState.CreatedAt, time.Now(), 100*time.Millisecond)
	t.Equal(qd.instance.ID, theState.CreatedBy)

	createdAt := theState.CreatedAt

	t.When("the job has started")
	<-jobStarted

	t.When("we refresh the job run state")
	err = theState.Refresh()
	t.NoError(err)

	t.Then("the state should match a job run that is running")
	t.Equal(0, int(theState.RunCount))
	t.WithinDuration(*theState.RunStartedAt, time.Now(), 800*time.Millisecond)
	t.Equal(qd.instance.ID, *theState.RunStartedBy)
	t.Nil(theState.RunCompletedAt)
	t.Nil(theState.RunCompletedError)
	t.Equal(0, int(theState.RetriesOnErrorCount))
	t.Equal(0, int(theState.RetriesOnTimeoutCount))
	t.Nil(theState.Schedule)
	t.Nil(theState.ClosedAt)
	t.Nil(theState.ClosedBy)
	t.Equal(theState.CreatedAt, createdAt)
	t.Equal(qd.instance.ID, theState.CreatedBy)

	RunStartedAt := *theState.RunStartedAt

	t.When("we let the job run complete")
	jobContinue <- struct{}{}

	t.When("we shutdown the JobsD instance to let everything complete")
	t.NoError(qd.Down())

	t.When("we refresh the job run state")
	err = theState.Refresh()
	t.NoError(err)

	t.Then("the state should match a job run that has completed without error")
	t.Equal(1, int(theState.RunCount))
	t.Equal(*theState.RunStartedAt, RunStartedAt)
	t.Equal(qd.instance.ID, *theState.RunStartedBy)
	t.NotNil(theState.RunCompletedAt)
	t.WithinDuration(*theState.RunCompletedAt, time.Now(), 100*time.Millisecond)
	t.Nil(theState.RunCompletedError)
	t.Equal(0, int(theState.RetriesOnErrorCount))
	t.Equal(0, int(theState.RetriesOnTimeoutCount))
	t.Nil(theState.Schedule)
	t.NotNil(theState.ClosedAt)
	t.WithinDuration(*theState.ClosedAt, time.Now(), 100*time.Millisecond)
	t.NotNil(theState.ClosedBy)
	t.Equal(qd.instance.ID, *theState.ClosedBy)
	t.Equal(theState.CreatedAt, createdAt)
	t.Equal(qd.instance.ID, theState.CreatedBy)

}
