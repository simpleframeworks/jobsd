package jobss

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
	"syreclabs.com/go/faker"
)

func TestRunnableLock(test *testing.T) {
	t := testc.New(test)

	jobName := "TestRunnableLock"

	t.Given("a JobsD instance")
	jd := testSetup(logrus.ErrorLevel)

	t.Given("a basic job")
	jd.RegisterJob(jobName, func(name string) error { return nil })

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.Given("a Runnable j0 has been created")
	params0 := []interface{}{"hello world!"}
	j0r := Run{
		NameActive: sql.NullString{Valid: true, String: faker.Name().String()},
		Job:        sql.NullString{Valid: true, String: jobName},
		JobArgs:    params0,
		RunAt:      time.Now(),
		CreatedAt:  time.Now(),
		CreatedBy:  jd.instance.ID,
	}
	j0r.insertGet(jd.db)

	j0, err1 := jd.buildRunnable(j0r)
	t.Assert.NoError(err1)
	t.Assert.Greater(j0.jobRun.ID, int64(0))
	t.Assert.Equal(jd.instance.ID, j0.jobRun.CreatedBy)

	t.Given("a Runnable j1 which the same as j0")
	j1r := Run{
		NameActive: j0r.NameActive,
		Job:        j0r.Job,
		JobArgs:    j0r.JobArgs,
		RunAt:      time.Now(),
		CreatedAt:  j0r.CreatedAt,
		CreatedBy:  j0r.CreatedBy,
	}
	j1r.insertGet(jd.db)

	j1, err2 := jd.buildRunnable(j1r)
	t.Assert.NoError(err2)
	t.Assert.Greater(j1.jobRun.ID, int64(0))
	t.Assert.Equal(j0.jobRun.ID, j1.jobRun.ID)

	t.Assert.Equal(j0r.NameActive, j1r.NameActive)
	t.Assert.Equal(j0r.Job, j1r.Job)
	t.Assert.Equal(j0r.JobArgs, j1r.JobArgs)
	t.Assert.WithinDuration(j0r.CreatedAt, j1r.CreatedAt, 1*time.Millisecond)
	t.Assert.Equal(j0r.CreatedBy, j1r.CreatedBy)

	t.When("j0 is locked")
	locked0 := j0.lock()
	t.Assert.True(locked0)

	t.Then("j0 should record the instance.ID that locked and started it")
	t.Assert.Equal(jd.instance.ID, j0.jobRun.RunStartedBy.Int64)

	t.Then("j1 should not be able to lock it")
	locked1 := j1.lock()
	t.Assert.False(locked1)

}

func TestRunnableReschedule(test *testing.T) {
	t := testc.New(test)

	t.Given("a JobsD instance")
	jd := testSetup(logrus.ErrorLevel)
	jd.Up()

	t.Given("arguments needed to create a runnable")
	jobRun := Run{Schedule: sql.NullString{Valid: true, String: ""}}
	jobFunc := NewJobFunc(func() error { return nil })

	timeToAdd := 200 * time.Minute
	t.Givenf("a scheduler that schedules things %s in advanced", timeToAdd.String())
	jobSchedule := ScheduleFunc(func(now time.Time) time.Time { return now.Add(timeToAdd) })

	t.Given("a chan that sends new job runs to be queued and executed")
	runQAdd := make(chan *Runnable)

	t.Given("a runnable")
	runnable, err := newRunnable(
		1,
		jobRun,
		jobFunc,
		&jobSchedule,
		runQAdd,
		jd.GetDB(),
		nil,
		jd.GetLogger(),
	)
	t.Assert.NoError(err)

	t.When("we reschedule the runnable")
	nextRunnable, err := runnable.reschedule(jd.GetDB())
	t.Assert.NoError(err)

	t.Thenf("we should get a new runnable that is scheduled to run %s from now", timeToAdd)
	t.Assert.WithinDuration(time.Now().Add(timeToAdd), nextRunnable.runAt(), time.Millisecond*50)

	testTeardown(jd)
}

func TestRunnablePanic(test *testing.T) {
	t := testc.New(test)

	jobName := "TestRunnablePanic" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := testSetup(logrus.ErrorLevel)

	t.Given("a Job that panics on the first run")
	wait := sync.WaitGroup{}
	var runStart uint32
	jobFunc := func() error {
		count := atomic.AddUint32(&runStart, 1)
		if count == 1 {
			panic("ITS ON FIRE!")
		}
		wait.Done()
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

	t.Then("the job should have run twice. 1 panic and 1 successful retry")
	wait.Wait()
	t.Assert.Equal(2, int(atomic.LoadUint32(&runStart)))

	t.Then("the job run should have completed within 1 second")
	t.Assert.WithinDuration(time.Now(), startTime, 1*time.Second)

	testTeardown(jd)
}

func TestRunnableCancel(test *testing.T) {
	t := testc.New(test)

	jobName := "TestRunnableCancel" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance")
	jd := testSetup(logrus.ErrorLevel)

	timeout := time.Millisecond * 300
	t.Given("a Job that times out or stops on RunInfo.Cancel")
	wait := sync.WaitGroup{}
	var runCount uint32
	var runTO uint32
	var runCancel uint32
	jobFunc := func(info RunInfo) error {
		ct := atomic.AddUint32(&runCount, 1)

		if ct == 1 {
			select {
			case <-time.After(timeout + 100*time.Millisecond):
				atomic.AddUint32(&runTO, 1)
			case <-info.Cancel:
				atomic.AddUint32(&runCancel, 1)
			}
		}
		wait.Done()
		return nil
	}

	t.Given("we register the job to the JobsD instance")
	jd.RegisterJob(jobName, jobFunc)

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.When("we run the job once with the timeout")
	wait.Add(2)
	_, err := jd.CreateRun(jobName).RunTimeout(timeout).Run()
	t.Assert.NoError(err)

	t.Then("the job should have run twice. 1 timeout and 1 successful retry")
	wait.Wait()
	t.Assert.Equal(2, int(atomic.LoadUint32(&runCount)))

	t.Then("the job should have canceled because of the timeout")
	t.Assert.Equal(1, int(atomic.LoadUint32(&runCancel)))
	t.Assert.Equal(0, int(atomic.LoadUint32(&runTO)))

	testTeardown(jd)
}
