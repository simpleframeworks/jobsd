package jobsd

import (
	"database/sql"
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
	"syreclabs.com/go/faker"
)

func TestRunnableLock(test *testing.T) {
	t := testc.New(test)

	logger := setupLogging(logrus.ErrorLevel)
	db := setupDB(logger)
	jobName := "TestRunnableLock" // Must be unique otherwise tests may collide

	t.Given("a JobsD instance with a basic job")
	jd := New(db).Logger(logger)
	jd.RegisterJob(jobName, func(name string) error { return nil })

	t.When("we bring up the JobsD instance")
	t.Assert.NoError(jd.Up())

	t.Given("a Runnable j0 has been created")
	params0 := []interface{}{"hello world!"}
	j0r := Run{
		NameActive: sql.NullString{Valid: true, String: faker.Name().String()},
		Job:        jobName,
		JobArgs:    params0,
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
