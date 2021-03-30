package jobsd

import (
	"database/sql"
	"testing"

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
	t.NoError(jd.Up())

	t.Given("a Runnable j0 has been created")
	params0 := []interface{}{"hello world!"}
	j0r := Run{
		NameActive: sql.NullString{Valid: true, String: faker.Name().String()},
		Job:        jobName,
		JobArgs:    params0,
	}
	j0, err1 := jd.buildRunnable(j0r)
	t.NoError(err1)
	t.Greater(j0.ID, int64(0))

	t.Given("a Runnable j1 which the same as j0")
	j1r := Run{
		NameActive: j0r.NameActive,
		Job:        jobName,
		JobArgs:    params0,
	}
	j1, err2 := jd.buildRunnable(j1r)
	t.NoError(err2)
	t.Greater(j1.ID, int64(0))
	t.Equal(j0.ID, j1.ID)
	t.Equal(j0.JobArgs, j1.JobArgs)

	t.When("j0 is locked")
	locked0 := j0.lock()
	t.True(locked0)

	t.Then("j0 should record the instance.ID that locked and started it")
	t.Equal(jd.instance.ID, j0.RunStartedBy)

	t.Then("j1 should not be able to lock it")
	locked1 := j1.lock()
	t.False(locked1)

}
