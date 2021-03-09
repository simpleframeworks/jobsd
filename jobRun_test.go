package jobsd

import (
	"database/sql"
	"testing"

	"github.com/simpleframeworks/testc"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"syreclabs.com/go/faker"
)

func TestJobRunCreate(test *testing.T) {
	t := testc.New(test)

	t.Given("a database connection")
	db, err0 := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	t.NoError(err0)

	t.Given("a JobRun has been synced")
	db.AutoMigrate(&JobRun{})
	params0 := []interface{}{"hello world!"}
	j0 := JobRun{
		NameActive: sql.NullString{Valid: true, String: faker.Name().String()},
		JobArgs:    params0,
	}
	err1 := j0.insertGet(db)
	t.NoError(err1)
	t.Greater(j0.ID, int64(0))

	t.When("a JobRun with the same NameActive")
	j1 := JobRun{
		NameActive: j0.NameActive,
		Job:        j0.Job,
		Schedule:   j0.Schedule,
	}
	err2 := j1.insertGet(db)
	t.NoError(err2)
	t.Greater(j1.ID, int64(0))

	t.Then("its IDs are updated to match")
	t.Equal(j0.ID, j1.ID)

	t.Then("its JobParams are update to match ")
	t.Equal(j0.JobArgs, j1.JobArgs)
}

func TestJobRunLock(test *testing.T) {
	t := testc.New(test)

	t.Given("a database connection")
	db, err0 := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	t.NoError(err0)

	t.Given("a JobRun j0 has been created")
	db.AutoMigrate(&JobRun{})
	params0 := []interface{}{"hello world!"}
	j0 := JobRun{
		NameActive: sql.NullString{Valid: true, String: faker.Name().String()},
		JobArgs:    params0,
	}
	err1 := j0.insertGet(db)
	t.NoError(err1)
	t.Greater(j0.ID, int64(0))

	t.Given("a JobRun j1 which the same as j0")
	j1 := JobRun{
		NameActive: j0.NameActive,
		Job:        j0.Job,
		Schedule:   j0.Schedule,
	}
	err2 := j1.insertGet(db)
	t.NoError(err2)
	t.Greater(j1.ID, int64(0))
	t.Equal(j0.ID, j1.ID)
	t.Equal(j0.JobArgs, j1.JobArgs)

	t.Given("an instance ID")
	instanceID := int64(1)

	t.When("j0 is locked")
	locked0, err3 := j0.lockStart(db, instanceID)
	t.NoError(err3)
	t.True(locked0)

	t.Then("j0 should record the instanceID that locked and started it")
	t.Equal(instanceID, j0.RunStartedBy.Int64)

	t.Then("j1 should not be able to lock it")
	locked1, err4 := j1.lockStart(db, instanceID)
	t.NoError(err4)
	t.False(locked1)
}
