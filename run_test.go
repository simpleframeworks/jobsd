package jobsd

import (
	"database/sql"
	"testing"

	"github.com/simpleframeworks/testc"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"syreclabs.com/go/faker"
)

func TestRunCreate(test *testing.T) {
	t := testc.New(test)

	t.Given("a database connection")
	db, err0 := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	t.Assert.NoError(err0)

	t.Given("a Run has been synced")
	db.AutoMigrate(&Run{})
	params0 := []interface{}{"hello world!"}
	j0 := Run{
		NameActive: sql.NullString{Valid: true, String: faker.Name().String()},
		JobArgs:    params0,
	}
	err1 := j0.insertGet(db)
	t.Assert.NoError(err1)
	t.Assert.Greater(j0.ID, int64(0))

	t.When("a Run with the same NameActive")
	j1 := Run{
		NameActive: j0.NameActive,
		Job:        j0.Job,
		Schedule:   j0.Schedule,
	}
	err2 := j1.insertGet(db)
	t.Assert.NoError(err2)
	t.Assert.Greater(j1.ID, int64(0))

	t.Then("its IDs are updated to match")
	t.Assert.Equal(j0.ID, j1.ID)

	t.Then("its JobParams are update to match ")
	t.Assert.Equal(j0.JobArgs, j1.JobArgs)
}
