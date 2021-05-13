package jobspec

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// testSetup for testing
func testSetup(logLvl logrus.Level) *Instance {

	logger := testSetupLogging(logLvl)

	db := testSetupDB(logger)

	if dbToUse() == "" || dbToUse() == "sqllite" {
		return New(db).SetLogger(logger)
	}

	// DB migrations / setup are disabled for MySQL and PostgreSQL as structure should be there
	return New(db).SetMigration(false).SetLogger(logger)
}

// testTeardown Instance after testing
func testTeardown(j *Instance) {
	err := j.Stop()
	testPanicErr(err)

	sqlDB, err := j.GetDB().DB()
	testPanicErr(err)
	sqlDB.Close()
}

func dbToUse() string {
	return strings.ToLower(strings.TrimSpace(os.Getenv("JOBSD_DB")))
}

// testSetupDB .
func testSetupDB(logger logc.Logger) *gorm.DB {
	dbToUse := dbToUse()

	if dbToUse == "" || dbToUse == "sqllite" {
		return testSetupSQLLite(logger)
	}
	if dbToUse == "postgres" {
		return testSetupPostgreSQL(logger)
	}
	if dbToUse == "mysql" {
		return testSetupMySQL(logger)
	}
	return nil
}

// testCloseDB .
func testCloseDB(db *gorm.DB) {

	con, err := db.DB()
	testPanicErr(err)

	con.Close()
}

// testPanicErr .
func testPanicErr(err error) {
	if err != nil {
		panic(err)
	}
}

// testSetupLogging .
func testSetupLogging(level logrus.Level) logc.Logger {
	log := logrus.New()
	log.SetLevel(level)
	return logc.NewLogrus(log)
}

// testSetupSQLLite .
func testSetupSQLLite(logger logc.Logger) *gorm.DB {
	db, err0 := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		// Logger: logc.NewGormLogger(logger),
	})

	sqlDB, err := db.DB()
	testPanicErr(err)

	// SQLLite does not work well with concurrent connections
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetMaxOpenConns(1)

	testPanicErr(err0)
	return db
}

// testSetupPostgreSQL .
func testSetupPostgreSQL(logger logc.Logger) *gorm.DB {
	host := os.Getenv("JOBSD_PG_HOST")
	port := os.Getenv("JOBSD_PG_PORT")
	dbname := os.Getenv("JOBSD_PG_DB")
	user := os.Getenv("JOBSD_PG_USER")
	password := os.Getenv("JOBSD_PG_PASSWORD")
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable", host, user, password, dbname, port)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logc.NewGormLogger(logger),
	})
	testPanicErr(err)

	// sqlDB, err := db.DB()
	// testPanicErr(err)
	// sqlDB.SetMaxIdleConns(0)
	// sqlDB.SetMaxOpenConns(1)

	return db
}

// testSetupMySQL .
func testSetupMySQL(logger logc.Logger) *gorm.DB {
	host := os.Getenv("JOBSD_MY_HOST")
	port := os.Getenv("JOBSD_MY_PORT")
	dbname := os.Getenv("JOBSD_MY_DB")
	user := os.Getenv("JOBSD_MY_USER")
	password := os.Getenv("JOBSD_MY_PASSWORD")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", user, password, host, port, dbname)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logc.NewGormLogger(logger),
	})
	testPanicErr(err)

	// sqlDB, err := db.DB()
	// testPanicErr(err)
	// sqlDB.SetMaxIdleConns(1)
	// sqlDB.SetMaxOpenConns(1)

	return db
}

// testFuncName Get the name of the running function
func testFuncName() string {
	pc := make([]uintptr, 1)
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	sl := strings.Split(f.Name(), "/")
	return sl[len(sl)-1]
}
