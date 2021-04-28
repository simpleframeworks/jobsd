package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/simpleframeworks/jobspec"
	"github.com/simpleframeworks/logc"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// The main purpose of this is setup the database structure for testing

func main() {
	logger := testSetupLogging(logrus.ErrorLevel)

	logger.Debug("Connecting to DB")
	db := testSetupDB(logger)

	logger.Debug("Auto Migrate DB")
	db.AutoMigrate(&jobspec.Run{}, &jobspec.Instance{})

	tx := db.Session(&gorm.Session{
		AllowGlobalUpdate: true,
	})

	logger.Debug("Cleaning up")
	tx.Delete(&jobspec.Run{})
	tx.Delete(&jobspec.Instance{})
}

// testSetup for testing
func testSetup(logLvl logrus.Level) *jobspec.JobsS {

	logger := testSetupLogging(logLvl)
	db := testSetupDB(logger)

	if dbToUse() == "" || dbToUse() == "sqllite" {
		return jobspec.New(db).Logger(logger)
	}

	// Auto migrations are disabled for MySQL and PostgreSQL as structure should be
	return jobspec.New(db.Begin()).AutoMigration(false).Logger(logger)
}

// testTeardown JobsS after testing
func testTeardown(j *jobspec.JobsS) {
	err := j.Down()
	testPanicErr(err)

	j.GetDB().Rollback()
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

	return db
}
