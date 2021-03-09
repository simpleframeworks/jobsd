package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/simpleframeworks/jobsd"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupDB() *gorm.DB {
	// Create a database
	db, _ := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	sqlDB, _ := db.DB()

	// SQLLite does not work well with concurrent connections so we set it to have one
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetMaxOpenConns(1)

	return db
}

func main() {

	db := setupDB()

	jd := jobsd.New(db) // Create a JobsD service instance

	// Register a Job that announces the time
	jd.RegisterJob("Announce", func(name string) error {
		fmt.Printf("Hi %s! The date/time is %s.\n", name, time.Now().Format(time.RFC1123))
		return nil
	})

	// Register a schedule that tells JobsD when to trigger next
	jd.RegisterSchedule("OnTheMin", func(now time.Time) time.Time {
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute()+1, 0, 0, now.Location())
	})

	jd.Up() // Bring up the JobsD service

	// Create and schedule the job "Announce" to run "OnTheMin"
	jd.CreateRun("Announce", "Simon").Schedule("OnTheMin").Run()

	// Pause until os signal e.g ctrl-z
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Started")

	<-sigs // Wait for an OS signal

	jd.Down() // Shutdown the JobsD service

	fmt.Println("Completed")

}
