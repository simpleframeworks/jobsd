# JobsS
A shared distributed and reliable database backed, job execution framework

[![Go Report Card](https://goreportcard.com/badge/github.com/simpleframeworks/jobspec)](https://goreportcard.com/report/github.com/simpleframeworks/jobspec) [![Test Status](https://github.com/simpleframeworks/jobspec/actions/workflows/testing.yml/badge.svg?branch=main)](https://github.com/simpleframeworks/jobspec/actions/workflows/testing.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/simpleframeworks/jobspec.svg)](https://pkg.go.dev/github.com/simpleframeworks/jobspec)
### Download it

```
go get -u github.com/simpleframeworks/jobspec
```

WORK IN PROGRESS

<!-- 
## How does it work? (in short)

- A db table is a queue.
- Every "job run" has an associated db row. 
- Instances in a cluster compete to acquire and run a job (without locking).
- A worker pool runs jobs.
- A new reoccurring scheduled "job run" is created after a "job run" is complete. 
- The db queue and the local JobsS instance queue are periodically synchronized.

## Quick Example

Announce the time every minute on the minute.

```go

jd := jobspec.New(db) // Create a JobsS service instance

// Register a Job that announces the time
jd.RegisterJob("Announce", func(name string) error {
  fmt.Printf("Hi %s! The date/time is %s.\n", name, time.Now().Format(time.RFC1123))
  return nil
})

// Register a schedule that tells JobsS when to trigger next
jd.RegisterSchedule("OnTheMin", func(now time.Time) time.Time {
  return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute()+1, 0, 0, now.Location())
})

jd.Up() // Bring up the JobsS service

// Create and schedule the job "Announce" to run "OnTheMin"
jd.CreateRun("Announce", "Simon").Schedule("OnTheMin").Run()


<-time.After(2*time.Minute) // Should really wait for OS kill signal here

jb.Down() // Shutdown the JobsS service instance, wait for running jobs to complete, record stats, and tidy up

```

A runnable example can be found in the [examples](examples/minute) folder. Just run it `go run main.go` from the directory.

## Basic Usage

### Creating jobs

The characteristics of a job is as follows:
- Jobs are just funcs
- Jobs must return an error
- Jobs can have a number of params
  - All job params must be serializable with [gob encoding](https://golang.org/pkg/encoding/gob/)
- Across a cluster all jobs should be named the same and have the same implementation.
  - Not all jobs need to implemented across the cluster (facilitates new code and new jobs)
- All jobs need to be registered before the instance `Up()` func is called
- The first argument can optional be of the type `jobspec.RunInfo`
  - RunInfo contains a `Cancel` channel for graceful shutdown / timeout amongst other things


```go
jobFunc1 := func() error {
  //DO SOME STUFF
  return nil
}

jobFunc2 := func(name string, age int) error {
  //DO SOME STUFF
  return nil
}

jd.RegisterJob("job1", jobFunc1)
jd.RegisterJob("job2", jobFunc2)
```

### Creating Schedules

A schedule is a simple function that takes in the current time and returns the next scheduled time.

- Schedules must be registered before the `Up()` func is called

```go
afterASecond := func(now time.Time) time.Time {
  return now.Add(time.Second)
}

onTheMin := func(now time.Time) time.Time {
  return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute()+1, 0, 0, now.Location())
})

onTheHour := func(now time.Time) time.Time {
  return time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
})

jd.RegisterSchedule("afterASecond", afterASecond)
jd.RegisterSchedule("onTheMin", onTheMin)
jd.RegisterSchedule("onTheHour", onTheHour)

```

### Running jobs

A **job run** is an instance of a job to be executed. A job must be registered first before creating a job run using `jd.CreateRun("job1", args...)`

```go
jobFunc := func(txt string) error {
  fmt.Printf("Hello %s!", txt)
  return nil
}
scheduleFunc := func(now time.Time) time.Time {
  return now.Add(time.Second)
}

jd := New(db)

jd.RegisterJob("job1", jobFunc)

jd.RegisterSchedule("schedule1", scheduleFunc)

jd.Up()

jd.CreateRun("job1", "World A").Run() // Run job1 once immediately
jd.CreateRun("job1", "World B").RunDelayed(time.Second) // Run job1 once after one second

jd.CreateRun("job1", "World C").Schedule("schedule1").Limit(2).Run() // Run job1 every second twice
jd.CreateRun("job1", "World D").Schedule("schedule1").Limit(2).RunAfter(time.Second) // After one second schedule job1 to run twice

// Runs only one "GlobalUniqueJob1" job at a time, across a cluster of JobsS instances
jd.CreateRun("job1", "World E").Unique("GlobalUniqueJob1").Run() 

// Runs and schedules only one "GlobalUniqueJob2" job at a time, across a cluster of JobsS instances. Runs only twice.
jd.CreateRun("job1", "World F").Schedule("schedule1").Limit(2).Unique("GlobalUniqueJob2").Run() 

<-time.After(10 * time.Second)
jd.Down() // Make sure to shutdown to cleanup and record stats
```

#### Getting the job run state:

```go

id, err := jd.CreateRun("job1", "World A").Run()
checkError(err)

runState := jd.GetRunState(id) // Get the run state of the job.

spew.Dump(runState.OriginID)
spew.Dump(runState.Name)
spew.Dump(runState.Job)
spew.Dump(runState.Schedule)
spew.Dump(runState.RunSuccessCount)
spew.Dump(runState.RunStartedAt)
spew.Dump(runState.RunStartedBy)
spew.Dump(runState.RunCompletedAt)
spew.Dump(runState.RunCompletedError)
spew.Dump(runState.RetriesOnErrorCount)
spew.Dump(runState.RetriesOnTimeoutCount)
spew.Dump(runState.CreatedAt)
spew.Dump(runState.CreatedBy)

err = runState.Refresh() // Refreshes the run state.
checkError(err)

```

## Advanced Usage

```go

jd := New(db)

jd.WorkerNum(10) // Set the number of workers to run the jobs

jd.PollInterval(10*time.Second) // The time between checks for new jobs across the cluster

jd.PollLimit(100) // The number of jobs to retrieve across the cluster at once

```
### Error handling

A job func needs to return an `error` . If an error is returned a job can be retried. You can set how many times a retry is attempted

**Error retries instance defaults**
```go
jd.RetriesErrorLimit(3) // How many times to retry a job when an error is returned (-1 = unlimited)
```

**Error retries can be set on the Job**
```go
jd.RegisterJob("job1", jobFunc).RetriesErrorLimit(2) // -1 = unlimited
```

**Error retries can be set on the Job Run**
```go
jd.CreateRun("job1", "World A").RetriesErrorLimit(2).Run() // -1 = unlimited
```

### Timeouts

IMPORTANT: Timeouts will not kill running jobs, they will keep running. In order to cancel a running job on time out, add the `jobspec.RunInfo` as the first argument in your job and use the `jobspec.RunInfo.Cancel` channel (see example below).

**Timeouts instance defaults**
```go
jd.RunTimeout(30*time.Minute) // How long before retrying a job (0 disables time outs)

jd.RetriesTimeoutLimit(3) // How many times to retry a job when it times out (-1 = unlimited)

jd.TimeoutCheck(10*time.Second) // The time between checks for jobs that have timed out (or crashed) on other nodes in the cluster
```

**Timeouts can be set on the Job**
```go
jd.RegisterJob("job1", jobFunc).RunTimeout(10*time.Minute).RetriesTimeoutLimit(2) 
// RunTimeout set to 0 disables time outs
// RetriesTimeoutLimit set to -1 = unlimited
```

**Timeouts can be set on a Job Run**
```go
jd.CreateRun("job1", "World A").RunTimeout(1*time.Minute).RetriesTimeoutLimit(5).Run() 
// RunTimeout set to 0 disables time outs
// RetriesTimeoutLimit set to -1 = unlimited
```

#### Canceling a job on timeout / shutdown

Create a job like the following
```go
jobFunc := func(info RunInfo) error {
			select {
			case <-time.After(timeout + 10*time.Second):
				fmt.Println("Did some work")
			case <-info.Cancel:
				fmt.Println("Job canceled")
			}
		return nil
}

jd.RegisterJob("CancelableJob", jobFunc)
```

### Database

PostgreSQL, SQLite, and MySQL are supported out of the box.

#### Using PostgreSQL
```go
host := os.Getenv("JOBSD_PG_HOST")
port := os.Getenv("JOBSD_PG_PORT")
dbname := os.Getenv("JOBSD_PG_DB")
user := os.Getenv("JOBSD_PG_USER")
password := os.Getenv("JOBSD_PG_PASSWORD")
dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable", host, user, password, dbname, port)

db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})

jd := New(db)
```

#### Using MySQL
```go
host := os.Getenv("JOBSD_MY_HOST")
port := os.Getenv("JOBSD_MY_PORT")
dbname := os.Getenv("JOBSD_MY_DB")
user := os.Getenv("JOBSD_MY_USER")
password := os.Getenv("JOBSD_MY_PASSWORD")
dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", user, password, host, port, dbname)

db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
  Logger: logc.NewGormLogger(logger),
})
checkError(err)

jd := New(db)
```

#### Using SQLite
```go
db, err0 := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
  Logger: logc.NewGormLogger(logger),
})

sqlDB, err := db.DB()
checkError(err)

// SQLLite does not work with concurrent connections
sqlDB.SetMaxIdleConns(1)
sqlDB.SetMaxOpenConns(1)

jd := New(db)
```

### Disable Auto Migrations

Auto migrations create the DB tables and structure required for JobsS. It is run when starting JobsS `Up()`. Auto migrations are only required the first time JobsS runs so it can be disabled using the following method.

```go
jd.AutoMigrate(false)

// Register Jobs and Schedules etc...

jd.Up()
```

### Logging

A logger can be supplied. The logger must implement the [logc interface](https://github.com/simpleframeworks/logc)

```go
jd := New(db)
jd.Logger(logger)
``` -->