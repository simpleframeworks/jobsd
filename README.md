# JobsD
A distributed and reliable database backed, job execution framework

[![Go Report Card](https://goreportcard.com/badge/github.com/simpleframeworks/jobsd)](https://goreportcard.com/report/github.com/simpleframeworks/jobsd) [![Test Status](https://github.com/simpleframeworks/jobsd/actions/workflows/testing.yml/badge.svg?branch=main)](https://github.com/simpleframeworks/jobsd/actions/workflows/testing.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/simpleframeworks/jobsd.svg)](https://pkg.go.dev/github.com/simpleframeworks/jobsd)
### Download it

```
go get -u github.com/simpleframeworks/jobsd
```

## How does it work? (in short)

- A db table is a queue.
- Every "job run" has an associated db row. 
- Instances in a cluster compete to acquire and run a job (without locking).
- A worker pool runs jobs.
- A new reoccurring scheduled "job run" is created after a "job run" is complete. 
- The db queue and the local JobsD instance queue are periodically synchronized.

## Quick Example

Announce the time every minute on the minute.

```go

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


// .... DO STUFF and wait for sig kill

jb.Down() // Shutdown the JobsD service instance, wait for running jobs to complete and tidy up

```

A runnable example can be found in the [examples](examples/minute) folder. Just run it `go run main.go`.

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


Examples of jobs:
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

Examples:
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

Example
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
jd.CreateRun("job1", "World D").Schedule("schedule1").Limit(2).RunDelayed(time.Second) // Schedule job1 to run after one second twice

// Runs only one "GlobalUniqueJob1" at a time, across a cluster of JobsD instances
jd.CreateRun("job1", "World E").Unique("GlobalUniqueJob1").Run() 

// Runs and schedules only one "GlobalUniqueJob2" at a time, across a cluster of JobsD instances
jd.CreateRun("job1", "World F").Schedule("schedule1").Limit(2).Unique("GlobalUniqueJob2").Run() 

<-time.After(time.Duration(5) * time.Second)
jd.Down()
```

Getting the job run state:

```go

id, err := jd.CreateRun("job1", "World A").Run()
checkError(err)

runState := jd.GetJobRunState(id) // Get the run state of the job.

spew.Dump(runState.OriginID)
spew.Dump(runState.Name)
spew.Dump(runState.RunCount)
spew.Dump(runState.RunStartedAt)
spew.Dump(runState.RunStartedBy)
spew.Dump(runState.RunCompletedAt)
spew.Dump(runState.RunCompletedError)
spew.Dump(runState.RetriesOnErrorCount)
spew.Dump(runState.RetriesOnTimeoutCount)
spew.Dump(runState.Schedule)
spew.Dump(runState.ClosedAt)
spew.Dump(runState.ClosedBy)
spew.Dump(runState.CreatedAt)
spew.Dump(runState.CreatedBy)

err = runState.Refresh() // Refreshes the run state.
checkError(err)

```

## Advanced Usage

```go

jd := New(db)

jd.WorkerNum(10) // Set the number of workers to run the jobs

jd.JobPollInterval(10*time.Second) // The time between checks for new jobs across the cluster
jd.JobPollLimit(100) // The number of jobs to retrieve across the cluster

jd.JobRetryErrorLimit(3) // How many times to retry a job when an error is returned

```

### Timeouts

Timeouts are mainly used to resurrect jobs after an instance crashes. If an instance crashes or is killed in a cluster, another instance will pickup the job after the timeout and run it.

NB Timeouts will not kill running jobs, so its important to set timeouts as long as possible, otherwise the same job may run twice.

**Timeouts instance defaults**
```go
jd.JobRetryTimeout(30*time.Minute) // How long before retrying a job
jd.JobRetryTimeoutLimit(3) // How many retires to attempt before giving up
jd.JobRetryTimeoutCheck(1*time.Minute) // Check for timeouts interval 
```

**Timeouts can be set on the Job**
```go
jd.RegisterJob("job1", jobFunc).RetryTimeout(10*time.Minute).RetryTimeoutLimit(2)

```

**TBA Timeouts can be set on a Job Run**
```go
// TBA
// jd.CreateRun("job1", "World A").RetryTimeout(1*time.Minute).RetryTimeoutLimit(5)
```

### Logging

A logger can be supplied. The logger must implement the [logc interface](https://github.com/simpleframeworks/logc)

```go
jd := New(db)
jd.Logger(logger)
```