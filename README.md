# JobsD
A distributed and reliable database backed, job execution framework

### Download it

```
go get -u github.com/simpleframeworks/jobsd
```

## Quick Example

Announce the time every hour on the hour.

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

```

