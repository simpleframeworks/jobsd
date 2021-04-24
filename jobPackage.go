package jobsd

import "time"

// JobPackage defines a complete implementation of a job that can run and optionally be scheduled
type JobPackage interface {
	// Name the package name
	Name() string

	// Unique if true ensures only one package Job func is running at a time across the cluster.
	// The Name is used to deduplicate running Job funcs
	Unique() bool

	// Job is any work that needs to be done
	Job(info RunInfo) error

	// Timeout determines how long to wait till we cleanup a running Job and then send a RunInfo.Cancel message
	// A timeout of 0 will disable this and let a job run indefinitely
	Timeout() time.Duration

	// RetriesOnError determines how many times to retry a Job if it returns an error
	//  0 will disable retries
	// -1 will retry a Job on error indefinitely
	RetriesOnError() int

	// RetriesOnTimeout determines how many times to retry a Job if it times out
	//  0 will disable retries
	// -1 will retry a Job on timeout indefinitely
	RetriesOnTimeout() int

	// Schedule if true schedules the Job
	// if false the job is run immediately and only runs once
	Schedule() bool

	// Scheduler given a time return the next time the job should run
	Scheduler(now time.Time) time.Time

	// Limit sets the number of times a Job will run (ignoring errors and timeouts)
	// 0 or -1 will ensure the Job is scheduled to run indefinitely
	Limit() int

	// Delay adds a delay before scheduling or running the job the very first time
	Delay() time.Duration

	// AutoRun runs the job as soon as the JobsD instance starts up
	// A delay can be introduced using the Delay func
	AutoRun() bool
}

type JobPackageCreator struct{}
