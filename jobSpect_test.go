package jobspec

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
	"github.com/sirupsen/logrus"
)

func TestJobSpecBasicJob1(testT *testing.T) {
	t := testc.New(testT)

	jobName := "TestJobSpec"

	t.Given("a JobSpec instance")
	inst := testSetup(logrus.ErrorLevel)

	t.Given("a func that increments a counter")
	var counter uint32
	wait := &sync.WaitGroup{}
	jobFunc := func() error {
		atomic.AddUint32(&counter, 1)
		defer wait.Done()
		return nil
	}

	t.Given("a new job that runs the func")
	job, err0 := inst.NewJob(jobName, jobFunc).Register()

	t.Assert.NoError(err0)
	t.Assert.NotNil(job)

	t.When("we start the JobSpec instance")
	err1 := inst.Start()

	t.Assert.NoError(err1)

	t.When("we get and run the job")
	_, err2 := inst.GetJob(jobName).Run()

	t.Assert.NoError(err2)

	t.When("we wait for it to run")
	t.WaitTimeout(wait, time.Second*5)

	t.Then("the job should have run and incremented the counter")
	count := int(atomic.LoadUint32(&counter))

	t.Assert.Equal(1, count)

}

func TestJobSpecBasicJob2(testT *testing.T) {
	t := testc.New(testT)

	jobName := "TestJobSpec"

	t.Given("a JobSpec instance")
	inst := testSetup(logrus.ErrorLevel)

	t.Given("a func that increments a counter")
	var counter uint32
	wait := &sync.WaitGroup{}
	jobFunc := func() error {
		atomic.AddUint32(&counter, 1)
		defer wait.Done()
		return nil
	}

	t.Given("a new job that runs the func")
	job, err0 := inst.NewJob(jobName, jobFunc).Register()

	t.Assert.NoError(err0)
	t.Assert.NotNil(job)

	t.When("we start the JobSpec instance")
	err1 := inst.Start()

	t.Assert.NoError(err1)

	t.When("run the job")
	_, err2 := job.Run()

	t.Assert.NoError(err2)

	t.When("we wait for it to run")
	t.WaitTimeout(wait, time.Second*5)

	t.Then("the job should have run and incremented the counter")
	count := int(atomic.LoadUint32(&counter))

	t.Assert.Equal(1, count)

}
