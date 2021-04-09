package jobsd

import (
	"fmt"
	"testing"

	"github.com/simpleframeworks/testc"
)

func TestRunInfoJobFuncDetected(test *testing.T) {
	t := testc.New(test)

	t.Given("a func with RunInfo as the first arg")
	theJob := func(ri RunInfo) {
		fmt.Println("Hi!")
	}

	t.When("we create a JobFunc using the func")
	theJobFunc := NewJobFunc(theJob)

	t.Then("the JobFunc should recognize it has a RunInfo arg")
	t.Assert.True(theJobFunc.runInfoArg)
}

func TestRunInfoJobFuncNotDetected(test *testing.T) {
	t := testc.New(test)

	t.Given("a func with a string as the first arg")
	theJob := func(ri string) {
		fmt.Println("Hi!")
	}

	t.When("we create a JobFunc using the func")
	theJobFunc := NewJobFunc(theJob)

	t.Then("the JobFunc should know it does not have a RunInfo arg")
	t.Assert.False(theJobFunc.runInfoArg)
}
