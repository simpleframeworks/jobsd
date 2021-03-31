package jobsd

import (
	"fmt"
	"testing"

	"github.com/simpleframeworks/testc"
)

func TestJobFuncNotFunc(test *testing.T) {
	t := testc.New(test)

	t.Given("we create a JobFunc with an int instead of a func")
	jf := NewJobFunc(int(123))

	t.When("we run the validation check")
	err := jf.check([]interface{}{})

	t.Then("it should return the not func error")
	t.Require.NotNil(err)
	t.Assert.Equal(ErrJobFuncNotFunc, err)
}
func TestJobFuncNotFuncWithErr(test *testing.T) {

	theTest := func(jobFunc interface{}) func(*testing.T) {
		return func(test *testing.T) {
			t := testc.New(test)

			t.Given("we create a JobFunc with a func that does not return an error")
			jf := NewJobFunc(jobFunc)

			t.When("we run the validation check")
			err := jf.check([]interface{}{})

			t.Then("it should return the 'func no error return' error")
			t.Require.NotNil(err)
			t.Assert.Equal(ErrJobFuncNoErrRtn, err)
		}
	}

	test.Run("Job Func no rtn", theTest(func() {}))
	test.Run("Job Func incorrect type rtn", theTest(func() int { return 1 }))
}

func TestJobFuncArgsMismatch(test *testing.T) {

	theTest := func(args []interface{}) func(test *testing.T) {
		return func(test *testing.T) {
			t := testc.New(test)

			t.Given("we create a JobFunc with a func that takes 3 int args")
			jobFunc := func(a, b, c int) error {
				fmt.Printf("%d, %d, %d", a, b, c)
				return nil
			}
			jf := NewJobFunc(jobFunc)

			t.When("we run the validation check with intended args")
			err := jf.check(args)

			t.Then("it should return the 'func args mismatch' error")
			t.Require.NotNil(err)
			t.Assert.ErrorIs(err, ErrJobFuncArgsMismatch)
		}
	}

	test.Run("Job Func wrong args num 1", theTest([]interface{}{1}))
	test.Run("Job Func wrong args num 2", theTest([]interface{}{1, 2}))
	test.Run("Job Func wrong args type - all", theTest([]interface{}{"1", "2", "3"}))
	test.Run("Job Func wrong args type - 1", theTest([]interface{}{1, 2, "3"}))
	test.Run("Job Func wrong args type - 2", theTest([]interface{}{1, "2", "3"}))
}
func TestJobFuncExecute(test *testing.T) {
	t := testc.New(test)

	t.Given("we create a JobFunc with a func that takes 3 args and collects the results")
	results := ""
	jobFunc := func(a, b int, c string) error {
		results = fmt.Sprintf("%d, %d, %s", a, b, c)
		return nil
	}
	jf := NewJobFunc(jobFunc)

	t.Given("the correct arguments to execute the job func")
	args := []interface{}{1, 2, "3"}

	t.When("we execute the JobFunc")
	err := jf.execute(args)
	t.Assert.Nil(err)

	t.Then("the results contain an expected string")
	t.Assert.Equal("1, 2, 3", results)
}
