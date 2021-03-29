package jobsd

import (
	"bytes"
	"database/sql/driver"
	"encoding/gob"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm/schema"
)

// JobFunc .
type JobFunc struct {
	jobFunc reflect.Value
}

var (
	//ErrJobFuncNotFunc Job Func not a func
	ErrJobFuncNotFunc = errors.New("jobFunc is not a func")
	// ErrJobFuncNoErrRtn Job Func does not return an error
	ErrJobFuncNoErrRtn = errors.New("jobFunc needs to return an error")
	// ErrJobFuncArgsMismatch Calling Job Func args are mismatched
	ErrJobFuncArgsMismatch = errors.New("jobFunc calling args mismatch")
)

// check throws an error if the func is not valid and the args don't match func args
func (j *JobFunc) check(args []interface{}) error {
	if j.jobFunc.Kind() != reflect.Func {
		return ErrJobFuncNotFunc
	}

	theType := j.jobFunc.Type()
	// We expect 1 return value
	if theType.NumOut() != 1 {
		return ErrJobFuncNoErrRtn
	}

	// We expect the return value is an error
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !theType.Out(0).Implements(errorInterface) {
		return ErrJobFuncNoErrRtn
	}

	// We expect the number of jobFunc args matches
	if theType.NumIn() != len(args) {
		return ErrJobFuncArgsMismatch
	}

	// We expect the supplied args types are equal to the jobFuncs args
	for i := 0; i < theType.NumIn(); i++ {
		if reflect.ValueOf(args[i]).Kind() != theType.In(i).Kind() {
			return fmt.Errorf("%w, arg %d is not a %s", ErrJobFuncArgsMismatch, i, theType.In(i).Kind().String())
		}
	}

	return nil
}

// paramsCount returns the number of parameters required
func (j *JobFunc) paramsCount() int {
	return j.jobFunc.Type().NumIn()
}

// firstParamJR . Is the first param a Job Runnable struct?
func (j *JobFunc) firstParamJR() bool {
	//TODO
	return false
}

// execute the JobFunc
// there is no param validation, use the check func
func (j *JobFunc) execute(params []interface{}) error {
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	res := j.jobFunc.Call(in)

	if len(res) != 1 {
		return errors.New("func does not return a value")
	}

	if err, ok := res[0].Interface().(error); ok && err != nil {
		return err
	}
	return nil
}

// NewJobFunc .
func NewJobFunc(theFunc interface{}) *JobFunc {
	return &JobFunc{
		jobFunc: reflect.ValueOf(theFunc),
	}
}

// JobContainer .
type JobContainer struct {
	jobFunc             *JobFunc
	retryTimeout        time.Duration
	retryOnErrorLimit   int
	retryOnTimeoutLimit int
}

// RetryTimeout set the job default timeout
func (j *JobContainer) RetryTimeout(timeout time.Duration) *JobContainer {
	j.retryTimeout = timeout
	return j
}

// RetryErrorLimit set the job default number of retries on error
func (j *JobContainer) RetryErrorLimit(limit int) *JobContainer {
	j.retryOnErrorLimit = limit
	return j
}

// RetryTimeoutLimit set the job default number of retries on timeout
func (j *JobContainer) RetryTimeoutLimit(limit int) *JobContainer {
	j.retryOnTimeoutLimit = limit
	return j
}

// JobArgs holds job func parameters used to run a job. It can be serialized for DB storage
type JobArgs []interface{}

// GormDataType .
func (p JobArgs) GormDataType() string {
	return string(schema.Bytes)
}

// Scan scan value into []
func (p *JobArgs) Scan(value interface{}) error {
	data, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal params value:", value))
	}
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	return dec.Decode(p)
}

// Value return params value, implement driver.Valuer interface
func (p JobArgs) Value() (driver.Value, error) {
	if len(p) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(p)
	return buf.Bytes(), nil
}
