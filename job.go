package jobss

import (
	"bytes"
	"database/sql"
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
	runInfoArg bool
	jobFunc    reflect.Value
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

	// Make sure its a func
	if j.jobFunc.Kind() != reflect.Func {
		return ErrJobFuncNotFunc
	}

	theType := j.jobFunc.Type()
	// Make sure it returns 1 value
	if theType.NumOut() != 1 {
		return ErrJobFuncNoErrRtn
	}

	// Make sure the return value is an error
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !theType.Out(0).Implements(errorInterface) {
		return ErrJobFuncNoErrRtn
	}

	// Make sure the number of jobFunc args matches
	lenArgs := len(args)
	if j.runInfoArg {
		lenArgs++
	}
	if theType.NumIn() != lenArgs {
		return ErrJobFuncArgsMismatch
	}

	// Make sure the supplied args types are equal to the jobFuncs args
	start := 0
	if j.runInfoArg {
		start = 1
	}
	for i := start; i < lenArgs; i++ {
		if reflect.ValueOf(args[i-start]).Kind() != theType.In(i).Kind() {
			return fmt.Errorf("%w, arg %d is not a %s", ErrJobFuncArgsMismatch, i, theType.In(i).Kind().String())
		}
	}

	return nil
}

var riType = reflect.TypeOf(RunInfo{})

// setRunInfoArg. Checks to see if first arg is a Run Info struct?
func (j *JobFunc) setRunInfoArg() {
	theType := j.jobFunc.Type()

	// Make sure its a func
	if j.jobFunc.Kind() != reflect.Func {
		j.runInfoArg = false
		return
	}

	// Make sure there is at least one arg
	if theType.NumIn() < 1 {
		j.runInfoArg = false
		return
	}

	theArg := theType.In(0)
	// Make sure the first arg is a struct
	if theArg.Kind() != reflect.Struct {
		j.runInfoArg = false
		return
	}

	// Make sure the arg is a RunInfo struct
	if theArg.AssignableTo(riType) {
		j.runInfoArg = true
	}
}

// execute the JobFunc
// there is no param validation, use the check func
func (j JobFunc) execute(r RunInfo, params []interface{}) error {
	if j.runInfoArg {
		// Add run info as the first arg of the job func
		params = append([]interface{}{r}, params...)
	}
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
func NewJobFunc(theFunc interface{}) JobFunc {
	rtn := JobFunc{
		jobFunc: reflect.ValueOf(theFunc),
	}
	rtn.setRunInfoArg()
	return rtn
}

// JobContainer .
type JobContainer struct {
	jobFunc             JobFunc
	runTimeout          sql.NullInt64
	retriesTimeoutLimit sql.NullInt64
	retriesErrorLimit   sql.NullInt64
}

// RunTimeout sets the default timeout of a job run
// Setting it to 0 disables timeout
func (j *JobContainer) RunTimeout(timeout time.Duration) *JobContainer {
	if timeout <= 0 {
		j.runTimeout = sql.NullInt64{}
	} else {
		j.runTimeout = sql.NullInt64{Valid: true, Int64: int64(timeout)}
	}
	return j
}

// RetriesTimeoutLimit sets how many times a job run can timeout
// Setting it to -1 removes the limit
func (j *JobContainer) RetriesTimeoutLimit(limit int) *JobContainer {
	if limit < 0 {
		j.retriesTimeoutLimit = sql.NullInt64{}
	} else {
		j.retriesTimeoutLimit = sql.NullInt64{Valid: true, Int64: int64(limit)}
	}
	return j
}

// RetriesErrorLimit sets the RetriesErrorLimit
// Setting it to -1 removes the limit
func (j *JobContainer) RetriesErrorLimit(limit int) *JobContainer {
	if limit < 0 {
		j.retriesErrorLimit = sql.NullInt64{}
	} else {
		j.retriesErrorLimit = sql.NullInt64{Valid: true, Int64: int64(limit)}
	}
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
