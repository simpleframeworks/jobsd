package testc

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// New .
func New(t *testing.T) *TestC {
	return &TestC{
		t:          t,
		Assertions: assert.New(t),
	}
}

// TestC extends the standard testing functions with assertions and BDD logging
type TestC struct {
	t *testing.T
	*assert.Assertions
}

func (t *TestC) logItf(name string, format string, args ...interface{}) {
	t.t.Helper()
	t.t.Logf(name+" "+format, args...)
}

// Givenf prepends the BDD GIVEN word
func (t *TestC) Givenf(format string, args ...interface{}) {
	t.t.Helper()
	t.logItf("GIVEN", format, args...)
}

// Whenf prepends the BDD WHEN word
func (t *TestC) Whenf(format string, args ...interface{}) {
	t.t.Helper()
	t.logItf("WHEN", format, args...)
}

// Thenf prepends the BDD THEN word
func (t *TestC) Thenf(format string, args ...interface{}) {
	t.t.Helper()
	t.logItf("THEN", format, args...)
}

func (t *TestC) logIt(name string, args ...interface{}) {
	t.t.Helper()
	newArgs := append([]interface{}{}, name)
	newArgs = append(newArgs, args...)
	t.t.Log(newArgs...)
}

// Given prepends the BDD GIVEN word
func (t *TestC) Given(args ...interface{}) {
	t.t.Helper()
	t.logIt("GIVEN", args...)
}

// When prepends the BDD WHEN word
func (t *TestC) When(args ...interface{}) {
	t.t.Helper()
	t.logIt("WHEN", args...)
}

// Then prepends the BDD THEN word
func (t *TestC) Then(args ...interface{}) {
	t.t.Helper()
	t.logIt("THEN", args...)
}

// WaitTimeout waits for the sync group or times out and fails
func (t *TestC) WaitTimeout(wait *sync.WaitGroup, timeout time.Duration) {
	t.t.Helper()

	finished := make(chan struct{})
	go func() {
		wait.Wait()
		finished <- struct{}{}
	}()
	select {
	case <-finished:
		break
	case <-time.After(timeout):
		t.FailNow("timeout triggered")
	}
}
