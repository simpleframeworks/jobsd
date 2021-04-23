package jobsd

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
)

func TestRunQueue(test *testing.T) {
	t := testc.New(test)

	t.Given("a job run queue")
	q := NewRunnableQueue()

	t.Given("the queue has job runs")
	for i := 9; i >= 0; i-- {
		chr := &Runnable{
			jobRun: &Run{
				Name:       fmt.Sprintf("%d", i),
				NameActive: sql.NullString{Valid: true, String: fmt.Sprintf("%d", i)},
				RunAt:      time.Now(),
			},
		}
		q.Push(chr)
	}

	t.When("we pop the job runs off the queue")
	runOrder := []string{}
	for i := 0; i < 10; i++ {
		topItem := q.Peek()
		j := q.Pop()
		t.Assert.Equal(topItem.jobRun.Name, j.jobRun.Name)
		runOrder = append(runOrder, j.jobRun.Name)
	}

	t.Then("the order of job runs should be sorted in chronological order")
	t.Assert.ElementsMatch([]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}, runOrder)
}

func TestRunQueueUnique(test *testing.T) {

	t := testc.New(test)

	t.Given("a job run queue")
	q := NewRunnableQueue()
	now := time.Now()

	t.Given("the queue has unique job runs")
	for i := 0; i < 10; i++ {
		chr := &Runnable{
			jobRun: &Run{
				Name:       fmt.Sprintf("%d", i),
				NameActive: sql.NullString{Valid: true, String: fmt.Sprintf("%d", i)},
				RunAt:      now.Add(time.Second * time.Duration(i)),
			},
		}
		q.Push(chr)
	}

	t.When("we add the same unique job runs to the queue")
	for i := 0; i < 10; i++ {
		chr := &Runnable{
			jobRun: &Run{
				Name:       fmt.Sprintf("%d", i),
				NameActive: sql.NullString{Valid: true, String: fmt.Sprintf("%d", i)},
				RunAt:      now.Add(time.Second * time.Duration(i)),
			},
		}
		q.Push(chr)
	}

	t.Then("the queue will automatically deduplicate the job runs")
	items := []string{}
	for q.Len() > 0 {
		j := q.Pop()
		items = append(items, j.jobRun.Name)
	}

	t.Assert.ElementsMatch([]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}, items)
}
