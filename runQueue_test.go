package jobspec

import (
	"testing"
	"time"

	"github.com/simpleframeworks/jobspec/models"
	"github.com/simpleframeworks/testc"
)

func TestRunQueue(test *testing.T) {
	t := testc.New(test)

	t.Given("a job run queue")
	q := newRunQueue()

	t.Given("the queue has job runs")
	for i := 9; i >= 0; i-- {
		chr := &run{
			run: &models.Run{
				ID:    int64(i),
				RunAt: time.Now(),
			},
		}
		q.push(chr)
	}

	t.When("we pop the job runs off the queue")
	runOrder := []int64{}
	for i := 0; i < 10; i++ {
		topItem := q.peek()
		j := q.pop()
		t.Assert.Equal(topItem.run.ID, j.run.ID)
		runOrder = append(runOrder, j.run.ID)
	}

	t.Then("the order of job runs should be sorted in chronological order")
	t.Assert.ElementsMatch([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, runOrder)
}

func TestRunQueueUnique(test *testing.T) {

	t := testc.New(test)

	t.Given("a job run queue")
	q := newRunQueue()
	now := time.Now()

	t.Given("the queue has unique job runs")
	for i := 0; i < 10; i++ {
		chr := &run{
			run: &models.Run{
				ID:    int64(i),
				RunAt: time.Now(),
			},
		}
		q.push(chr)
	}

	t.When("we add the same unique job runs to the queue")
	for i := 0; i < 10; i++ {
		chr := &run{
			run: &models.Run{
				ID:    int64(i),
				RunAt: now.Add(time.Second * time.Duration(i)),
			},
		}
		q.push(chr)
	}

	t.Then("the queue will automatically deduplicate the job runs and ignore new runs")
	items := []int64{}
	for q.len() > 0 {
		j := q.pop()
		items = append(items, j.run.ID)
	}

	t.Assert.ElementsMatch([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, items)
}
