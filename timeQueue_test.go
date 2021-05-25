package jobspec

import (
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
)

type testTimeQueue struct {
	id   int64
	time time.Time
}

func (t testTimeQueue) QueueTime() time.Time {
	return t.time
}
func (t testTimeQueue) QueueID() int64 {
	return t.id
}

func TestTimeQueue(test *testing.T) {
	t := testc.New(test)

	t.Given("a job run queue")
	q := NewTimeQueue()

	t.Given("the queue has job runs")
	for i := 9; i >= 0; i-- {
		item := testTimeQueue{
			id:   int64(i),
			time: time.Now(),
		}

		q.Push(item)
	}

	t.When("we pop the job runs off the queue")
	runOrder := []int64{}
	for i := 0; i < 10; i++ {
		topItem := q.Peek()
		j := q.Pop()
		t.Assert.Equal(topItem.QueueID(), j.QueueID())
		runOrder = append(runOrder, j.QueueID())
	}

	t.Then("the order of job runs should be sorted in chronological order")
	t.Assert.ElementsMatch([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, runOrder)
}

func TestTimeQueueUnique(test *testing.T) {

	t := testc.New(test)

	t.Given("a job run queue")
	q := NewTimeQueue()
	now := time.Now()

	t.Given("the queue has unique job runs")
	for i := 0; i < 10; i++ {
		item := testTimeQueue{
			id:   int64(i),
			time: now,
		}

		q.Push(item)
	}

	t.When("we add the same unique job runs to the queue")
	for i := 0; i < 10; i++ {
		item := testTimeQueue{
			id:   int64(i),
			time: time.Now(),
		}

		q.Push(item)
	}

	t.Then("the queue will automatically deduplicate the job runs and ignore new runs")
	items := []int64{}
	for q.len() > 0 {
		j := q.Pop()
		items = append(items, j.QueueID())
	}

	t.Assert.ElementsMatch([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, items)
}

func TestTimeQueuePushNotification(test *testing.T) {
	t := testc.New(test)

	t.Given("a job run queue")
	q := NewTimeQueue()

	for i := 0; i < 10; i++ {

		t.Given("the queue push notification channel")
		pushed := q.Pushed()

		t.When("we push an item on the queue")
		item := testTimeQueue{
			id:   int64(i),
			time: time.Now(),
		}

		q.Push(item)

		t.Then("we should have received a notification")
		hasPushed := false
		select {
		case <-pushed:
			hasPushed = true
		case <-time.After(100 * time.Millisecond):
			hasPushed = false
		}
		t.Assert.True(hasPushed)
	}

}
