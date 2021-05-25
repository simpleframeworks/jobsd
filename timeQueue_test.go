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

	t.Given("a TimeQueue")
	q := NewTimeQueue()

	t.Given("the queue has TimeItems")
	itemNum := 10
	for i := 0; i < itemNum; i++ {
		item := testTimeQueue{
			id:   int64(i),
			time: time.Now(),
		}

		q.Push(item)
	}

	t.When("we stream the TimeItems off the queue")
	expectedOrder := []int64{}
	runOrder := []int64{}
	for i := 0; i < itemNum; i++ {
		topItem := <-q.Stream()
		expectedOrder = append(expectedOrder, int64(i))
		runOrder = append(runOrder, topItem.QueueID())
	}

	t.Then("the order of TimeItems should be sorted in chronological order")
	t.Assert.ElementsMatch(expectedOrder, runOrder)

	q.StopStream()
}

func TestTimeQueueConcurrent(test *testing.T) {
	t := testc.New(test)

	t.Given("a TimeQueue")
	q := NewTimeQueue()

	t.Given("we add TimeItems concurrently")
	itemNum := 100
	go func() {
		for i := 0; i < itemNum; i++ {
			q.Push(testTimeQueue{
				id:   int64(i),
				time: time.Now(),
			})
		}
	}()

	<-time.After(time.Second)

	t.When("we stream the TimeItems off the queue")
	expectedOrder := []int64{}
	runOrder := []int64{}
	for i := 0; i < itemNum; i++ {
		topItem := <-q.Stream()
		expectedOrder = append(expectedOrder, int64(i))
		runOrder = append(runOrder, topItem.QueueID())
	}

	t.Then("the order of TimeItems should be sorted in chronological order")
	t.Assert.ElementsMatch(expectedOrder, runOrder)

	q.StopStream()
}

func TestTimeQueuePush(test *testing.T) {
	t := testc.New(test)

	t.Given("a TimeQueue")
	q := NewTimeQueue()

	t.Given("the queue has TimeItems")
	for i := 0; i < 10; i++ {
		item := testTimeQueue{
			id:   int64(i),
			time: time.Now(),
		}

		q.Push(item)
	}

	t.When("we stop the stream")
	q.StopStream()

	t.Then("it should not be blocking")
}

func TestTimeQueueOrdering(test *testing.T) {

	t := testc.New(test)

	t.Given("a TimeQueue")
	q := NewTimeQueue()

	now := time.Now()

	t.Given("the queue has TimeItems")
	for i := 0; i < 10; i++ {
		q.Push(testTimeQueue{
			id:   int64(i),
			time: now.Add(time.Second * 2).Add(time.Millisecond * time.Duration(i)),
		})
	}

	t.Given("we add an item that should be at the front of the queue")
	q.Push(testTimeQueue{
		id:   int64(-100),
		time: now,
	})

	<-time.After(time.Second)

	t.When("we stream the TimeItems off the queue")
	runOrder := []int64{}
	for i := 0; i < 11; i++ {
		topItem := <-q.Stream()
		runOrder = append(runOrder, topItem.QueueID())
	}

	t.Then("the order of TimeItems should be sorted in chronological order")
	t.Assert.ElementsMatch([]int64{-100, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, runOrder)

	q.StopStream()
}

func TestTimeQueueUnique(test *testing.T) {

	t := testc.New(test)

	t.Given("a TimeQueue")
	q := NewTimeQueue()

	tempItems := []TimeItem{}

	t.Given("the queue has TimeItems")
	for i := 9; i >= 0; i-- {
		item := testTimeQueue{
			id:   int64(i),
			time: time.Now(),
		}
		tempItems = append(tempItems, item)
		q.Push(item)
	}

	t.When("we add the same unique TimeItems")
	for _, item := range tempItems {

		t.Assert.False(q.Push(item))
	}

	t.Then("the queue will automatically deduplicate the TimeItems")
	runOrder := []int64{}
	for i := 0; i < 10; i++ {
		topItem := <-q.Stream()
		runOrder = append(runOrder, topItem.QueueID())
	}

	t.Assert.ElementsMatch([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, runOrder)

	q.StopStream()
}
