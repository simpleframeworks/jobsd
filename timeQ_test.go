package jobspec

import (
	"testing"
	"time"

	"github.com/simpleframeworks/testc"
)

type testTimeQ struct {
	id   int64
	time time.Time
}

func (t testTimeQ) TimeQTime() time.Time {
	return t.time
}
func (t testTimeQ) TimeQID() int64 {
	return t.id
}

func TestTimeQ(test *testing.T) {
	t := testc.New(test)

	t.Given("a TimeQueue")
	q := NewTimeQ()

	t.Given("the queue has TimeItems")
	itemNum := 10
	for i := 0; i < itemNum; i++ {
		item := testTimeQ{
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
		runOrder = append(runOrder, topItem.TimeQID())
	}

	t.Then("the order of TimeItems should be sorted in chronological order")
	t.Assert.ElementsMatch(expectedOrder, runOrder)

	q.StopStream()
}

func TestTimeQConcurrent(test *testing.T) {
	t := testc.New(test)

	t.Given("a TimeQueue")
	q := NewTimeQ()

	t.Given("we add TimeItems concurrently")
	itemNum := 100
	go func() {
		for i := 0; i < itemNum; i++ {
			q.Push(testTimeQ{
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
		runOrder = append(runOrder, topItem.TimeQID())
	}

	t.Then("the order of TimeItems should be sorted in chronological order")
	t.Assert.ElementsMatch(expectedOrder, runOrder)

	q.StopStream()
}

func TestTimeQPush(test *testing.T) {
	t := testc.New(test)

	t.Given("a TimeQueue")
	q := NewTimeQ()

	t.Given("the queue has TimeItems")
	for i := 0; i < 10; i++ {
		item := testTimeQ{
			id:   int64(i),
			time: time.Now(),
		}

		q.Push(item)
	}

	t.When("we stop the stream")
	q.StopStream()

	t.Then("it should not be blocking")
}

func TestTimeQOrdering(test *testing.T) {

	t := testc.New(test)

	t.Given("a TimeQueue")
	q := NewTimeQ()

	now := time.Now()

	t.Given("the queue has TimeItems")
	for i := 0; i < 10; i++ {
		q.Push(testTimeQ{
			id:   int64(i),
			time: now.Add(time.Second * 2).Add(time.Millisecond * time.Duration(i)),
		})
	}

	t.Given("we add an item that should be at the front of the queue")
	q.Push(testTimeQ{
		id:   int64(-100),
		time: now,
	})

	<-time.After(time.Second)

	t.When("we stream the TimeItems off the queue")
	runOrder := []int64{}
	for i := 0; i < 11; i++ {
		topItem := <-q.Stream()
		runOrder = append(runOrder, topItem.TimeQID())
	}

	t.Then("the order of TimeItems should be sorted in chronological order")
	t.Assert.ElementsMatch([]int64{-100, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, runOrder)

	q.StopStream()
}

func TestTimeQUnique(test *testing.T) {

	t := testc.New(test)

	t.Given("a TimeQueue")
	q := NewTimeQ()

	tempItems := []TimeItem{}

	t.Given("the queue has TimeItems")
	for i := 9; i >= 0; i-- {
		item := testTimeQ{
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
		runOrder = append(runOrder, topItem.TimeQID())
	}

	t.Assert.ElementsMatch([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, runOrder)

	q.StopStream()
}
