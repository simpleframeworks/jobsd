package jobspec

import (
	"container/heap"
	"sync"
	"time"
)

// TimeItem .
type TimeItem interface {
	QueueTime() time.Time
	QueueID() int64
}

type timeQueueInternal []TimeItem

func (pq *timeQueueInternal) Len() int { return len(*pq) }

func (pq *timeQueueInternal) Less(i, j int) bool {
	return (*pq)[i].QueueTime().Before((*pq)[j].QueueTime())
}

func (pq *timeQueueInternal) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *timeQueueInternal) Push(x interface{}) {
	item := x.(TimeItem)
	*pq = append(*pq, item)
}

func (pq *timeQueueInternal) Pop() interface{} {
	n := len(timeQueueInternal(*pq))
	x := timeQueueInternal(*pq)[n-1]
	*pq = timeQueueInternal(*pq)[0 : n-1]
	return x
}

func (pq *timeQueueInternal) Peek() TimeItem {
	if pq.Len() <= 0 {
		return nil
	}
	return (*pq)[0]
}

// TimeQueue is a priority queue of timed items
type TimeQueue struct {
	mx       sync.Mutex
	queue    *timeQueueInternal
	dup      map[int64]struct{}
	pushChan chan struct{}
}

// Push .
func (q *TimeQueue) Push(r TimeItem) bool {
	q.mx.Lock()
	defer q.mx.Unlock()

	if _, ok := q.dup[r.QueueID()]; ok {
		return false // this de-duplicates
	}
	q.dup[r.QueueID()] = struct{}{}
	heap.Push(q.queue, r)
	// Notify pushed chan of a push
	if q.pushChan != nil {
		close(q.pushChan)
		q.pushChan = nil
	}
	return true
}

// Pushed notifies when an item is pushed. Channel expires after one push.
func (q *TimeQueue) Pushed() chan struct{} {
	q.mx.Lock()
	defer q.mx.Unlock()

	if q.pushChan == nil {
		q.pushChan = make(chan struct{})
	}
	return q.pushChan
}

// Pop .
func (q *TimeQueue) Pop() TimeItem {
	q.mx.Lock()
	defer q.mx.Unlock()

	if q.queue.Len() <= 0 {
		return nil
	}
	rtn := heap.Pop(q.queue).(TimeItem)
	delete(q.dup, rtn.QueueID())
	return rtn
}

// Peek .
func (q *TimeQueue) Peek() TimeItem {
	q.mx.Lock()
	defer q.mx.Unlock()

	if q.queue.Len() <= 0 {
		return nil
	}
	return q.queue.Peek()
}

func (q *TimeQueue) len() int {
	q.mx.Lock()
	defer q.mx.Unlock()

	return q.queue.Len()
}

// NewTimeQueue .
func NewTimeQueue() *TimeQueue {
	return &TimeQueue{
		queue: &timeQueueInternal{},
		dup:   map[int64]struct{}{},
	}
}
