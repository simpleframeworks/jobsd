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
	return (*pq)[i].QueueTime().Before((*pq)[j].QueueTime()) && (*pq)[i].QueueID() < (*pq)[i].QueueID()
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
	mx    sync.Mutex
	queue *timeQueueInternal
	dup   map[int64]struct{}

	started     bool
	stream      chan TimeItem
	streamStop  chan struct{}
	streamReset chan struct{}
}

// Push .
func (q *TimeQueue) Push(r TimeItem) bool {
	q.mx.Lock()
	defer q.mx.Unlock()

	if _, ok := q.dup[r.QueueID()]; ok {
		return false // this de-duplicates
	}
	reset := false
	if q.queue.Len() == 0 {
		reset = true
	} else if q.queue.Peek().QueueTime().After(r.QueueTime()) {
		reset = true
	}

	q.dup[r.QueueID()] = struct{}{}
	heap.Push(q.queue, r)
	if reset {
		q.notifyReset()
	}

	return true
}

func (q *TimeQueue) notifyReset() {
	go func() { q.streamReset <- struct{}{} }()
}

// Len .
func (q *TimeQueue) Len() int {
	q.mx.Lock()
	defer q.mx.Unlock()

	return q.queue.Len()
}

// Peek .
func (q *TimeQueue) Peek() TimeItem {
	q.mx.Lock()
	defer q.mx.Unlock()

	return q.queue.Peek()
}

// Stream .
func (q *TimeQueue) Stream() chan TimeItem {
	return q.stream
}

func (q *TimeQueue) startStream() {
	for {
		if q.Len() <= 0 {
			select {
			case <-q.streamReset:
				break
			case <-q.streamStop:
				return
			}
		}

		waitTime := q.Peek().QueueTime().Sub(time.Now())
		select {
		case <-time.After(waitTime):
			q.mx.Lock()
			toSend := q.queue.Pop().(TimeItem)
			q.mx.Unlock()
			q.stream <- toSend
		case <-q.streamReset:
			for len(q.streamReset) > 0 {
				<-q.streamReset
			}
			break
		case <-q.streamStop:
			return
		}
		// Clear reset messages
		for len(q.streamReset) > 0 {
			<-q.streamReset
		}
	}
}

// StartStream .
func (q *TimeQueue) StartStream() {
	if q.started {
		return
	}
	q.started = true
	q.streamStop = make(chan struct{})
	go q.startStream()
}

// StopStream .
func (q *TimeQueue) StopStream() {
	q.mx.Lock()
	defer q.mx.Unlock()
	close(q.streamStop)
	q.started = false
}

// NewTimeQueue .
func NewTimeQueue() *TimeQueue {
	rtn := &TimeQueue{
		queue:       &timeQueueInternal{},
		dup:         map[int64]struct{}{},
		stream:      make(chan TimeItem),
		streamReset: make(chan struct{}),
	}
	rtn.StartStream()
	return rtn
}
