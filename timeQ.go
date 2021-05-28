package jobspec

import (
	"container/heap"
	"sync"
	"time"
)

// TimeItem .
type TimeItem interface {
	TimeQTime() time.Time
	TimeQID() int64
}

type timeQInternal []TimeItem

func (pq *timeQInternal) Len() int { return len(*pq) }

func (pq *timeQInternal) Less(i, j int) bool {
	return (*pq)[i].TimeQTime().Before((*pq)[j].TimeQTime()) && (*pq)[i].TimeQID() < (*pq)[i].TimeQID()
}

func (pq *timeQInternal) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *timeQInternal) Push(x interface{}) {
	item := x.(TimeItem)
	*pq = append(*pq, item)
}

func (pq *timeQInternal) Pop() interface{} {
	n := len(timeQInternal(*pq))
	x := timeQInternal(*pq)[n-1]
	*pq = timeQInternal(*pq)[0 : n-1]
	return x
}

func (pq *timeQInternal) Peek() TimeItem {
	if pq.Len() <= 0 {
		return nil
	}
	return (*pq)[0]
}

// TimeQ is a priority queue of timed items
type TimeQ struct {
	mx    sync.Mutex
	queue *timeQInternal
	dup   map[int64]struct{}

	started     bool
	stream      chan TimeItem
	streamStop  chan struct{}
	streamReset chan struct{}
}

// Push .
func (q *TimeQ) Push(r TimeItem) bool {
	q.mx.Lock()
	defer q.mx.Unlock()

	if _, ok := q.dup[r.TimeQID()]; ok {
		return false // this de-duplicates
	}
	reset := false
	if q.queue.Len() == 0 {
		reset = true
	} else if q.queue.Peek().TimeQTime().After(r.TimeQTime()) {
		reset = true
	}

	q.dup[r.TimeQID()] = struct{}{}
	heap.Push(q.queue, r)
	if reset {
		q.notifyReset()
	}

	return true
}

func (q *TimeQ) notifyReset() {
	go func() { q.streamReset <- struct{}{} }()
}

// Len .
func (q *TimeQ) Len() int {
	q.mx.Lock()
	defer q.mx.Unlock()

	return q.queue.Len()
}

// Peek .
func (q *TimeQ) Peek() TimeItem {
	q.mx.Lock()
	defer q.mx.Unlock()

	return q.queue.Peek()
}

// Stream .
func (q *TimeQ) Stream() chan TimeItem {
	return q.stream
}

func (q *TimeQ) startStream() {
	for {
		if q.Len() <= 0 {
			select {
			case <-q.streamReset:
				break
			case <-q.streamStop:
				return
			}
		}

		waitTime := q.Peek().TimeQTime().Sub(time.Now())
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
func (q *TimeQ) StartStream() {
	if q.started {
		return
	}
	q.started = true
	q.streamStop = make(chan struct{})
	go q.startStream()
}

// StopStream .
func (q *TimeQ) StopStream() {
	q.mx.Lock()
	defer q.mx.Unlock()
	close(q.streamStop)
	q.started = false
}

// NewTimeQ .
func NewTimeQ() *TimeQ {
	rtn := &TimeQ{
		queue:       &timeQInternal{},
		dup:         map[int64]struct{}{},
		stream:      make(chan TimeItem),
		streamReset: make(chan struct{}),
	}
	rtn.StartStream()
	return rtn
}
