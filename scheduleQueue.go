package jobspec

import (
	"container/heap"
	"sync"
)

type scheduleQueueInternal []*run

func (pq *scheduleQueueInternal) Len() int { return len(*pq) }

func (pq *scheduleQueueInternal) Less(i, j int) bool {
	return (*pq)[i].model.RunAt.Before((*pq)[j].model.RunAt)
}

func (pq *scheduleQueueInternal) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *scheduleQueueInternal) Push(x interface{}) {
	item := x.(*run)
	*pq = append(*pq, item)
}

func (pq *scheduleQueueInternal) Pop() interface{} {
	n := len(scheduleQueueInternal(*pq))
	x := scheduleQueueInternal(*pq)[n-1]
	*pq = scheduleQueueInternal(*pq)[0 : n-1]
	return x
}

func (pq *scheduleQueueInternal) Peek() *run {
	if pq.Len() <= 0 {
		return &run{}
	}
	return (*pq)[0]
}

// scheduleQueue is a priority queue of jobs to run
type scheduleQueue struct {
	mx       sync.Mutex
	queue    *scheduleQueueInternal
	dup      map[int64]struct{}
	pushChan chan struct{}
}

func (q *scheduleQueue) push(r *run) bool {
	q.mx.Lock()
	defer q.mx.Unlock()

	if _, ok := q.dup[r.model.ID]; ok {
		return false // this de-duplicates
	}
	q.dup[r.model.ID] = struct{}{}
	heap.Push(q.queue, r)
	// Notify pushed chan of a push
	if q.pushChan != nil {
		close(q.pushChan)
		q.pushChan = nil
	}
	return true
}

func (q *scheduleQueue) pushed() chan struct{} {
	q.mx.Lock()
	defer q.mx.Unlock()

	if q.pushChan == nil {
		q.pushChan = make(chan struct{})
	}
	return q.pushChan
}

func (q *scheduleQueue) pop() *run {
	q.mx.Lock()
	defer q.mx.Unlock()

	if q.queue.Len() <= 0 {
		return &run{}
	}
	rtn := heap.Pop(q.queue).(*run)
	delete(q.dup, rtn.model.ID)
	return rtn
}

func (q *scheduleQueue) peek() *run {
	q.mx.Lock()
	defer q.mx.Unlock()

	if q.queue.Len() <= 0 {
		return &run{}
	}
	return q.queue.Peek()
}

func (q *scheduleQueue) len() int {
	q.mx.Lock()
	defer q.mx.Unlock()

	return q.queue.Len()
}

func newRunQueue() *scheduleQueue {
	return &scheduleQueue{
		queue: &scheduleQueueInternal{},
		dup:   map[int64]struct{}{},
	}
}
