package jobspec

import (
	"container/heap"
	"sync"
)

type runQueueInternal []*run

func (pq *runQueueInternal) Len() int { return len(*pq) }

func (pq *runQueueInternal) Less(i, j int) bool {
	return (*pq)[i].model.RunAt.Before((*pq)[j].model.RunAt)
}

func (pq *runQueueInternal) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *runQueueInternal) Push(x interface{}) {
	item := x.(*run)
	*pq = append(*pq, item)
}

func (pq *runQueueInternal) Pop() interface{} {
	n := len(runQueueInternal(*pq))
	x := runQueueInternal(*pq)[n-1]
	*pq = runQueueInternal(*pq)[0 : n-1]
	return x
}

func (pq *runQueueInternal) Peek() *run {
	if pq.Len() <= 0 {
		return &run{}
	}
	return (*pq)[0]
}

// runQueue is a priority queue of jobs to run
type runQueue struct {
	mx    sync.Mutex
	queue *runQueueInternal
	dup   map[int64]struct{}
}

func (q *runQueue) push(r *run) bool {
	q.mx.Lock()
	defer q.mx.Unlock()

	if _, ok := q.dup[r.model.ID]; ok {
		return false // this de-duplicates
	}
	q.dup[r.model.ID] = struct{}{}
	heap.Push(q.queue, r)
	return true
}

func (q *runQueue) pop() *run {
	q.mx.Lock()
	defer q.mx.Unlock()

	if q.queue.Len() <= 0 {
		return &run{}
	}
	rtn := heap.Pop(q.queue).(*run)
	delete(q.dup, rtn.model.ID)
	return rtn
}

func (q *runQueue) peek() *run {
	q.mx.Lock()
	defer q.mx.Unlock()

	if q.queue.Len() <= 0 {
		return &run{}
	}
	return q.queue.Peek()
}

func (q *runQueue) len() int {
	q.mx.Lock()
	defer q.mx.Unlock()

	return q.queue.Len()
}

func newRunQueue() *runQueue {
	return &runQueue{
		queue: &runQueueInternal{},
		dup:   map[int64]struct{}{},
	}
}
