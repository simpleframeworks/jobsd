package jobss

import (
	"container/heap"
	"sync"
)

type runnableQueue []*Runnable

func (pq *runnableQueue) Len() int { return len(*pq) }

func (pq *runnableQueue) Less(i, j int) bool {
	return (*pq)[i].runAt().Before((*pq)[j].runAt())
}

func (pq *runnableQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *runnableQueue) Push(x interface{}) {
	item := x.(*Runnable)
	*pq = append(*pq, item)
}

func (pq *runnableQueue) Pop() interface{} {
	n := len(runnableQueue(*pq))
	x := runnableQueue(*pq)[n-1]
	*pq = runnableQueue(*pq)[0 : n-1]
	return x
}

func (pq *runnableQueue) Peek() *Runnable {
	if pq.Len() <= 0 {
		return &Runnable{}
	}
	return (*pq)[0]
}

// RunnableQueue is a priority queue of jobs to run
type RunnableQueue struct {
	mx    sync.Mutex
	queue *runnableQueue
	dup   map[string]struct{}
}

// Push .
func (q *RunnableQueue) Push(j *Runnable) bool {
	q.mx.Lock()
	defer q.mx.Unlock()
	if !j.jobRun.NameActive.Valid {
		return false
	}
	if _, ok := q.dup[j.jobRun.NameActive.String]; ok {
		return false // this de-duplicates
	}
	q.dup[j.jobRun.NameActive.String] = struct{}{}
	heap.Push(q.queue, j)
	return true
}

// Pop .
func (q *RunnableQueue) Pop() *Runnable {
	q.mx.Lock()
	defer q.mx.Unlock()
	if q.queue.Len() <= 0 {
		return &Runnable{}
	}
	rtn := heap.Pop(q.queue).(*Runnable)
	delete(q.dup, rtn.jobRun.NameActive.String)
	return rtn
}

// Peek .
func (q *RunnableQueue) Peek() *Runnable {
	q.mx.Lock()
	defer q.mx.Unlock()
	if q.queue.Len() <= 0 {
		return &Runnable{}
	}
	return q.queue.Peek()
}

// Len .
func (q *RunnableQueue) Len() int {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.queue.Len()
}

// NewRunnableQueue .
func NewRunnableQueue() *RunnableQueue {
	return &RunnableQueue{
		queue: &runnableQueue{},
		dup:   map[string]struct{}{},
	}
}
