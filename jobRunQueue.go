package jobsd

import (
	"container/heap"
	"sync"
)

type jobRunQueue []JobRun

func (pq *jobRunQueue) Len() int { return len(*pq) }

func (pq *jobRunQueue) Less(i, j int) bool {
	return (*pq)[i].RunAt.Before((*pq)[j].RunAt)
}

func (pq *jobRunQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *jobRunQueue) Push(x interface{}) {
	item := x.(JobRun)
	*pq = append(*pq, item)
}

func (pq *jobRunQueue) Pop() interface{} {
	n := len(jobRunQueue(*pq))
	x := jobRunQueue(*pq)[n-1]
	*pq = jobRunQueue(*pq)[0 : n-1]
	return x
}

func (pq *jobRunQueue) Peek() JobRun {
	if pq.Len() <= 0 {
		return JobRun{}
	}
	return (*pq)[0]
}

// JobRunQueue is a priority queue of jobs to run
type JobRunQueue struct {
	mx    sync.Mutex
	queue *jobRunQueue
	dup   map[string]struct{}
}

// Push .
func (q *JobRunQueue) Push(j JobRun) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if !j.NameActive.Valid {
		return
	}
	if _, ok := q.dup[j.NameActive.String]; ok {
		return // this de-duplicates
	}
	q.dup[j.NameActive.String] = struct{}{}
	heap.Push(q.queue, j)
}

// Pop .
func (q *JobRunQueue) Pop() JobRun {
	q.mx.Lock()
	defer q.mx.Unlock()
	if q.queue.Len() <= 0 {
		return JobRun{}
	}
	rtn := heap.Pop(q.queue).(JobRun)
	delete(q.dup, rtn.NameActive.String)
	return rtn
}

// Peek .
func (q *JobRunQueue) Peek() JobRun {
	q.mx.Lock()
	defer q.mx.Unlock()
	if q.queue.Len() <= 0 {
		return JobRun{}
	}
	return q.queue.Peek()
}

// Len .
func (q *JobRunQueue) Len() int {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.queue.Len()
}

// NewJobRunQueue .
func NewJobRunQueue() *JobRunQueue {
	return &JobRunQueue{
		queue: &jobRunQueue{},
		dup:   map[string]struct{}{},
	}
}
