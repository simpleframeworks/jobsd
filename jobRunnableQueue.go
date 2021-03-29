package jobsd

import (
	"container/heap"
	"sync"
)

type jobRunnableQueue []JobRunnable

func (pq *jobRunnableQueue) Len() int { return len(*pq) }

func (pq *jobRunnableQueue) Less(i, j int) bool {
	return (*pq)[i].RunAt.Before((*pq)[j].RunAt)
}

func (pq *jobRunnableQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *jobRunnableQueue) Push(x interface{}) {
	item := x.(JobRunnable)
	*pq = append(*pq, item)
}

func (pq *jobRunnableQueue) Pop() interface{} {
	n := len(jobRunnableQueue(*pq))
	x := jobRunnableQueue(*pq)[n-1]
	*pq = jobRunnableQueue(*pq)[0 : n-1]
	return x
}

func (pq *jobRunnableQueue) Peek() JobRunnable {
	if pq.Len() <= 0 {
		return JobRunnable{}
	}
	return (*pq)[0]
}

// JobRunnableQueue is a priority queue of jobs to run
type JobRunnableQueue struct {
	mx    sync.Mutex
	queue *jobRunnableQueue
	dup   map[string]struct{}
}

// Push .
func (q *JobRunnableQueue) Push(j JobRunnable) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if !j.jobRun.NameActive.Valid {
		return
	}
	if _, ok := q.dup[j.jobRun.NameActive.String]; ok {
		return // this de-duplicates
	}
	q.dup[j.jobRun.NameActive.String] = struct{}{}
	heap.Push(q.queue, j)
}

// Pop .
func (q *JobRunnableQueue) Pop() JobRunnable {
	q.mx.Lock()
	defer q.mx.Unlock()
	if q.queue.Len() <= 0 {
		return JobRunnable{}
	}
	rtn := heap.Pop(q.queue).(JobRunnable)
	delete(q.dup, rtn.jobRun.NameActive.String)
	return rtn
}

// Peek .
func (q *JobRunnableQueue) Peek() JobRunnable {
	q.mx.Lock()
	defer q.mx.Unlock()
	if q.queue.Len() <= 0 {
		return JobRunnable{}
	}
	return q.queue.Peek()
}

// Len .
func (q *JobRunnableQueue) Len() int {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.queue.Len()
}

// NewJobRunnableQueue .
func NewJobRunnableQueue() *JobRunnableQueue {
	return &JobRunnableQueue{
		queue: &jobRunnableQueue{},
		dup:   map[string]struct{}{},
	}
}
