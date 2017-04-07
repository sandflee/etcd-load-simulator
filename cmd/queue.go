package cmd

import (
	"sync"
	"time"
)

func newQueue() queue {
	return queue{
		values: []interface{}{},
	}
}

type queue struct {
	values []interface{}
	sync.Mutex
}

func (q *queue) push(e interface{}) {
	q.Lock()
	defer q.Unlock()
	q.values = append(q.values, e)
}

func (q *queue) len() int {
	q.Lock()
	defer q.Unlock()
	return len(q.values)
}

func (q *queue) pop() interface{} {
	for {
		if len(q.values) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		q.Lock()
		defer q.Unlock()

		if len(q.values) == 0 {
			continue
		}

		v := q.values[0]
		q.values = q.values[1:]
		return v
	}
}

