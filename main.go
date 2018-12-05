package loadbalancer

import (
	"container/heap"
	"math/rand"
	"time"
)

type Pool []*Worker

type Balancer struct {
	pool Pool
	done chan *Worker
}

type Request struct {
	fn func() int // The operation perform
	c  chan int   // The channer to return the result
}

type Worker struct {
	requests chan Request // work to do (buffered channel)
	pending  int          // count of pending tasks
	index    int          // index in the heap
}

func (p Pool) Less(i, j int) bool {
	return p[i].pending < p[j].pending
}

// Send Request to worker
func (b *Balancer) dispatch(req Request) {
	// Grab the least loaded worker
	w := heap.Pop(&b.pool).(*Worker)
	// Send it the task
	w.requests <- req
	// One more in its work queue
	w.pending++
	// Put it into its place on the heap
	heap.Push(&b.pool, w)
}

// Job is complete; update heap
func (b *Balancer) completed(w *Worker) {
	// One fewer in the queue
	w.pending--
	// Remove it from the heap
	heap.Remove(&b.pool, w.index)
	// Put it into its place on the heap
	heap.Push(&b.pool, w)
}

func (b *Balancer) balance(work chan Request) {
	for {
		select {
		case req := <-work: // received a Request ...
			b.dispatch(req)
		case w := <-b.done: // a worker has finished ...
			b.completed(w) // ... so update its info
		}
	}
}

func (p Pool) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].index = i
	p[j].index = j
}

func (p *Pool) Push(x interface{}) {
	n := len(*p)
	item := x.(*Worker)
	item.index = n
	*p = append(*p, item)
}

func (p *Pool) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	item.index = -1
	*p = old[0 : n-1]
	return item
}

func (p Pool) Len() int { return len(p) }

// Balancer sends request to most lightly loaded worker
func (w *Worker) work(done chan *Worker) {
	for {
		req := <-w.requests // get Request from balancer
		req.c <- req.fn()   // call fn and send result
		done <- w           // we've finished this request
	}
}

// Illustrative simulation of a requester, one of many possible ones
func requester(work chan<- Request) {
	c := make(chan int)
	for {
		// Kill some time, fake load
		var nWorker int64 = 4
		rand := rand.Int63n(nWorker)
		time.Sleep(time.Duration(time.Duration(rand) * time.Second))
		workFn := func() int {
			return 23
		}
		work <- Request{workFn , c} // send request
		result := <-c              // wait for response
		furtherProcess(result)
	}
}

func furtherProcess(i int) {

}
