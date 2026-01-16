package wait

import (
	"sync"
	"time"
)

type Wait struct {
	wg sync.WaitGroup
}

// Add adds delta, which may be negative, to the WaitGroup counter.
func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

// Done decrements the WaitGroup counter by one
func (w *Wait) Done() {
	w.wg.Done()
}

// Wait blocks until the WaitGroup counter is zero.
func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout blocks until the WaitGroup counter is zero or timeout
// return true if timeout
func (w *Wait) WaitWithTimeout(waitTime time.Duration) bool {
	c := make(chan struct{})
	//use close(c) instead of c <- struct{}{}, cz close(c) will trigger case <-c
	go func() {
		w.wg.Wait()
		close(c)
	}()

	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	select {
	case <-c:
		return false
	case <-timer.C:
		return true
	}
}
