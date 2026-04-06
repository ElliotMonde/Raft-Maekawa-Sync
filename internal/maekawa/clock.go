package maekawa

import "sync/atomic"

func (w *Worker) tick() int64 {
	return atomic.AddInt64(&w.clock, 1)
}

func (w *Worker) updateClock(received int64) {
	for {
		cur := atomic.LoadInt64(&w.clock)
		next := cur + 1
		if received+1 > next {
			next = received + 1
		}
		if atomic.CompareAndSwapInt64(&w.clock, cur, next) {
			return
		}
	}
}
