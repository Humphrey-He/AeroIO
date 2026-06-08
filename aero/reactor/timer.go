package reactor

import (
	"sync"
	"sync/atomic"
	"time"
)

// TimerWheel is a hierarchical timing wheel for managing connection timeouts.
type TimerWheel struct {
	tickMs    int64
	slots     int
	buckets   [][]*TimerEntry
	current   int64
	mu        sync.Mutex
}

type TimerEntry struct {
	FD       int
	Deadline int64 // unix nano
	Callback func(fd int)
	next     *TimerEntry
}

func NewTimerWheel(tickMs int64, slots int) *TimerWheel {
	tw := &TimerWheel{
		tickMs:  tickMs,
		slots:   slots,
		buckets: make([][]*TimerEntry, slots),
		current: 0,
	}
	go tw.run()
	return tw
}

func (tw *TimerWheel) run() {
	ticker := time.NewTicker(time.Duration(tw.tickMs) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		tw.tick()
	}
}

func (tw *TimerWheel) tick() {
	tw.mu.Lock()
	idx := atomic.AddInt64(&tw.current, 1) % int64(tw.slots)
	bucket := tw.buckets[idx]
	tw.buckets[idx] = nil
	tw.mu.Unlock()

	now := time.Now().UnixNano()
	for _, entry := range bucket {
		if entry.Deadline <= now {
			entry.Callback(entry.FD)
		} else {
			// Re-add to appropriate slot
			tw.AddEntry(entry)
		}
	}
}

func (tw *TimerWheel) Add(deadline time.Time, callback func(fd int), fd int) {
	tw.AddEntry(&TimerEntry{
		FD:       fd,
		Deadline: deadline.UnixNano(),
		Callback: callback,
	})
}

func (tw *TimerWheel) AddEntry(entry *TimerEntry) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	delay := entry.Deadline - time.Now().UnixNano()
	if delay <= 0 {
		go entry.Callback(entry.FD)
		return
	}

	ticks := delay / (tw.tickMs * int64(time.Millisecond))
	idx := (atomic.LoadInt64(&tw.current) + ticks) % int64(tw.slots)
	tw.buckets[idx] = append(tw.buckets[idx], entry)
}
