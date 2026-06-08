package reactor

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// EventLoop is the main reactor event loop.
// It demultiplexes I/O events from a Poller and dispatches to registered handlers.
type EventLoop struct {
	poller  *Poller
	handler map[int]Handler
	mu      sync.RWMutex

	timerWheel *TimerWheel
	bufferPool *BufferPool

	stopped atomic.Int32
	done    chan struct{}
}

func NewEventLoop() (*EventLoop, error) {
	poller, err := NewPoller()
	if err != nil {
		return nil, err
	}

	return &EventLoop{
		poller:     poller,
		handler:    make(map[int]Handler),
		timerWheel: NewTimerWheel(100, 600), // 100ms tick, 60 seconds
		bufferPool: NewBufferPool(),
		done:       make(chan struct{}),
	}, nil
}

func (el *EventLoop) Register(fd int, h Handler) error {
	el.mu.Lock()
	el.handler[fd] = h
	el.mu.Unlock()

	return el.poller.Add(fd)
}

func (el *EventLoop) Unregister(fd int) error {
	el.mu.Lock()
	delete(el.handler, fd)
	el.mu.Unlock()

	return el.poller.Remove(fd)
}

func (el *EventLoop) SetReadTimeout(fd int, timeout time.Duration) {
	if timeout > 0 {
		deadline := time.Now().Add(timeout)
		el.timerWheel.Add(deadline, func(fd int) {
			el.mu.RLock()
			h, ok := el.handler[fd]
			el.mu.RUnlock()
			if ok {
				h.OnClose(fd, fmt.Errorf("read timeout"))
			}
		}, fd)
	}
}

func (el *EventLoop) BufferPool() *BufferPool { return el.bufferPool }

// Run starts the event loop. Blocks until Stop() is called.
func (el *EventLoop) Run() error {
	el.stopped.Store(0)

	for el.stopped.Load() == 0 {
		events, err := el.poller.Wait(100) // 100ms timeout
		if err != nil {
			if el.stopped.Load() != 0 {
				return nil
			}
			log.Printf("poller.Wait error: %v", err)
			continue
		}

		for _, ev := range events {
			el.dispatchEvent(ev)
		}
	}

	return nil
}

func (el *EventLoop) dispatchEvent(ev Event) {
	el.mu.RLock()
	h, ok := el.handler[ev.FD]
	el.mu.RUnlock()

	if !ok {
		return
	}

	var err error
	switch {
	case ev.Type&EventError != 0:
		h.OnClose(ev.FD, fmt.Errorf("epoll error"))
		return
	case ev.Type&EventClose != 0:
		h.OnClose(ev.FD, nil)
		return
	case ev.Type&EventRead != 0:
		err = h.OnRead(ev.FD)
		if err != nil {
			h.OnClose(ev.FD, err)
			return
		}
	}

	if ev.Type&EventWrite != 0 && err == nil {
		err = h.OnWrite(ev.FD)
		if err != nil {
			h.OnClose(ev.FD, err)
		}
	}
}

// Stop gracefully stops the event loop.
func (el *EventLoop) Stop() {
	el.stopped.Store(1)
	el.poller.Close()
	close(el.done)
}

// Wait blocks until the event loop stops.
func (el *EventLoop) Wait() {
	<-el.done
}
