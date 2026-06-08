//go:build !linux
// +build !linux

package reactor

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Fallback poller using channels — works on any platform via Go's built-in netpoller.
type Poller struct {
	mu       sync.Mutex
	fds      map[int]*fdState
	eventCh  chan Event
	closeCh  chan struct{}
}

type fdState struct {
	conn   net.Conn
	events EventType
}

func NewPoller() (*Poller, error) {
	return &Poller{
		fds:     make(map[int]*fdState),
		eventCh: make(chan Event, 1024),
		closeCh: make(chan struct{}),
	}, nil
}

func (p *Poller) Add(fd int) error {
	// In the fallback, fds are managed externally via net.Conn.
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fds[fd] = &fdState{}
	return nil
}

func (p *Poller) AddConn(fd int, conn net.Conn) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fds[fd] = &fdState{conn: conn}
	return nil
}

func (p *Poller) ModRead(fd int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.fds[fd]; ok {
		s.events = EventRead
	}
	return nil
}

func (p *Poller) ModWrite(fd int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.fds[fd]; ok {
		s.events = EventWrite
	}
	return nil
}

func (p *Poller) Remove(fd int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.fds, fd)
	return nil
}

func (p *Poller) Close() error {
	close(p.closeCh)
	return nil
}

func (p *Poller) NotifyRead(fd int) {
	select {
	case p.eventCh <- Event{FD: fd, Type: EventRead}:
	default:
	}
}

func (p *Poller) NotifyWrite(fd int) {
	select {
	case p.eventCh <- Event{FD: fd, Type: EventWrite}:
	default:
	}
}

// Wait blocks until events are ready.
func (p *Poller) Wait(timeoutMs int) ([]Event, error) {
	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()

	select {
	case ev := <-p.eventCh:
		return []Event{ev}, nil
	case <-timer.C:
		return nil, fmt.Errorf("timeout")
	case <-p.closeCh:
		return nil, fmt.Errorf("poller closed")
	}
}
