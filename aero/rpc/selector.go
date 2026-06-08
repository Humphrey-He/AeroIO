package rpc

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

// Selector implements load balancing across service endpoints.
type Selector struct {
	mu      sync.RWMutex
	addrs   []string
	rrIdx   atomic.Uint64
}

type SelectMode int

const (
	SelectRandom     SelectMode = iota
	SelectRoundRobin
	SelectFirst
)

func NewSelector(addrs []string) *Selector {
	s := &Selector{}
	s.addrs = append(s.addrs, addrs...)
	return s
}

func (s *Selector) AddAddr(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addrs = append(s.addrs, addr)
}

func (s *Selector) RemoveAddr(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, a := range s.addrs {
		if a == addr {
			s.addrs = append(s.addrs[:i], s.addrs[i+1:]...)
			return
		}
	}
}

func (s *Selector) Pick(mode SelectMode) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.addrs) == 0 {
		return ""
	}

	switch mode {
	case SelectRandom:
		return s.addrs[rand.Intn(len(s.addrs))]
	case SelectRoundRobin:
		idx := s.rrIdx.Add(1) % uint64(len(s.addrs))
		return s.addrs[idx]
	case SelectFirst:
		return s.addrs[0]
	default:
		return s.addrs[0]
	}
}

func (s *Selector) All() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.addrs))
	copy(out, s.addrs)
	return out
}
