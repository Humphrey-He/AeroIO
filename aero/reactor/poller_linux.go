//go:build linux
// +build linux

package reactor

import (
	"fmt"
	"syscall"
)

// Poller wraps epoll for I/O event notification.
type Poller struct {
	epfd   int
	events []syscall.EpollEvent
}

func NewPoller() (*Poller, error) {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}
	return &Poller{
		epfd:   epfd,
		events: make([]syscall.EpollEvent, 1024),
	}, nil
}

func (p *Poller) Add(fd int) error {
	epET := syscall.EPOLLET
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_ADD, fd,
		&syscall.EpollEvent{
			Events: uint32(syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP) | uint32(epET),
			Fd:     int32(fd),
		})
}

func (p *Poller) ModRead(fd int) error {
	epET := syscall.EPOLLET
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_MOD, fd,
		&syscall.EpollEvent{
			Events: uint32(syscall.EPOLLIN|syscall.EPOLLRDHUP) | uint32(epET),
			Fd:     int32(fd),
		})
}

func (p *Poller) ModWrite(fd int) error {
	epET := syscall.EPOLLET
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_MOD, fd,
		&syscall.EpollEvent{
			Events: uint32(syscall.EPOLLOUT|syscall.EPOLLRDHUP) | uint32(epET),
			Fd:     int32(fd),
		})
}

func (p *Poller) Remove(fd int) error {
	return syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_DEL, fd, nil)
}

func (p *Poller) Close() error {
	return syscall.Close(p.epfd)
}

// Wait blocks until events are ready. Returns up to maxEvents fd+event pairs.
func (p *Poller) Wait(timeoutMs int) ([]Event, error) {
	n, err := syscall.EpollWait(p.epfd, p.events, timeoutMs)
	if err != nil {
		if err == syscall.EINTR {
			return nil, nil
		}
		return nil, err
	}

	result := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		ev := p.events[i]
		var etype EventType
		if ev.Events&(syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLHUP) != 0 {
			etype |= EventRead
		}
		if ev.Events&syscall.EPOLLOUT != 0 {
			etype |= EventWrite
		}
		if ev.Events&(syscall.EPOLLERR|syscall.EPOLLHUP) != 0 {
			etype |= EventError
		}
		if etype != 0 {
			result = append(result, Event{FD: int(ev.Fd), Type: etype})
		}
	}
	return result, nil
}

