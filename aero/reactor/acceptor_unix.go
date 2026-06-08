//go:build !windows
// +build !windows

package reactor

import "syscall"

// SetNonblock sets the O_NONBLOCK flag on a file descriptor.
func SetNonblock(fd int) error {
	flags, err := syscall.Fcntl(uintptr(fd), syscall.F_GETFL, 0)
	if err != nil {
		return err
	}
	return syscall.Fcntl(uintptr(fd), syscall.F_SETFL, flags|syscall.O_NONBLOCK)
}
