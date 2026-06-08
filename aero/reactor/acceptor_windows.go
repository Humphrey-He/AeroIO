//go:build windows
// +build windows

package reactor

import "fmt"

// SetNonblock is a no-op on Windows since Go's netpoller handles non-blocking I/O.
func SetNonblock(fd int) error {
	return fmt.Errorf("SetNonblock not supported on Windows; use Go's built-in netpoller")
}
