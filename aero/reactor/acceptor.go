package reactor

import (
	"net"
)

// Acceptor wraps a TCP listener fd for use with the Reactor.
type Acceptor struct {
	ln      *net.TCPListener
	fd      int
	handler func(fd int, conn net.Conn)
}

func NewAcceptor(addr string, handler func(fd int, conn net.Conn)) (*Acceptor, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	tcpLn := ln.(*net.TCPListener)
	file, err := tcpLn.File()
	if err != nil {
		ln.Close()
		return nil, err
	}
	fd := int(file.Fd())

	return &Acceptor{
		ln:      tcpLn,
		fd:      fd,
		handler: handler,
	}, nil
}

func (a *Acceptor) FD() int     { return a.fd }
func (a *Acceptor) Accept() (net.Conn, error) { return a.ln.Accept() }

func (a *Acceptor) AcceptRaw() (int, net.Conn, error) {
	raw, err := a.ln.SyscallConn()
	if err != nil {
		return 0, nil, err
	}

	var (
		connFd int
		conn   net.Conn
		acceptErr error
	)

	err = raw.Read(func(fd uintptr) bool {
		conn, acceptErr = a.ln.Accept()
		if acceptErr != nil {
			return true
		}
		// Extract fd from accepted connection
		tcpConn := conn.(*net.TCPConn)
		if cf, e := tcpConn.File(); e == nil {
			connFd = int(cf.Fd())
			cf.Close() // dup the fd; close File wrapper
		}
		return true
	})
	if err != nil {
		return 0, nil, err
	}
	if acceptErr != nil {
		return 0, nil, acceptErr
	}
	return connFd, conn, nil
}

// GetFD extracts the file descriptor from a net.Conn.
func GetFD(conn net.Conn) int {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		f, err := tcpConn.File()
		if err != nil {
			return -1
		}
		fd := int(f.Fd())
		f.Close() // dup; close File wrapper, fd still valid
		return fd
	}
	return -1
}

