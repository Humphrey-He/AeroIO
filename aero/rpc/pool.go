package rpc

import (
	"net"
	"sync"
	"time"
)

// ConnPool manages a pool of reusable TCP connections to RPC servers.
type ConnPool struct {
	mu      sync.Mutex
	addr    string
	conns   []*pooledConn
	maxIdle int
	maxSize int
	total   int
}

type pooledConn struct {
	conn      net.Conn
	createdAt time.Time
	lastUsed  time.Time
}

func NewConnPool(addr string, maxIdle, maxSize int) *ConnPool {
	return &ConnPool{
		addr:    addr,
		maxIdle: maxIdle,
		maxSize: maxSize,
	}
}

// Get returns a connection from the pool or creates a new one.
func (p *ConnPool) Get() (net.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Try to reuse an idle connection
	for len(p.conns) > 0 {
		pc := p.conns[len(p.conns)-1]
		p.conns = p.conns[:len(p.conns)-1]

		// Check if connection is still alive
		pc.conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
		buf := make([]byte, 1)
		_, err := pc.conn.Read(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Connection is alive (read timeout means no data, not dead)
				pc.conn.SetReadDeadline(time.Time{})
				return pc.conn, nil
			}
			// Connection is dead, close and try next
			pc.conn.Close()
			p.total--
			continue
		}
		// Unexpected data on idle connection
		pc.conn.Close()
		p.total--
	}

	// Create new connection
	if p.maxSize > 0 && p.total >= p.maxSize {
		return nil, errPoolExhausted
	}

	conn, err := net.DialTimeout("tcp", p.addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	p.total++
	return conn, nil
}

// Put returns a connection to the pool.
func (p *ConnPool) Put(conn net.Conn) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.conns) >= p.maxIdle {
		conn.Close()
		p.total--
		return
	}

	p.conns = append(p.conns, &pooledConn{
		conn:      conn,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
	})
}

// Close closes all idle connections in the pool.
func (p *ConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, pc := range p.conns {
		pc.conn.Close()
	}
	p.conns = nil
	p.total = 0
}

// CleanExpired closes connections idle longer than maxIdleTime.
func (p *ConnPool) CleanExpired(maxIdleTime time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	alive := p.conns[:0]
	for _, pc := range p.conns {
		if now.Sub(pc.lastUsed) > maxIdleTime {
			pc.conn.Close()
			p.total--
		} else {
			alive = append(alive, pc)
		}
	}
	p.conns = alive
}

var errPoolExhausted = &PoolExhaustedError{"connection pool exhausted"}

type PoolExhaustedError struct{ msg string }

func (e *PoolExhaustedError) Error() string { return e.msg }
