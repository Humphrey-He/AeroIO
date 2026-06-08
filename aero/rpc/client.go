package rpc

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/AeroIO/aero/rpc/protocol"
)

// Client is the RPC client.
type Client struct {
	addr     string
	conn     net.Conn
	reqID    atomic.Uint64
	timeout  time.Duration
	pool     *ConnPool
	usePool  bool
}

func NewClient(addr string) *Client {
	return &Client{
		addr:    addr,
		timeout: 30 * time.Second,
	}
}

// WithPool enables connection pooling.
func (c *Client) WithPool(maxIdle, maxSize int) *Client {
	c.pool = NewConnPool(c.addr, maxIdle, maxSize)
	c.usePool = true
	return c
}

// Call invokes a remote method. serviceMethod is "ServiceName.Method".
func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	var conn net.Conn
	var err error

	if c.usePool {
		conn, err = c.pool.Get()
	} else {
		conn, err = c.dial()
	}
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	defer func() {
		if c.usePool {
			c.pool.Put(conn)
		} else {
			conn.Close()
		}
	}()

	// Set deadline
	conn.SetDeadline(time.Now().Add(c.timeout))

	// Encode args
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&args); err != nil {
		return fmt.Errorf("encode args: %w", err)
	}

	reqID := c.reqID.Add(1)
	req := protocol.NewRequest(reqID, serviceMethod, buf.Bytes())

	if err := req.Encode(conn); err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	resp, err := protocol.Decode(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.Type == protocol.TypeError {
		return fmt.Errorf("remote error: %s", string(resp.Header))
	}

	if reply != nil && len(resp.Payload) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(resp.Payload)).Decode(reply); err != nil {
			return fmt.Errorf("decode reply: %w", err)
		}
	}

	return nil
}

func (c *Client) dial() (net.Conn, error) {
	return net.DialTimeout("tcp", c.addr, 5*time.Second)
}

// Close closes the client and its connection pool.
func (c *Client) Close() error {
	if c.pool != nil {
		c.pool.Close()
	}
	return nil
}
