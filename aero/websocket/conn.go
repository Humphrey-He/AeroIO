package websocket

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type Conn struct {
	raw    net.Conn
	reader io.Reader

	// Write mutex — only one writer at a time per RFC 6455.
	writeMu sync.Mutex

	// Incoming message assembly (for fragmented messages)
	msgBuf     []byte
	msgOpcode  byte

	// Close state
	closed   bool
	closeMu  sync.Mutex

	// Config
	readTimeout  time.Duration
	writeTimeout time.Duration
	pingPeriod   time.Duration

	// Handler callbacks
	onMessage func(msgType int, data []byte)
	onClose   func(code uint16, text string)
	onError   func(err error)

	done chan struct{}
}

const (
	MessageText   = 1
	MessageBinary = 2
)

func NewConn(raw net.Conn) *Conn {
	return &Conn{
		raw:         raw,
		reader:      raw,
		pingPeriod:  30 * time.Second,
		writeTimeout: 10 * time.Second,
		readTimeout:  60 * time.Second,
		done:        make(chan struct{}),
	}
}

func (c *Conn) OnMessage(fn func(msgType int, data []byte)) { c.onMessage = fn }
func (c *Conn) OnClose(fn func(code uint16, text string))   { c.onClose = fn }
func (c *Conn) OnError(fn func(err error))                   { c.onError = fn }

// ReadLoop continuously reads frames from the connection.
func (c *Conn) ReadLoop() {
	defer c.Close()

	for {
		c.raw.SetReadDeadline(time.Now().Add(c.readTimeout))

		f, err := ReadFrame(c.reader)
		if err != nil {
			if c.onError != nil {
				c.onError(err)
			}
			return
		}

		switch f.Opcode {
		case OpText, OpBinary:
			if f.Fin {
				// Complete message
				msgType := MessageText
				if f.Opcode == OpBinary {
					msgType = MessageBinary
				}
				if c.onMessage != nil {
					c.onMessage(msgType, c.msgBuf)
				}
				c.msgBuf = nil
			} else {
				// Start of fragmented message
				c.msgOpcode = f.Opcode
				c.msgBuf = append(c.msgBuf[:0], f.Payload...)
			}

		case OpContinuation:
			c.msgBuf = append(c.msgBuf, f.Payload...)
			if f.Fin {
				msgType := MessageText
				if c.msgOpcode == OpBinary {
					msgType = MessageBinary
				}
				if c.onMessage != nil {
					c.onMessage(msgType, c.msgBuf)
				}
				c.msgBuf = nil
			}

		case OpPing:
			c.writeFrame(NewPongFrame(f.Payload))

		case OpPong:
			// Pong received — heartbeat handled implicitly

		case OpClose:
			replyCode := f.CloseCode
			if replyCode == 0 {
				replyCode = closeNormalClosure
			}
			c.writeFrame(NewCloseFrame(replyCode, ""))
			if c.onClose != nil {
				c.onClose(f.CloseCode, f.CloseText)
			}
			return
		}
	}
}

// WriteMessage sends a text or binary message.
func (c *Conn) WriteMessage(msgType int, data []byte) error {
	var opcode byte
	switch msgType {
	case MessageText:
		opcode = OpText
	case MessageBinary:
		opcode = OpBinary
	default:
		return fmt.Errorf("unknown message type: %d", msgType)
	}
	return c.writeFrame(&Frame{Fin: true, Opcode: opcode, Payload: data})
}

func (c *Conn) writeFrame(f *Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closed {
		return io.ErrClosedPipe
	}

	c.raw.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	return WriteFrame(c.raw, f)
}

func (c *Conn) Ping(data []byte) error {
	return c.writeFrame(NewPingFrame(data))
}

// Close sends a close frame and closes the connection.
func (c *Conn) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	c.writeFrame(NewCloseFrame(closeNormalClosure, ""))
	return c.raw.Close()
}
