package tunnel

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Channel represents a bidirectional data tunnel between two endpoints.
// It wraps a read/write stream and provides channel ID based routing.
type Channel struct {
	ID       uint64
	closed   atomic.Bool
	closeCh  chan struct{}
	readCh   chan []byte
	writeCh  chan []byte
	mu       sync.RWMutex
	onClose  func(*Channel)
	onData   func(*Channel, []byte)
	readErr  error
}

// NewChannel creates a new channel with the given ID
func NewChannel(id uint64) *Channel {
	return &Channel{
		ID:      id,
		closeCh: make(chan struct{}),
		readCh:  make(chan []byte, 256),  // Buffered for backpressure
		writeCh: make(chan []byte, 256),
	}
}

// SetOnClose sets the callback for channel close events
func (c *Channel) SetOnClose(fn func(*Channel)) {
	c.mu.Lock()
	c.onClose = fn
	c.mu.Unlock()
}

// SetOnData sets the callback for incoming data
func (c *Channel) SetOnData(fn func(*Channel, []byte)) {
	c.mu.Lock()
	c.onData = fn
	c.mu.Unlock()
}

// Read reads data from the channel
func (c *Channel) Read(p []byte) (int, error) {
	select {
	case data := <-c.readCh:
		if data == nil {
			return 0, io.EOF
		}
		n := copy(p, data)
		// Put back remaining data
		if len(data) > n {
			c.readCh <- data[n:]
		}
		return n, nil
	case <-c.closeCh:
		return 0, io.EOF
	}
}

// Write writes data to the channel
func (c *Channel) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	select {
	case c.writeCh <- p:
		return len(p), nil
	case <-c.closeCh:
		return 0, io.ErrClosedPipe
	}
}

// WriteAsync writes data asynchronously, returns immediately
func (c *Channel) WriteAsync(p []byte) bool {
	if c.closed.Load() {
		return false
	}

	select {
	case c.writeCh <- p:
		return true
	case <-c.closeCh:
		return false
	default:
		// Channel full, drop data
		return false
	}
}

// Close closes the channel
func (c *Channel) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.closeCh)

	c.mu.RLock()
	onClose := c.onClose
	c.mu.RUnlock()

	if onClose != nil {
		onClose(c)
	}

	return nil
}

// IsClosed returns whether the channel is closed
func (c *Channel) IsClosed() bool {
	return c.closed.Load()
}

// ReadCh returns the read channel for external consumption
func (c *Channel) ReadCh() <-chan []byte {
	return c.readCh
}

// WriteCh returns the write channel for external production
func (c *Channel) WriteCh() chan<- []byte {
	return c.writeCh
}

// CloseCh returns the close signal channel
func (c *Channel) CloseCh() <-chan struct{} {
	return c.closeCh
}

// ReadLoop starts a goroutine that reads from the source and feeds to readCh
func (c *Channel) ReadLoop(reader io.Reader, conn net.Conn, bufSize int) {
	go func() {
		buf := make([]byte, bufSize)
		for {
			if c.closed.Load() {
				return
			}
			if conn != nil {
				conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			}
			n, err := reader.Read(buf)
			if err != nil {
				c.Close()
				return
			}
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				select {
				case c.readCh <- data:
				case <-c.closeCh:
					return
				}
			}
		}
	}()
}

// WriteLoop starts a goroutine that writes from writeCh to the destination
func (c *Channel) WriteLoop(writer io.Writer) {
	go func() {
		for {
			select {
			case data := <-c.writeCh:
				_, err := writer.Write(data)
				if err != nil {
					c.Close()
					return
				}
			case <-c.closeCh:
				return
			}
		}
	}()
}

// Pipe creates a bidirectional pipe between two channels
func Pipe(ch1, ch2 *Channel) {
	// ch1 -> ch2
	go func() {
		for data := range ch1.ReadCh() {
			if !ch2.WriteAsync(data) {
				return
			}
		}
		ch2.Close()
	}()

	// ch2 -> ch1
	go func() {
		for data := range ch2.ReadCh() {
			if !ch1.WriteAsync(data) {
				return
			}
		}
		ch1.Close()
	}()
}
