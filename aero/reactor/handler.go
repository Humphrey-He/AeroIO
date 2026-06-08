package reactor

// EventType represents the type of I/O event.
type EventType uint32

const (
	EventRead  EventType = 1 << 0
	EventWrite EventType = 1 << 1
	EventError EventType = 1 << 2
	EventClose EventType = 1 << 3
	EventTimer EventType = 1 << 4
)

// Handler is the callback interface for I/O events.
type Handler interface {
	OnRead(fd int) error
	OnWrite(fd int) error
	OnClose(fd int, err error)
}

// BaseHandler provides no-op defaults so concrete handlers only override what they need.
type BaseHandler struct{}

func (BaseHandler) OnRead(fd int) error        { return nil }
func (BaseHandler) OnWrite(fd int) error        { return nil }
func (BaseHandler) OnClose(fd int, err error)   {}
