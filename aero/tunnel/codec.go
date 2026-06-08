package tunnel

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Encoder encodes tunnel messages to wire format
type Encoder struct {
	w io.Writer
}

// NewEncoder creates a new encoder
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

// Encode writes a message to the wire
func (e *Encoder) Encode(msg *Message) error {
	header := make([]byte, HeaderSize)

	// Magic (2 bytes)
	binary.BigEndian.PutUint16(header[0:2], Magic)

	// Type (1 byte)
	header[2] = msg.Type

	// ChannelID (8 bytes)
	binary.BigEndian.PutUint64(header[3:11], msg.ChannelID)

		// Length (4 bytes)
	binary.BigEndian.PutUint32(header[11:15], uint32(len(msg.Payload)))

	// Write header
	if _, err := e.w.Write(header); err != nil {
		return err
	}

	// Write payload if any
	if len(msg.Payload) > 0 {
		if _, err := e.w.Write(msg.Payload); err != nil {
			return err
		}
	}

	return nil
}

// Decoder decodes tunnel messages from wire format
type Decoder struct {
	r io.Reader
}

// NewDecoder creates a new decoder
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// Decode reads a message from the wire
func (d *Decoder) Decode() (*Message, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(d.r, header); err != nil {
		return nil, err
	}

	// Verify magic
	magic := binary.BigEndian.Uint16(header[0:2])
	if magic != Magic {
		return nil, fmt.Errorf("invalid magic: 0x%X", magic)
	}

	msg := &Message{
		Type:      header[2],
		ChannelID: binary.BigEndian.Uint64(header[3:11]),
	}

	length := binary.BigEndian.Uint32(header[11:15])
	if length > 0 {
		// Limit max payload size to 64KB
		if length > 65536 {
			return nil, fmt.Errorf("payload too large: %d bytes", length)
		}
		msg.Payload = make([]byte, length)
		if _, err := io.ReadFull(d.r, msg.Payload); err != nil {
			return nil, err
		}
	}

	return msg, nil
}
