package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Magic bytes for protocol identification
const Magic = 0xA3E0

// Message types
const (
	TypeRequest  byte = 0x01
	TypeResponse byte = 0x02
	TypeError    byte = 0x03
)

// Message is the RPC protocol message.
// Wire format:
//
//	Magic(2B) + Version(1B) + Type(1B) + Codec(1B) +
//	RequestID(8B) + HeaderLen(4B) + PayloadLen(4B) +
//	Header(HeaderLen bytes) + Payload(PayloadLen bytes)
type Message struct {
	Version   byte
	Type      byte
	Codec     byte
	RequestID uint64
	Header    []byte
	Payload   []byte
}

const (
	HeaderSize   = 21 // 2+1+1+1+8+4+4
	Version1     = 1
	CodecGob     = 1
	CodecJSON    = 2
	CodecProtobuf = 3
)

func NewRequest(requestID uint64, serviceMethod string, payload []byte) *Message {
	header := []byte(serviceMethod)
	return &Message{
		Version:   Version1,
		Type:      TypeRequest,
		Codec:     CodecGob,
		RequestID: requestID,
		Header:    header,
		Payload:   payload,
	}
}

func NewResponse(requestID uint64, payload []byte) *Message {
	return &Message{
		Version:   Version1,
		Type:      TypeResponse,
		Codec:     CodecGob,
		RequestID: requestID,
		Payload:   payload,
	}
}

func NewError(requestID uint64, errMsg string) *Message {
	return &Message{
		Version:   Version1,
		Type:      TypeError,
		Codec:     CodecGob,
		RequestID: requestID,
		Header:    []byte(errMsg),
	}
}

// Encode serializes the message to the wire format.
func (m *Message) Encode(w io.Writer) error {
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint16(header[0:2], Magic)
	header[2] = m.Version
	header[3] = m.Type
	header[4] = m.Codec
	binary.BigEndian.PutUint64(header[5:13], m.RequestID)
	binary.BigEndian.PutUint32(header[13:17], uint32(len(m.Header)))
	binary.BigEndian.PutUint32(header[17:21], uint32(len(m.Payload)))

	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(m.Header) > 0 {
		if _, err := w.Write(m.Header); err != nil {
			return err
		}
	}
	if len(m.Payload) > 0 {
		if _, err := w.Write(m.Payload); err != nil {
			return err
		}
	}
	return nil
}

// Decode deserializes a message from the wire format.
func Decode(r io.Reader) (*Message, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	magic := binary.BigEndian.Uint16(header[0:2])
	if magic != Magic {
		return nil, fmt.Errorf("invalid magic: 0x%X", magic)
	}

	m := &Message{
		Version:   header[2],
		Type:      header[3],
		Codec:     header[4],
		RequestID: binary.BigEndian.Uint64(header[5:13]),
	}

	headerLen := binary.BigEndian.Uint32(header[13:17])
	payloadLen := binary.BigEndian.Uint32(header[17:21])

	if headerLen > 0 {
		m.Header = make([]byte, headerLen)
		if _, err := io.ReadFull(r, m.Header); err != nil {
			return nil, err
		}
	}
	if payloadLen > 0 {
		m.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, m.Payload); err != nil {
			return nil, err
		}
	}

	return m, nil
}
