package websocket

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Opcodes defined by RFC 6455.
const (
	OpContinuation = 0x0
	OpText         = 0x1
	OpBinary       = 0x2
	OpClose        = 0x8
	OpPing         = 0x9
	OpPong         = 0xA
)

const (
	closeNormalClosure    = 1000
	closeGoingAway        = 1001
	closeProtocolError    = 1002
	closeUnsupportedData  = 1003
	closeInvalidPayload   = 1007
	closePolicyViolation  = 1008
	closeMessageTooBig    = 1009
	closeInternalError    = 1011
)

// Frame represents a WebSocket frame.
type Frame struct {
	Fin       bool
	Opcode    byte
	Mask      bool
	Payload   []byte
	MaskKey   [4]byte
	CloseCode uint16
	CloseText string
}

// ReadFrame reads a single WebSocket frame from r.
func ReadFrame(r io.Reader) (*Frame, error) {
	// First 2 bytes: FIN+RSV+Opcode | MASK+PayloadLen
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	f := &Frame{
		Fin:    header[0]&0x80 != 0,
		Opcode: header[0] & 0x0F,
		Mask:   header[1]&0x80 != 0,
	}

	length := uint64(header[1] & 0x7F)

	// Extended payload length
	switch length {
	case 126:
		ext := make([]byte, 2)
		if _, err := io.ReadFull(r, ext); err != nil {
			return nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := io.ReadFull(r, ext); err != nil {
			return nil, err
		}
		length = binary.BigEndian.Uint64(ext)
	}

	// Masking key (client-to-server frames must be masked)
	if f.Mask {
		if _, err := io.ReadFull(r, f.MaskKey[:]); err != nil {
			return nil, err
		}
	}

	if length > 0 {
		// Reasonable limit: 1MB
		if length > 1<<20 {
			return nil, fmt.Errorf("frame too large: %d bytes", length)
		}
		f.Payload = make([]byte, length)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return nil, err
		}
		// Unmask
		if f.Mask {
			maskBytes(f.Payload, f.MaskKey)
		}
	}

	// Parse close frame
	if f.Opcode == OpClose && len(f.Payload) >= 2 {
		f.CloseCode = binary.BigEndian.Uint16(f.Payload[:2])
		f.CloseText = string(f.Payload[2:])
	}

	return f, nil
}

// WriteFrame writes a WebSocket frame to w.
func WriteFrame(w io.Writer, f *Frame) error {
	// Byte 0: FIN + Opcode
	b0 := f.Opcode & 0x0F
	if f.Fin {
		b0 |= 0x80
	}

	// Byte 1+: MASK + PayloadLen
	payloadLen := len(f.Payload)

	var frame []byte
	if payloadLen < 126 {
		frame = make([]byte, 2+payloadLen)
		frame[0] = b0
		frame[1] = byte(payloadLen)
		copy(frame[2:], f.Payload)
	} else if payloadLen < 65536 {
		frame = make([]byte, 4+payloadLen)
		frame[0] = b0
		frame[1] = 126
		binary.BigEndian.PutUint16(frame[2:4], uint16(payloadLen))
		copy(frame[4:], f.Payload)
	} else {
		frame = make([]byte, 10+payloadLen)
		frame[0] = b0
		frame[1] = 127
		binary.BigEndian.PutUint64(frame[2:10], uint64(payloadLen))
		copy(frame[10:], f.Payload)
	}

	_, err := w.Write(frame)
	return err
}

func maskBytes(data []byte, key [4]byte) {
	for i := 0; i < len(data); i++ {
		data[i] ^= key[i%4]
	}
}

// Convenience constructors

func NewTextFrame(data []byte) *Frame {
	return &Frame{Fin: true, Opcode: OpText, Payload: data}
}

func NewBinaryFrame(data []byte) *Frame {
	return &Frame{Fin: true, Opcode: OpBinary, Payload: data}
}

func NewPingFrame(data []byte) *Frame {
	return &Frame{Fin: true, Opcode: OpPing, Payload: data}
}

func NewPongFrame(data []byte) *Frame {
	return &Frame{Fin: true, Opcode: OpPong, Payload: data}
}

func NewCloseFrame(code uint16, text string) *Frame {
	payload := make([]byte, 2+len(text))
	binary.BigEndian.PutUint16(payload, code)
	copy(payload[2:], text)
	return &Frame{Fin: true, Opcode: OpClose, Payload: payload}
}
