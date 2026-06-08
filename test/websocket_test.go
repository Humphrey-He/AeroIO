package test

import (
	"testing"

	"github.com/AeroIO/aero/websocket"
)

func TestComputeAcceptKey(t *testing.T) {
	// RFC 6455 §4.2.2 example
	clientKey := "dGhlIHNhbXBsZSBub25jZQ=="
	expected := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="

	accept := websocket.ComputeAcceptKey(clientKey)
	if accept != expected {
		t.Errorf("expected %q, got %q", expected, accept)
	}
}

func TestHandshakeResponse(t *testing.T) {
	clientKey := "dGhlIHNhbXBsZSBub25jZQ=="
	resp := websocket.BuildHandshakeResponse(clientKey)
	respStr := string(resp)

	if !contains(respStr, "101 Switching Protocols") {
		t.Error("expected 101 status")
	}
	if !contains(respStr, "Upgrade: websocket") {
		t.Error("expected Upgrade header")
	}
	if !contains(respStr, "Connection: Upgrade") {
		t.Error("expected Connection header")
	}
	if !contains(respStr, "Sec-WebSocket-Accept:") {
		t.Error("expected Sec-WebSocket-Accept header")
	}
}

func TestFrameEncoding(t *testing.T) {
	// Test that frame encoding/decoding is consistent
	// Text frame with "Hello"
	frame := websocket.NewTextFrame([]byte("Hello"))

	if frame.Fin != true {
		t.Error("expected Fin=true")
	}
	if frame.Opcode != websocket.OpText {
		t.Errorf("expected OpText, got %d", frame.Opcode)
	}
	if string(frame.Payload) != "Hello" {
		t.Errorf("expected 'Hello', got %q", string(frame.Payload))
	}
}

func TestCloseFrame(t *testing.T) {
	frame := websocket.NewCloseFrame(1000, "Normal Closure")

	if frame.Opcode != websocket.OpClose {
		t.Errorf("expected OpClose, got %d", frame.Opcode)
	}
	if len(frame.Payload) < 2 {
		t.Error("close frame payload too short")
	}
}
