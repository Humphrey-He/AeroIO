package test

import (
	"bytes"
	"testing"

	"github.com/AeroIO/aero/tunnel"
)

func TestTunnelProtocolEncodeDecode(t *testing.T) {
	// Test Register message
	t.Run("RegisterMessage", func(t *testing.T) {
		msg := tunnel.NewRegisterMessage("test-agent")
		var buf bytes.Buffer
		enc := tunnel.NewEncoder(&buf)
		if err := enc.Encode(msg); err != nil {
			t.Fatalf("encode error: %v", err)
		}

		dec := tunnel.NewDecoder(&buf)
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		if decoded.Type != tunnel.MsgRegister {
			t.Errorf("expected MsgRegister (%d), got %d", tunnel.MsgRegister, decoded.Type)
		}
		if string(decoded.Payload) != "test-agent" {
			t.Errorf("expected 'test-agent', got %q", string(decoded.Payload))
		}
	})

	// Test OpenPort message
	t.Run("OpenPortMessage", func(t *testing.T) {
		msg := tunnel.NewOpenPortMessage(8080, "localhost:3000")
		var buf bytes.Buffer
		enc := tunnel.NewEncoder(&buf)
		if err := enc.Encode(msg); err != nil {
			t.Fatalf("encode error: %v", err)
		}

		dec := tunnel.NewDecoder(&buf)
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		if decoded.Type != tunnel.MsgOpenPort {
			t.Errorf("expected MsgOpenPort (%d), got %d", tunnel.MsgOpenPort, decoded.Type)
		}
	})

	// Test Data message
	t.Run("DataMessage", func(t *testing.T) {
		msg := tunnel.NewDataMessage(123, []byte("hello world"))
		var buf bytes.Buffer
		enc := tunnel.NewEncoder(&buf)
		if err := enc.Encode(msg); err != nil {
			t.Fatalf("encode error: %v", err)
		}

		dec := tunnel.NewDecoder(&buf)
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		if decoded.Type != tunnel.MsgData {
			t.Errorf("expected MsgData (%d), got %d", tunnel.MsgData, decoded.Type)
		}
		if decoded.ChannelID != 123 {
			t.Errorf("expected ChannelID 123, got %d", decoded.ChannelID)
		}
		if string(decoded.Payload) != "hello world" {
			t.Errorf("expected 'hello world', got %q", string(decoded.Payload))
		}
	})

	// Test Heartbeat message
	t.Run("HeartbeatMessage", func(t *testing.T) {
		msg := tunnel.NewHeartbeatMessage()
		var buf bytes.Buffer
		enc := tunnel.NewEncoder(&buf)
		if err := enc.Encode(msg); err != nil {
			t.Fatalf("encode error: %v", err)
		}

		dec := tunnel.NewDecoder(&buf)
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		if decoded.Type != tunnel.MsgHeartbeat {
			t.Errorf("expected MsgHeartbeat (%d), got %d", tunnel.MsgHeartbeat, decoded.Type)
		}
	})

	// Test Error message
	t.Run("ErrorMessage", func(t *testing.T) {
		msg := tunnel.NewErrorMessage(456, "connection refused")
		var buf bytes.Buffer
		enc := tunnel.NewEncoder(&buf)
		if err := enc.Encode(msg); err != nil {
			t.Fatalf("encode error: %v", err)
		}

		dec := tunnel.NewDecoder(&buf)
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		if decoded.Type != tunnel.MsgError {
			t.Errorf("expected MsgError (%d), got %d", tunnel.MsgError, decoded.Type)
		}
		if decoded.ChannelID != 456 {
			t.Errorf("expected ChannelID 456, got %d", decoded.ChannelID)
		}
		if string(decoded.Payload) != "connection refused" {
			t.Errorf("expected 'connection refused', got %q", string(decoded.Payload))
		}
	})
}

func TestTunnelChannel(t *testing.T) {
	ch := tunnel.NewChannel(100)

	// Test WriteAsync
	if !ch.WriteAsync([]byte("test")) {
		t.Error("WriteAsync should succeed")
	}

	// Test IsClosed
	if ch.IsClosed() {
		t.Error("Channel should not be closed")
	}

	// Test Close
	ch.Close()
	if !ch.IsClosed() {
		t.Error("Channel should be closed after Close()")
	}

	// Test double close
	ch.Close() // Should not panic
}

func TestTunnelManager(t *testing.T) {
	mgr := tunnel.NewManager()

	// Test NewChannel
	ch := mgr.NewChannel()
	if ch.ID == 0 {
		t.Error("Channel ID should not be 0")
	}

	// Test GetChannel
	got, ok := mgr.GetChannel(ch.ID)
	if !ok {
		t.Error("GetChannel should find the channel")
	}
	if got.ID != ch.ID {
		t.Errorf("Expected channel ID %d, got %d", ch.ID, got.ID)
	}

	// Test ChannelCount
	if mgr.ChannelCount() != 1 {
		t.Errorf("Expected 1 channel, got %d", mgr.ChannelCount())
	}

	// Test RemoveChannel
	mgr.RemoveChannel(ch.ID)
	if mgr.ChannelCount() != 0 {
		t.Errorf("Expected 0 channels after RemoveChannel, got %d", mgr.ChannelCount())
	}

	// Test NewChannel generates unique IDs
	ch2 := mgr.NewChannel()
	ch3 := mgr.NewChannel()
	if ch2.ID == ch3.ID {
		t.Error("Channel IDs should be unique")
	}
}
