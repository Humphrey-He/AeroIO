package test

import (
	"bytes"
	"testing"

	"github.com/AeroIO/aero/rpc/protocol"
)

func TestRPCProtocolEncodeDecode(t *testing.T) {
	// Create a request message
	serviceMethod := "Arith.Add"
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	req := protocol.NewRequest(42, serviceMethod, payload)

	// Encode
	var buf bytes.Buffer
	if err := req.Encode(&buf); err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Decode
	decoded, err := protocol.Decode(&buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Version != protocol.Version1 {
		t.Errorf("expected version 1, got %d", decoded.Version)
	}
	if decoded.Type != protocol.TypeRequest {
		t.Errorf("expected TypeRequest, got %d", decoded.Type)
	}
	if decoded.RequestID != 42 {
		t.Errorf("expected RequestID 42, got %d", decoded.RequestID)
	}
	if string(decoded.Header) != serviceMethod {
		t.Errorf("expected header %q, got %q", serviceMethod, string(decoded.Header))
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Errorf("payload mismatch: %v != %v", decoded.Payload, payload)
	}
}

func TestRPCResponseEncodeDecode(t *testing.T) {
	resp := protocol.NewResponse(100, []byte("result"))
	var buf bytes.Buffer
	if err := resp.Encode(&buf); err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := protocol.Decode(&buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Type != protocol.TypeResponse {
		t.Errorf("expected TypeResponse, got %d", decoded.Type)
	}
	if decoded.RequestID != 100 {
		t.Errorf("expected RequestID 100, got %d", decoded.RequestID)
	}
	if string(decoded.Payload) != "result" {
		t.Errorf("expected 'result', got %q", string(decoded.Payload))
	}
}

func TestRPCErrorEncodeDecode(t *testing.T) {
	errMsg := protocol.NewError(99, "something went wrong")
	var buf bytes.Buffer
	if err := errMsg.Encode(&buf); err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := protocol.Decode(&buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Type != protocol.TypeError {
		t.Errorf("expected TypeError, got %d", decoded.Type)
	}
	if string(decoded.Header) != "something went wrong" {
		t.Errorf("expected error message, got %q", string(decoded.Header))
	}
}

func TestRPCInvalidMagic(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	_, err := protocol.Decode(buf)
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}
