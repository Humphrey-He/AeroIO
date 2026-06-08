package websocket

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"strings"
)

// websocketGUID is the magic string defined in RFC 6455 §4.2.2.
const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// ComputeAcceptKey computes the Sec-WebSocket-Accept value from the client's key.
func ComputeAcceptKey(clientKey string) string {
	h := sha1.New()
	h.Write([]byte(clientKey + websocketGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// BuildHandshakeResponse constructs the HTTP 101 Switching Protocols response.
func BuildHandshakeResponse(clientKey string) []byte {
	acceptKey := ComputeAcceptKey(strings.TrimSpace(clientKey))

	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		fmt.Sprintf("Sec-WebSocket-Accept: %s\r\n", acceptKey) +
		"\r\n"

	return []byte(response)
}
