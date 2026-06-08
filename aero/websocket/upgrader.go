package websocket

import (
	"net"

	"github.com/AeroIO/aero/http"
)

// Upgrader upgrades an HTTP connection to WebSocket.
type Upgrader struct{}

func NewUpgrader() *Upgrader {
	return &Upgrader{}
}

// Upgrade performs the WebSocket handshake and returns a WebSocket connection.
func (u *Upgrader) Upgrade(conn net.Conn, req *Request) (*Conn, error) {
	wsKey, ok := req.TryUpgrade()
	if !ok {
		return nil, errNotWebSocket
	}

	// Write HTTP 101 Switching Protocols response
	resp := BuildHandshakeResponse(wsKey)
	if _, err := conn.Write(resp); err != nil {
		return nil, err
	}

	wsConn := NewConn(conn)
	return wsConn, nil
}

// Request adapts our internal http.Request for the upgrader.
type Request = http.Request

var errNotWebSocket = &WSProtocolError{"not a websocket upgrade request"}

type WSProtocolError struct {
	msg string
}

func (e *WSProtocolError) Error() string { return e.msg }
