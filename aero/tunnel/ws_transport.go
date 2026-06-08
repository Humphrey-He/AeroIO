package tunnel

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/AeroIO/aero/websocket"
)

// WSTransportConfig holds the configuration for WebSocket transport
type WSTransportConfig struct {
	ServerAddr  string
	AgentID     string
	Timeout     time.Duration
	Heartbeat   time.Duration
	Path        string // WebSocket path, default "/tunnel"
	TLS         bool   // Use TLS
	TLSInsecure bool   // Skip TLS verification
}

// WSTransport implements a WebSocket-based tunnel transport
type WSTransport struct {
	config    WSTransportConfig
	conn      *websocket.Conn
	encoder   *Encoder
	decoder   *Decoder
	manager   *Manager
	sessionID string
	mu        sync.Mutex
	running   bool
	stopCh    chan struct{}
	onMessage func(*Message)
	onClose   func()
}

// NewWSTransport creates a new WebSocket transport client
func NewWSTransport(config WSTransportConfig) (*WSTransport, error) {
	if config.Heartbeat == 0 {
		config.Heartbeat = 30 * time.Second
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.Path == "" {
		config.Path = "/tunnel"
	}
	if config.AgentID == "" {
		// Generate a random agent ID
		b := make([]byte, 16)
		rand.Read(b)
		config.AgentID = hex.EncodeToString(b)
	}

	return &WSTransport{
		config:    config,
		manager:   NewManager(),
		sessionID: config.AgentID,
		stopCh:    make(chan struct{}),
	}, nil
}

// Connect establishes a WebSocket connection to the tunnel server
func (t *WSTransport) Connect() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.running {
		return fmt.Errorf("already connected")
	}

	// Build WebSocket URL
	scheme := "ws"
	if t.config.TLS {
		scheme = "wss"
	}
	url := fmt.Sprintf("%s://%s%s", scheme, t.config.ServerAddr, t.config.Path)

	// Dial raw TCP connection
	conn, err := net.DialTimeout("tcp", t.config.ServerAddr, t.config.Timeout)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	// Perform WebSocket handshake
	if err := t.performHandshake(conn, url); err != nil {
		conn.Close()
		return fmt.Errorf("WebSocket handshake failed: %w", err)
	}

	// Wrap with WebSocket connection
	t.conn = websocket.NewConn(conn)
	t.encoder = NewEncoder(&wsWriter{conn: t.conn})
	t.decoder = NewDecoder(&wsReader{conn: t.conn})
	t.running = true

	// Send registration
	if err := t.encoder.Encode(NewRegisterMessage(t.sessionID)); err != nil {
		t.conn.Close()
		t.running = false
		return fmt.Errorf("failed to register: %w", err)
	}

	// Start read loop
	go t.readLoop()
	// Start heartbeat
	go t.heartbeatLoop()

	log.Printf("[Tunnel-WS] Connected to server: %s", t.config.ServerAddr)
	return nil
}

// performHandshake performs the WebSocket handshake
func (t *WSTransport) performHandshake(conn net.Conn, url string) error {
	// Generate random key
	key := make([]byte, 16)
	rand.Read(key)
	keyStr := base64Encode(key)

	// Build handshake request
	host := t.config.ServerAddr
	request := fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n"+
			"Sec-WebSocket-Protocol: tunnel.v1\r\n"+
			"\r\n",
		t.config.Path, host, keyStr)

	if _, err := conn.Write([]byte(request)); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}

	// Read response
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(t.config.Timeout))
	n, err := conn.Read(buf)
	if err != nil {
		return fmt.Errorf("failed to read handshake response: %w", err)
	}

	// Simple response parsing - check for 101 Switching Protocols
	resp := string(buf[:n])
	if len(resp) < 12 || resp[:12] != "HTTP/1.1 101" {
		return fmt.Errorf("unexpected response: %s", resp[:min(100, len(resp))])
	}

	return nil
}

// base64Encode encodes bytes to base64
func base64Encode(data []byte) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	result := make([]byte, (len(data)+2)/3*4)
	for i := 0; i < len(data); i += 3 {
		var n uint32
		switch len(data) - i {
		case 1:
			n = uint32(data[i]) << 16
			result[i/3*4] = chars[n>>18&63]
			result[i/3*4+1] = chars[n>>12&63]
			result[i/3*4+2] = '='
			result[i/3*4+3] = '='
		case 2:
			n = uint32(data[i])<<16 | uint32(data[i+1])<<8
			result[i/3*4] = chars[n>>18&63]
			result[i/3*4+1] = chars[n>>12&63]
			result[i/3*4+2] = chars[n>>6&63]
			result[i/3*4+3] = '='
		default:
			n = uint32(data[i])<<16 | uint32(data[i+1])<<8 | uint32(data[i+2])
			result[i/3*4] = chars[n>>18&63]
			result[i/3*4+1] = chars[n>>12&63]
			result[i/3*4+2] = chars[n>>6&63]
			result[i/3*4+3] = chars[n&63]
		}
	}
	return string(result)
}

// wsWriter wraps WebSocket binary writes
type wsWriter struct {
	conn *websocket.Conn
}

func (w *wsWriter) Write(p []byte) (n int, err error) {
	// Write tunnel binary frames over WebSocket
	if err := w.conn.WriteMessage(websocket.MessageBinary, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// wsReader wraps WebSocket binary reads
type wsReader struct {
	conn *websocket.Conn
}

func (r *wsReader) Read(p []byte) (n int, err error) {
	// This is used with io.ReadFull, so we need actual reads
	// The read loop handles incoming messages
	return 0, io.EOF
}

// OpenPort requests to open a port on the server
func (t *WSTransport) OpenPort(publicPort uint16, target string) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.running || t.conn == nil {
		return 0, fmt.Errorf("not connected")
	}

	msg := NewOpenPortMessage(publicPort, target)
	if err := t.encoder.Encode(msg); err != nil {
		return 0, err
	}

	// Wait for response
	resp, err := t.decoder.Decode()
	if err != nil {
		return 0, err
	}

	if resp.Type == MsgAck {
		return resp.ChannelID, nil
	} else if resp.Type == MsgError {
		return 0, fmt.Errorf("server error: %s", string(resp.Payload))
	}

	return 0, fmt.Errorf("unexpected response type: %d", resp.Type)
}

// ClosePort closes an opened port
func (t *WSTransport) ClosePort(channelID uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.running || t.conn == nil {
		return fmt.Errorf("not connected")
	}

	msg := NewClosePortMessage(channelID)
	return t.encoder.Encode(msg)
}

// GetManager returns the channel manager
func (t *WSTransport) GetManager() *Manager {
	return t.manager
}

// Close closes the transport connection
func (t *WSTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.running {
		return nil
	}

	close(t.stopCh)
	t.running = false

	if t.conn != nil {
		t.conn.Close()
	}

	t.manager.CloseAll()
	log.Printf("[Tunnel-WS] Transport closed")
	return nil
}

// SetOnMessage sets a callback for incoming messages
func (t *WSTransport) SetOnMessage(fn func(*Message)) {
	t.onMessage = fn
}

// SetOnClose sets a callback for connection close
func (t *WSTransport) SetOnClose(fn func()) {
	t.onClose = fn
}

func (t *WSTransport) readLoop() {
	// Set up message handler
	t.conn.OnMessage(func(msgType int, data []byte) {
		if msgType != websocket.MessageBinary {
			return
		}

		// Decode tunnel message from the binary frame
		decoder := NewDecoder(&bytesReader{data: data})
		msg, err := decoder.Decode()
		if err != nil {
			log.Printf("[Tunnel-WS] Decode error: %v", err)
			return
		}

		t.handleMessage(msg)
	})

	t.conn.OnError(func(err error) {
		log.Printf("[Tunnel-WS] Connection error: %v", err)
		t.Close()
	})

	t.conn.OnClose(func(code uint16, text string) {
		log.Printf("[Tunnel-WS] Connection closed: %d %s", code, text)
		if t.onClose != nil {
			t.onClose()
		}
		t.Close()
	})

	// Start the read loop
	t.conn.ReadLoop()
}

func (t *WSTransport) handleMessage(msg *Message) {
	switch msg.Type {
	case MsgData:
		// Data for a channel
		ch, ok := t.manager.GetChannel(msg.ChannelID)
		if ok {
			ch.WriteAsync(msg.Payload)
		}

	case MsgAck:
		// Acknowledgment received

	case MsgError:
		log.Printf("[Tunnel-WS] Server error: %s", string(msg.Payload))

	case MsgClosePort:
		// Server wants to close a channel
		t.manager.CloseChannel(msg.ChannelID)
	}

	if t.onMessage != nil {
		t.onMessage(msg)
	}
}

func (t *WSTransport) heartbeatLoop() {
	ticker := time.NewTicker(t.config.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.mu.Lock()
			if t.running && t.conn != nil {
				// Send ping frame for WebSocket-level heartbeat
				if err := t.conn.Ping(nil); err != nil {
					log.Printf("[Tunnel-WS] Ping failed: %v", err)
					t.mu.Unlock()
					t.Close()
					return
				}
				// Also send tunnel heartbeat
				if err := t.encoder.Encode(NewHeartbeatMessage()); err != nil {
					log.Printf("[Tunnel-WS] Heartbeat failed: %v", err)
					t.mu.Unlock()
					t.Close()
					return
				}
			}
			t.mu.Unlock()

		case <-t.stopCh:
			return
		}
	}
}

// bytesReader implements io.Reader for byte slices
type bytesReader struct {
	data []byte
	pos  int
}

func (r bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

func (r bytesReader) ReadFull(p []byte) (n int, err error) {
	for r.pos < len(r.data) && n < len(p) {
		cnt := copy(p[n:], r.data[r.pos:])
		n += cnt
		r.pos += cnt
	}
	if n < len(p) {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

// WSTunnelMessage wraps a tunnel message for WebSocket transport
type WSTunnelMessage struct {
	Type      byte   `json:"type"`
	ChannelID uint64 `json:"channel_id"`
	Payload   []byte `json:"payload"`
}

// Encode encodes a tunnel message for WebSocket transport
func (m *WSTunnelMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// Decode decodes a tunnel message from WebSocket transport
func DecodeWSTunnelMessage(data []byte) (*WSTunnelMessage, error) {
	var msg WSTunnelMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
