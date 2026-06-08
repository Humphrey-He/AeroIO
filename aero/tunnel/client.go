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
)

// TransportType represents the type of tunnel transport
type TransportType int

const (
	// TransportTCP uses raw TCP connection
	TransportTCP TransportType = iota
	// TransportWebSocket uses WebSocket connection
	TransportWebSocket
)

// String returns the string representation of TransportType
func (t TransportType) String() string {
	switch t {
	case TransportTCP:
		return "tcp"
	case TransportWebSocket:
		return "websocket"
	default:
		return "unknown"
	}
}

// ParseTransportType parses a string to TransportType
func ParseTransportType(s string) (TransportType, error) {
	switch s {
	case "tcp":
		return TransportTCP, nil
	case "websocket", "ws":
		return TransportWebSocket, nil
	default:
		return TransportTCP, fmt.Errorf("unknown transport type: %s", s)
	}
}

// TunnelConfig holds the configuration for a tunnel connection
type TunnelConfig struct {
	ServerAddr  string
	AgentID     string
	Token       string // Authentication token
	Timeout     time.Duration
	Heartbeat   time.Duration
	Transport   TransportType      // Transport type (TCP or WebSocket)
	WSPath      string             // WebSocket path (for WebSocket transport)
	UseTLS      bool               // Use TLS (for WebSocket transport)
	Compression CompressionConfig // Compression settings
}

// ClientWithAutoFallback creates a client with automatic transport fallback.
// It first tries the specified transport, then falls back to the alternative if it fails.
func ClientWithAutoFallback(config TunnelConfig) (*Client, *WSTransport, error) {
	var lastErr error

	// Define transport order based on config
	transports := []TransportType{config.Transport}
	if config.Transport == TransportTCP {
		transports = append(transports, TransportWebSocket)
	} else {
		transports = append(transports, TransportTCP)
	}

	for _, transport := range transports {
		log.Printf("[Tunnel] Trying transport: %s", transport)

		switch transport {
		case TransportTCP:
			// Create TCP client
			tcpConfig := config
			tcpClient, err := NewClient(tcpConfig)
			if err != nil {
				lastErr = err
				continue
			}

			if err := tcpClient.Connect(); err != nil {
				tcpClient.Close()
				lastErr = err
				log.Printf("[Tunnel] TCP connection failed: %v", err)
				continue
			}

			log.Printf("[Tunnel] Connected via TCP successfully")
			return tcpClient, nil, nil

		case TransportWebSocket:
			// Create WebSocket transport
			wsConfig := WSTransportConfig{
				ServerAddr: config.ServerAddr,
				AgentID:    config.AgentID,
				Token:      config.Token,
				Timeout:    config.Timeout,
				Heartbeat:  config.Heartbeat,
				Path:       config.WSPath,
				TLS:        config.UseTLS,
			}

			wsTransport, err := NewWSTransport(wsConfig)
			if err != nil {
				lastErr = err
				continue
			}

			if err := wsTransport.Connect(); err != nil {
				wsTransport.Close()
				lastErr = err
				log.Printf("[Tunnel] WebSocket connection failed: %v", err)
				continue
			}

			log.Printf("[Tunnel] Connected via WebSocket successfully")
			return nil, wsTransport, nil
		}
	}

	return nil, nil, fmt.Errorf("all transports failed, last error: %w", lastErr)
}

// Client represents a tunnel client that connects to the server
type Client struct {
	config    TunnelConfig
	conn      net.Conn
	encoder   *Encoder
	decoder   *Decoder
	manager   *Manager
	sessionID string
	mu        sync.Mutex
	running   bool
	stopCh    chan struct{}
}

// NewClient creates a new tunnel client
func NewClient(config TunnelConfig) (*Client, error) {
	if config.Heartbeat == 0 {
		config.Heartbeat = 30 * time.Second
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.AgentID == "" {
		// Generate a random agent ID
		b := make([]byte, 16)
		rand.Read(b)
		config.AgentID = hex.EncodeToString(b)
	}

	return &Client{
		config:   config,
		manager:  NewManager(),
		sessionID: config.AgentID,
		stopCh:   make(chan struct{}),
	}, nil
}

// Connect establishes a connection to the tunnel server
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("already connected")
	}

	conn, err := net.DialTimeout("tcp", c.config.ServerAddr, c.config.Timeout)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	c.conn = conn
	c.encoder = NewEncoder(conn)
	c.decoder = NewDecoder(conn)
	c.running = true

	// Send auth message if token is configured
	if c.config.Token != "" {
		if err := c.encoder.Encode(NewAuthMessage(c.config.Token)); err != nil {
			conn.Close()
			c.running = false
			return fmt.Errorf("failed to send auth: %w", err)
		}

		// Wait for auth response
		resp, err := c.decoder.Decode()
		if err != nil {
			conn.Close()
			c.running = false
			return fmt.Errorf("failed to read auth response: %w", err)
		}

		if resp.Type == MsgError {
			conn.Close()
			c.running = false
			return fmt.Errorf("auth failed: %s", string(resp.Payload))
		}

		if resp.Type != MsgAck {
			conn.Close()
			c.running = false
			return fmt.Errorf("unexpected auth response: %d", resp.Type)
		}
	}

	// Send registration
	if err := c.encoder.Encode(NewRegisterMessage(c.sessionID)); err != nil {
		conn.Close()
		c.running = false
		return fmt.Errorf("failed to register: %w", err)
	}

	// Start read loop
	go c.readLoop()
	// Start heartbeat
	go c.heartbeatLoop()

	log.Printf("[Tunnel] Connected to server: %s", c.config.ServerAddr)
	return nil
}

// OpenPort requests to open a port on the server
func (c *Client) OpenPort(publicPort uint16, target string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running || c.conn == nil {
		return 0, fmt.Errorf("not connected")
	}

	msg := NewOpenPortMessage(publicPort, target)
	if err := c.encoder.Encode(msg); err != nil {
		return 0, err
	}

	// Wait for response
	resp, err := c.decoder.Decode()
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
func (c *Client) ClosePort(channelID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running || c.conn == nil {
		return fmt.Errorf("not connected")
	}

	msg := NewClosePortMessage(channelID)
	return c.encoder.Encode(msg)
}

// GetManager returns the channel manager
func (c *Client) GetManager() *Manager {
	return c.manager
}

// Close closes the tunnel client connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}

	close(c.stopCh)
	c.running = false

	if c.conn != nil {
		c.conn.Close()
	}

	c.manager.CloseAll()
	log.Printf("[Tunnel] Client closed")
	return nil
}

func (c *Client) readLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
			msg, err := c.decoder.Decode()
			if err != nil {
				if err != io.EOF {
					log.Printf("[Tunnel] Read error: %v", err)
				}
				c.Close()
				return
			}
			c.handleMessage(msg)
		}
	}
}

func (c *Client) handleMessage(msg *Message) {
	switch msg.Type {
	case MsgData:
		// Data for a channel
		ch, ok := c.manager.GetChannel(msg.ChannelID)
		if ok {
			ch.WriteAsync(msg.Payload)
		}

	case MsgAck:
		// Acknowledgment received

	case MsgError:
		log.Printf("[Tunnel] Server error: %s", string(msg.Payload))

	case MsgClosePort:
		// Server wants to close a channel
		c.manager.CloseChannel(msg.ChannelID)
	}
}

func (c *Client) heartbeatLoop() {
	ticker := time.NewTicker(c.config.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			if c.running && c.conn != nil {
				if err := c.encoder.Encode(NewHeartbeatMessage()); err != nil {
					log.Printf("[Tunnel] Heartbeat failed: %v", err)
					c.mu.Unlock()
					c.Close()
					return
				}
			}
			c.mu.Unlock()

		case <-c.stopCh:
			return
		}
	}
}

// Dial initiates a connection through the tunnel to a target service
func (c *Client) Dial(channelID uint64, target string) error {
	ch := c.manager.NewChannel()
	ch.ID = channelID

	// Open port on server
	id, err := c.OpenPort(0, target)
	if err != nil {
		c.manager.CloseChannel(ch.ID)
		return fmt.Errorf("failed to open port: %w", err)
	}

	ch.ID = id

	// Start a goroutine to read from channel and write to conn
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := ch.Read(buf)
			if err != nil {
				return
			}
			if n > 0 {
				c.mu.Lock()
				if c.running && c.conn != nil {
					msg := NewDataMessage(ch.ID, buf[:n])
					c.encoder.Encode(msg)
				}
				c.mu.Unlock()
			}
		}
	}()

	return nil
}

// PortMapping represents a port forwarding mapping
type PortMapping struct {
	PublicPort uint16 `json:"public_port"`
	LocalAddr  string `json:"local_addr"`
	Protocol   string `json:"protocol"`
}

// ProxyConfig holds the configuration for the local proxy
type ProxyConfig struct {
	ListenAddr   string       `json:"listen_addr"`
	ServerAddr   string       `json:"server_addr"`
	AgentID      string       `json:"agent_id"`
	LocalProxy   []PortMapping `json:"local_proxy"`
	Heartbeat    int          `json:"heartbeat"`
	ReconnectSec int          `json:"reconnect_sec"`
}

// DefaultProxyConfig returns a default proxy configuration
func DefaultProxyConfig() ProxyConfig {
	return ProxyConfig{
		ListenAddr:   "127.0.0.1:7000",
		ServerAddr:   "localhost:8888",
		AgentID:      "",
		LocalProxy:   []PortMapping{},
		Heartbeat:    30,
		ReconnectSec: 5,
	}
}

// LoadProxyConfig loads configuration from JSON
func LoadProxyConfig(data []byte) (*ProxyConfig, error) {
	var cfg ProxyConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = 30
	}
	if cfg.ReconnectSec == 0 {
		cfg.ReconnectSec = 5
	}
	return &cfg, nil
}
