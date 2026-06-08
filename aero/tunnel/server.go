package tunnel

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ServerConfig holds the configuration for a tunnel server
type ServerConfig struct {
	ListenAddr   string
	PortRange    [2]uint16 // Min and max port for dynamic allocation
	Timeout      time.Duration
	Heartbeat    time.Duration
	AuthTokens   []string   // Allowed authentication tokens
	Compression  CompressionConfig // Compression settings
}

// DefaultServerConfig returns a default server configuration
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		ListenAddr:  ":8888",
		PortRange:   [2]uint16{10000, 60000},
		Timeout:     60 * time.Second,
		Heartbeat:   30 * time.Second,
	}
}

// Server represents a tunnel server
type Server struct {
	config     ServerConfig
	listener   net.Listener
	sessions   *SessionManager
	portMap    map[uint16]*PortBinding
	nextPort   uint64
	mu         sync.RWMutex
	running    atomic.Bool
	stopCh     chan struct{}
	onPortOpen   func(*PortBinding)  // Callback when a port is opened
	onPortClose  func(*PortBinding) // Callback when a port is closed
	onClientConn func(net.Conn)     // Callback for client connections
}

// PortBinding represents a port binding on the server
type PortBinding struct {
	Port       uint16
	TargetAddr string
	SessionID  string
	ChannelID  uint64
	CreatedAt  time.Time
}

// NewServer creates a new tunnel server
func NewServer(config ServerConfig) *Server {
	return &Server{
		config:   config,
		sessions: NewSessionManager(30*time.Second, config.Timeout),
		portMap:  make(map[uint16]*PortBinding),
		stopCh:   make(chan struct{}),
		nextPort: uint64(config.PortRange[0]),
	}
}

// SetOnPortOpen sets the callback for port open events
func (s *Server) SetOnPortOpen(fn func(*PortBinding)) {
	s.onPortOpen = fn
}

// SetOnPortClose sets the callback for port close events
func (s *Server) SetOnPortClose(fn func(*PortBinding)) {
	s.onPortClose = fn
}

// SetOnClientConn sets the callback for client connections
func (s *Server) SetOnClientConn(fn func(net.Conn)) {
	s.onClientConn = fn
}

// ListenAndServe starts the tunnel server
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = ln
	s.running.Store(true)
	s.sessions.Cleanup()

	log.Printf("[Tunnel] Server listening on %s", s.config.ListenAddr)

	for s.running.Load() {
		conn, err := ln.Accept()
		if err != nil {
			if !s.running.Load() {
				return nil
			}
			log.Printf("[Tunnel] Accept error: %v", err)
			continue
		}

		if s.onClientConn != nil {
			s.onClientConn(conn)
		} else {
			go s.handleConnection(conn)
		}
	}

	return nil
}

// Stop stops the tunnel server
func (s *Server) Stop() {
	s.running.Store(false)
	if s.listener != nil {
		s.listener.Close()
	}
	s.sessions.Stop()
	s.mu.Lock()
	for _, binding := range s.portMap {
		if s.onPortClose != nil {
			s.onPortClose(binding)
		}
	}
	s.mu.Unlock()
	log.Printf("[Tunnel] Server stopped")
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	encoder := NewEncoder(conn)
	decoder := NewDecoder(conn)

	// If auth tokens are configured, require authentication
	if len(s.config.AuthTokens) > 0 {
		// Read auth message
		msg, err := decoder.Decode()
		if err != nil {
			log.Printf("[Tunnel] Failed to read auth message: %v", err)
			return
		}

		if msg.Type != MsgAuth {
			log.Printf("[Tunnel] Expected auth message, got: %d", msg.Type)
			encoder.Encode(NewErrorMessage(0, "authentication required"))
			return
		}

		token := string(msg.Payload)
		if !s.validateToken(token) {
			log.Printf("[Tunnel] Invalid authentication token from %s", conn.RemoteAddr())
			encoder.Encode(NewErrorMessage(0, "invalid token"))
			return
		}

		encoder.Encode(NewAckMessage(ChannelIDControl))
		log.Printf("[Tunnel] Client authenticated from %s", conn.RemoteAddr())
	}

	// Read registration message
	msg, err := decoder.Decode()
	if err != nil {
		log.Printf("[Tunnel] Failed to read registration: %v", err)
		return
	}

	if msg.Type != MsgRegister {
		log.Printf("[Tunnel] Expected registration message, got: %d", msg.Type)
		return
	}

	sessionID := string(msg.Payload)
	session := NewSession(sessionID)
	session.Touch()
	s.sessions.Register(session)
	defer s.sessions.Unregister(sessionID)

	log.Printf("[Tunnel] Client connected: %s", sessionID)

	// Connection handler
	for {
		select {
		case <-s.stopCh:
			return
		default:
			conn.SetReadDeadline(time.Now().Add(s.config.Heartbeat + 5*time.Second))
			msg, err := decoder.Decode()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// Timeout, send heartbeat check
					session.Touch()
					continue
				}
				log.Printf("[Tunnel] Client disconnected: %s (%v)", sessionID, err)
				return
			}
			session.Touch()
			s.handleMessage(session, msg, encoder)
		}
	}
}

func (s *Server) handleMessage(session *Session, msg *Message, encoder *Encoder) {
	switch msg.Type {
	case MsgOpenPort:
		// Parse port and target
		if len(msg.Payload) < 4 {
			encoder.Encode(NewErrorMessage(0, "invalid payload"))
			return
		}

		targetLen := binary.BigEndian.Uint16(msg.Payload[2:4])
		if len(msg.Payload) < 4+int(targetLen) {
			encoder.Encode(NewErrorMessage(0, "invalid payload"))
			return
		}

		targetAddr := string(msg.Payload[4 : 4+targetLen])
		var publicPort uint16

		// Allocate port
		if binary.BigEndian.Uint16(msg.Payload[0:2]) > 0 {
			publicPort = binary.BigEndian.Uint16(msg.Payload[0:2])
		} else {
			publicPort = s.allocatePort()
		}

		// Create channel
		ch := session.Manager.NewChannel()

		// Register port binding
		binding := &PortBinding{
			Port:       publicPort,
			TargetAddr: targetAddr,
			SessionID:  session.ID,
			ChannelID:  ch.ID,
			CreatedAt:  time.Now(),
		}
		s.mu.Lock()
		s.portMap[publicPort] = binding
		s.mu.Unlock()

		ch.SetOnClose(func(c *Channel) {
			s.mu.Lock()
			delete(s.portMap, publicPort)
			s.mu.Unlock()
			if s.onPortClose != nil {
				s.onPortClose(binding)
			}
		})

		// Start data relay
		go s.relayData(session, ch, encoder)

		if s.onPortOpen != nil {
			s.onPortOpen(binding)
		}

		log.Printf("[Tunnel] Port opened: :%d -> %s (channel: %d)", publicPort, targetAddr, ch.ID)
		encoder.Encode(NewAckMessage(ch.ID))

	case MsgHeartbeat:
		encoder.Encode(NewAckMessage(ChannelIDControl))

	case MsgData:
		ch, ok := session.Manager.GetChannel(msg.ChannelID)
		if ok {
			ch.WriteAsync(msg.Payload)
		}

	case MsgClosePort:
		session.Manager.CloseChannel(msg.ChannelID)
		encoder.Encode(NewAckMessage(msg.ChannelID))

	default:
		encoder.Encode(NewErrorMessage(msg.ChannelID, "unknown message type"))
	}
}

// validateToken checks if the provided token is in the allowed list
func (s *Server) validateToken(token string) bool {
	for _, t := range s.config.AuthTokens {
		if t == token {
			return true
		}
	}
	return false
}

func (s *Server) relayData(session *Session, ch *Channel, encoder *Encoder) {
	for {
		select {
		case data := <-ch.ReadCh():
			if data == nil {
				return
			}
			msg := NewDataMessage(ch.ID, data)
			if err := encoder.Encode(msg); err != nil {
				ch.Close()
				return
			}
		case <-ch.CloseCh():
			return
		}
	}
}

func (s *Server) allocatePort() uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()

	portRange := uint64(s.config.PortRange[1] - s.config.PortRange[0])
	for i := uint64(0); i < portRange; i++ {
		s.nextPort++
		if s.nextPort > uint64(s.config.PortRange[1]) {
			s.nextPort = uint64(s.config.PortRange[0])
		}
		port := uint16(s.nextPort)

		_, exists := s.portMap[port]
		if !exists {
			return port
		}
	}
	return 0 // No available ports
}

// GetPortBinding returns the port binding for a public port
func (s *Server) GetPortBinding(port uint16) (*PortBinding, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	binding, ok := s.portMap[port]
	return binding, ok
}

// ListPortBindings returns all port bindings
func (s *Server) ListPortBindings() []*PortBinding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bindings := make([]*PortBinding, 0, len(s.portMap))
	for _, b := range s.portMap {
		bindings = append(bindings, b)
	}
	return bindings
}

// StartPortForwarder starts a TCP port forwarder for a specific port
func (s *Server) StartPortForwarder(publicPort uint16, targetAddr string) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", publicPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", publicPort, err)
	}

	go func() {
		defer ln.Close()
		for s.running.Load() {
			clientConn, err := ln.Accept()
			if err != nil {
				continue
			}

			go s.handlePortForward(clientConn, publicPort, targetAddr)
		}
	}()

	log.Printf("[Tunnel] Port forwarder started: :%d -> %s", publicPort, targetAddr)
	return nil
}

func (s *Server) handlePortForward(clientConn net.Conn, publicPort uint16, targetAddr string) {
	defer clientConn.Close()

	// Find the channel for this port
	binding, ok := s.GetPortBinding(publicPort)
	if !ok {
		log.Printf("[Tunnel] No binding for port %d", publicPort)
		return
	}

	session, ok := s.sessions.Get(binding.SessionID)
	if !ok {
		log.Printf("[Tunnel] No session for port %d", publicPort)
		return
	}

	// Create a new channel for this connection
	ch := session.Manager.NewChannel()
	encoder := NewEncoder(clientConn)

	// Notify the session about the new connection
	msg := NewDataMessage(ch.ID, []byte(fmt.Sprintf("CONNECT %s", targetAddr)))
	encoder.Encode(msg)

	// Bidirectional relay
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := clientConn.Read(buf)
			if err != nil {
				ch.Close()
				return
			}
			data := make([]byte, n)
			copy(data, buf[:n])
			if !ch.WriteAsync(data) {
				return
			}
		}
	}()

	go func() {
		for data := range ch.ReadCh() {
			_, err := clientConn.Write(data)
			if err != nil {
				ch.Close()
				return
			}
		}
	}()
}
