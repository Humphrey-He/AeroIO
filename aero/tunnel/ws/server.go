// Package ws provides WebSocket transport support for the tunnel.
package ws

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AeroIO/aero/tunnel"
)

// Config holds the configuration for WebSocket tunnel server
type Config struct {
	ListenAddr string
	PortRange  [2]uint16
	Timeout    time.Duration
}

// Server represents a WebSocket tunnel server
type Server struct {
	config   Config
	listener net.Listener
	portMap  map[uint16]*PortBinding
	nextPort uint64
	mu       sync.RWMutex
	running  atomic.Bool
	stopCh   chan struct{}
	onPortOpen  func(*PortBinding)
	onPortClose func(*PortBinding)
}

// PortBinding represents a port binding
type PortBinding struct {
	Port       uint16
	TargetAddr string
	SessionID  string
	ChannelID  uint64
	CreatedAt  time.Time
}

// Session represents a WebSocket tunnel session
type Session struct {
	ID       string
	LastSeen time.Time
	Channels *tunnel.Manager
}

// NewServer creates a new WebSocket tunnel server
func NewServer(config Config) *Server {
	if config.Timeout == 0 {
		config.Timeout = 60 * time.Second
	}
	return &Server{
		config:  config,
		portMap: make(map[uint16]*PortBinding),
		stopCh:  make(chan struct{}),
		nextPort: uint64(config.PortRange[0]),
	}
}

// SetOnPortOpen sets the callback for port open events
func (s *Server) SetOnPortOpen(fn func(*PortBinding)) { s.onPortOpen = fn }

// SetOnPortClose sets the callback for port close events
func (s *Server) SetOnPortClose(fn func(*PortBinding)) { s.onPortClose = fn }

// ListenAndServe starts the WebSocket server
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = ln
	s.running.Store(true)

	log.Printf("[Tunnel/WS] WebSocket server listening on %s", s.config.ListenAddr)

	for s.running.Load() {
		conn, err := ln.Accept()
		if err != nil {
			if !s.running.Load() {
				return nil
			}
			log.Printf("[Tunnel/WS] Accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
	return nil
}

// Stop stops the WebSocket server
func (s *Server) Stop() {
	s.running.Store(false)
	if s.listener != nil {
		s.listener.Close()
	}
	log.Printf("[Tunnel/WS] WebSocket server stopped")
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		return
	}

	if !isWebSocketUpgrade(req) {
		resp := "HTTP/1.1 400 Bad Request\r\n\r\n"
		conn.Write([]byte(resp))
		return
	}

	key := req.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return
	}

	if err := performHandshake(conn, key); err != nil {
		return
	}

	s.handleWSSession(conn)
}

func isWebSocketUpgrade(req *http.Request) bool {
	return strings.EqualFold(req.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(req.Header.Get("Connection")), "upgrade") &&
		req.Header.Get("Sec-WebSocket-Key") != ""
}

func performHandshake(conn net.Conn, key string) error {
	acceptKey := computeAcceptKey(key)
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		fmt.Sprintf("Sec-WebSocket-Accept: %s\r\n", acceptKey) +
		"\r\n"
	_, err := conn.Write([]byte(response))
	return err
}

func computeAcceptKey(key string) string {
	const guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	h := sha1.New()
	h.Write([]byte(key + guid))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func (s *Server) handleWSSession(conn net.Conn) {
	conn.SetReadDeadline(time.Time{})
	conn.SetWriteDeadline(time.Time{})

	var session *Session
	stopCh := make(chan struct{})

	go func() {
		for {
			frame, err := readWSFrame(conn)
			if err != nil {
				close(stopCh)
				return
			}

			switch frame.Opcode {
			case 0x2: // Binary
				msgDecoder := tunnel.NewDecoder(&frameReader{data: frame.Payload})
				msg, err := msgDecoder.Decode()
				if err != nil {
					continue
				}

				if msg.Type == tunnel.MsgRegister {
					sessionID := string(msg.Payload)
					session = &Session{
						ID:       sessionID,
						LastSeen: time.Now(),
						Channels: tunnel.NewManager(),
					}
					log.Printf("[Tunnel/WS] Client connected: %s", sessionID)
				} else if session != nil {
					session.LastSeen = time.Now()
					s.handleWSMessage(session, msg, conn)
				}

			case 0x8: // Close
				close(stopCh)
				return
			case 0x9: // Ping
				writeWSFrame(conn, 0xA, frame.Payload)
			}
		}
	}()

	<-stopCh
	if session != nil {
		log.Printf("[Tunnel/WS] Client disconnected: %s", session.ID)
	}
	conn.Close()
}

// Frame represents a WebSocket frame
type Frame struct {
	Opcode  byte
	Payload []byte
	Fin     bool
}

func readWSFrame(r io.Reader) (*Frame, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	frame := &Frame{
		Fin:    (header[0] & 0x80) != 0,
		Opcode: header[0] & 0x0F,
	}

	length := uint64(header[1] & 0x7F)
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

	if length > 0 {
		if length > 1<<20 {
			return nil, fmt.Errorf("frame too large")
		}
		frame.Payload = make([]byte, length)
		if _, err := io.ReadFull(r, frame.Payload); err != nil {
			return nil, err
		}
	}
	return frame, nil
}

func writeWSFrame(w io.Writer, opcode byte, payload []byte) error {
	b0 := byte(0x80) | opcode
	payloadLen := len(payload)
	var header []byte

	if payloadLen < 126 {
		header = make([]byte, 2)
		header[0] = b0
		header[1] = byte(payloadLen)
	} else if payloadLen < 65536 {
		header = make([]byte, 4)
		header[0] = b0
		header[1] = 126
		binary.BigEndian.PutUint16(header[2:4], uint16(payloadLen))
	} else {
		header = make([]byte, 10)
		header[0] = b0
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:10], uint64(payloadLen))
	}

	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return nil
}

type frameReader struct{ data []byte; pos int }

func (r *frameReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (s *Server) handleWSMessage(session *Session, msg *tunnel.Message, conn net.Conn) {
	switch msg.Type {
	case tunnel.MsgOpenPort:
		if len(msg.Payload) < 4 {
			s.writeMessage(conn, tunnel.NewErrorMessage(0, "invalid payload"))
			return
		}

		targetLen := binary.BigEndian.Uint16(msg.Payload[2:4])
		if len(msg.Payload) < 4+int(targetLen) {
			s.writeMessage(conn, tunnel.NewErrorMessage(0, "invalid payload"))
			return
		}

		targetAddr := string(msg.Payload[4 : 4+targetLen])
		var publicPort uint16

		if binary.BigEndian.Uint16(msg.Payload[0:2]) > 0 {
			publicPort = binary.BigEndian.Uint16(msg.Payload[0:2])
		} else {
			publicPort = s.allocatePort()
		}

		ch := session.Channels.NewChannel()

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

		port := publicPort
		ch.SetOnClose(func(c *tunnel.Channel) {
			s.mu.Lock()
			delete(s.portMap, port)
			s.mu.Unlock()
			if s.onPortClose != nil {
				s.onPortClose(binding)
			}
		})

		go s.relayWSData(ch, conn)

		if s.onPortOpen != nil {
			s.onPortOpen(binding)
		}

		log.Printf("[Tunnel/WS] Port opened: :%d -> %s (channel: %d)", publicPort, targetAddr, ch.ID)
		s.writeMessage(conn, tunnel.NewAckMessage(ch.ID))

	case tunnel.MsgHeartbeat:
		s.writeMessage(conn, tunnel.NewAckMessage(tunnel.ChannelIDControl))

	case tunnel.MsgData:
		if ch, ok := session.Channels.GetChannel(msg.ChannelID); ok {
			ch.WriteAsync(msg.Payload)
		}

	case tunnel.MsgClosePort:
		session.Channels.CloseChannel(msg.ChannelID)
		s.writeMessage(conn, tunnel.NewAckMessage(msg.ChannelID))
	}
}

func (s *Server) writeMessage(conn net.Conn, msg *tunnel.Message) {
	var buf bytes.Buffer
	enc := tunnel.NewEncoder(&buf)
	enc.Encode(msg)
	writeWSFrame(conn, 0x2, buf.Bytes())
}

func (s *Server) relayWSData(ch *tunnel.Channel, conn net.Conn) {
	for {
		select {
		case data := <-ch.ReadCh():
			if data == nil {
				return
			}
			msg := tunnel.NewDataMessage(ch.ID, data)
			var buf bytes.Buffer
			enc := tunnel.NewEncoder(&buf)
			if err := enc.Encode(msg); err != nil {
				ch.Close()
				return
			}
			if err := writeWSFrame(conn, 0x2, buf.Bytes()); err != nil {
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
	return 0
}
