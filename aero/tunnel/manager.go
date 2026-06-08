package tunnel

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Manager manages tunnel channels and handles message routing
type Manager struct {
	mu        sync.RWMutex
	channels  map[uint64]*Channel
	nextID    uint64
	onChannel func(*Channel)  // Callback when new channel is created
	onClose   func(*Channel) // Callback when channel is closed
}

// NewManager creates a new tunnel manager
func NewManager() *Manager {
	return &Manager{
		channels: make(map[uint64]*Channel),
		nextID:   1, // Start from 1, 0 is reserved for control
	}
}

// SetOnChannel sets the callback for new channel creation
func (m *Manager) SetOnChannel(fn func(*Channel)) {
	m.onChannel = fn
}

// SetOnClose sets the callback for channel close
func (m *Manager) SetOnClose(fn func(*Channel)) {
	m.onClose = fn
}

// NewChannel creates a new channel with a unique ID
func (m *Manager) NewChannel() *Channel {
	m.mu.Lock()
	m.nextID++
	id := m.nextID
	m.mu.Unlock()

	ch := NewChannel(id)

	ch.SetOnClose(func(ch *Channel) {
		m.RemoveChannel(id)
	})

	m.mu.Lock()
	m.channels[id] = ch
	m.mu.Unlock()

	if m.onChannel != nil {
		m.onChannel(ch)
	}

	return ch
}

// GetChannel retrieves a channel by ID
func (m *Manager) GetChannel(id uint64) (*Channel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ch, ok := m.channels[id]
	return ch, ok
}

// RemoveChannel removes a channel from the manager
func (m *Manager) RemoveChannel(id uint64) {
	m.mu.Lock()
	delete(m.channels, id)
	m.mu.Unlock()

	// Notify if callback is set
	if m.onClose != nil {
		if ch, ok := m.GetChannel(id); ok {
			m.onClose(ch)
		}
	}
}

// CloseChannel closes a channel by ID
func (m *Manager) CloseChannel(id uint64) error {
	ch, ok := m.GetChannel(id)
	if !ok {
		return fmt.Errorf("channel %d not found", id)
	}
	return ch.Close()
}

// CloseAll closes all channels
func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ch := range m.channels {
		ch.Close()
	}
	m.channels = make(map[uint64]*Channel)
}

// ChannelCount returns the number of active channels
func (m *Manager) ChannelCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.channels)
}

// ListChannels returns all channel IDs
func (m *Manager) ListChannels() []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]uint64, 0, len(m.channels))
	for id := range m.channels {
		ids = append(ids, id)
	}
	return ids
}

// HandleMessage processes a message and routes it to the appropriate channel
func (m *Manager) HandleMessage(msg *Message) (*Message, error) {
	switch msg.Type {
	case MsgData:
		// Route data to the specified channel
		ch, ok := m.GetChannel(msg.ChannelID)
		if !ok {
			return NewErrorMessage(msg.ChannelID, "channel not found"), nil
		}
		if !ch.WriteAsync(msg.Payload) {
			return NewErrorMessage(msg.ChannelID, "channel write failed"), nil
		}
		return nil, nil // Data messages don't need response

	case MsgHeartbeat:
		// Respond with heartbeat ack
		return NewAckMessage(ChannelIDControl), nil

	case MsgClosePort:
		// Close the specified channel
		if err := m.CloseChannel(msg.ChannelID); err != nil {
			return NewErrorMessage(msg.ChannelID, err.Error()), nil
		}
		return NewAckMessage(msg.ChannelID), nil

	default:
		return NewErrorMessage(msg.ChannelID, "unknown message type"), nil
	}
}

// Session represents a tunnel session (Agent connection)
type Session struct {
	ID        string
	Manager   *Manager
	CreatedAt time.Time
	lastSeen  atomic.Int64
	mu        sync.Mutex
}

// NewSession creates a new tunnel session
func NewSession(id string) *Session {
	return &Session{
		ID:        id,
		Manager:   NewManager(),
		CreatedAt: time.Now(),
	}
}

// Touch updates the last seen time
func (s *Session) Touch() {
	s.lastSeen.Store(time.Now().Unix())
}

// IsAlive checks if the session is still alive
func (s *Session) IsAlive(timeout time.Duration) bool {
	last := time.Unix(s.lastSeen.Load(), 0)
	return time.Since(last) < timeout
}

// SessionManager manages multiple tunnel sessions
type SessionManager struct {
	mu        sync.RWMutex
	sessions  map[string]*Session
	cleanupInterval time.Duration
	timeout   time.Duration
	stopCh    chan struct{}
}

// NewSessionManager creates a new session manager
func NewSessionManager(cleanupInterval, timeout time.Duration) *SessionManager {
	return &SessionManager{
		sessions:        make(map[string]*Session),
		cleanupInterval: cleanupInterval,
		timeout:         timeout,
		stopCh:          make(chan struct{}),
	}
}

// Register registers a new session
func (sm *SessionManager) Register(session *Session) {
	sm.mu.Lock()
	sm.sessions[session.ID] = session
	sm.mu.Unlock()
	log.Printf("[Tunnel] Session registered: %s", session.ID)
}

// Unregister removes a session
func (sm *SessionManager) Unregister(id string) {
	sm.mu.Lock()
	delete(sm.sessions, id)
	sm.mu.Unlock()
	log.Printf("[Tunnel] Session unregistered: %s", id)
}

// Get retrieves a session by ID
func (sm *SessionManager) Get(id string) (*Session, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s, ok := sm.sessions[id]
	return s, ok
}

// Cleanup starts the background cleanup goroutine
func (sm *SessionManager) Cleanup() {
	go func() {
		ticker := time.NewTicker(sm.cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				sm.cleanExpired()
			case <-sm.stopCh:
				return
			}
		}
	}()
}

// Stop stops the session manager
func (sm *SessionManager) Stop() {
	close(sm.stopCh)
}

func (sm *SessionManager) cleanExpired() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for id, s := range sm.sessions {
		if !s.IsAlive(sm.timeout) {
			delete(sm.sessions, id)
			s.Manager.CloseAll()
			log.Printf("[Tunnel] Session expired and cleaned: %s", id)
		}
	}
}

// SessionCount returns the number of active sessions
func (sm *SessionManager) SessionCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.sessions)
}
