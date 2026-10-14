package session

import (
	"errors"
	"net"
	"sync"
)

// Handler processes one accepted connection.
//
// The handler owns its connection and must close it before returning.
// The manager only tracks admission and completion.
type Handler func(net.Conn)

// Manager tracks connections and coordinates their lifecycle.
//
// The mutex protects admission and the active-connection registry.
// The WaitGroup tracks whether all admitted handlers have finished.
type Manager struct {
	mu       sync.Mutex
	draining bool
	active   map[net.Conn]struct{}
	handlers sync.WaitGroup
	handle   Handler
}

// NewManager creates a session manager with an injected handler.
func NewManager(handle Handler) (*Manager, error) {
	if handle == nil {
		return nil, errors.New("session handler is required")
	}

	return &Manager{
		active: make(map[net.Conn]struct{}),
		handle: handle,
	}, nil
}

// Start registers a connection and starts its handler.
//
// The draining check and WaitGroup.Add happen under the same lock.
// This prevents new handlers from being registered after draining starts.
func (m *Manager) Start(conn net.Conn) bool {
	if conn == nil {
		return false
	}

	// 1. Serialize admission with shutdown.
	m.mu.Lock()

	if m.draining {
		m.mu.Unlock()
		_ = conn.Close()
		return false
	}

	if _, exists := m.active[conn]; exists {
		m.mu.Unlock()
		return false
	}

	// 2. Track the connection before launching the goroutine.
	m.active[conn] = struct{}{}
	m.handlers.Add(1)

	m.mu.Unlock()

	// 3. Let the injected handler own this connection.
	go func() {
		defer m.handlers.Done()

		defer func() {
			m.mu.Lock()
			delete(m.active, conn)
			m.mu.Unlock()
		}()

		m.handle(conn)
	}()

	return true
}

// BeginDrain permanently rejects new connections.
//
// It is safe to call multiple times.
func (m *Manager) BeginDrain() {
	m.mu.Lock()
	m.draining = true
	m.mu.Unlock()
}

// Wait blocks until all admitted handlers have returned.
//
// BeginDrain must happen before Wait, ensuring no new
// WaitGroup.Add operations can occur during shutdown.
func (m *Manager) Wait() {
	m.handlers.Wait()
}

// ActiveCount returns the number of registered connections.
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.active)
}

// CloseActive closes the connections currently registered.
//
// Closing a connection unblocks its pending reads and writes.
// It does not itself prove that the handler has finished;
// callers must subsequently use Wait().
func (m *Manager) CloseActive() int {
	// 1. Snapshot the active connections under the lock.
	m.mu.Lock()

	conns := make([]net.Conn, 0, len(m.active))
	for conn := range m.active {
		conns = append(conns, conn)
	}

	m.mu.Unlock()

	// 2. Close outside the lock so handlers can unregister.
	for _, conn := range conns {
		_ = conn.Close()
	}

	return len(conns)
}
