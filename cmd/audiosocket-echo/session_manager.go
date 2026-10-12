package main

import (
	"net"
	"sync"
)

// sessionManager tracks active AudioSocket connections during this
// process's lifetime.
//
// The manager owns admission and handler tracking.
// Each handleConnection invocation owns and closes its connection.
// The registry is in memory and does not persist call history.
type sessionManager struct {
	mu       sync.Mutex
	draining bool
	active   map[net.Conn]struct{}
	handlers sync.WaitGroup
}

// newSessionManager creates an empty registry for active call handlers.
func newSessionManager() *sessionManager {
	return &sessionManager{
		active: make(map[net.Conn]struct{}),
	}
}

// Start admits one connection and starts its session handler.
//
// It returns false if draining has started or this connection is
// already registered. A connection rejected during draining is closed.
func (m *sessionManager) Start(conn net.Conn) bool {
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

	// 2. Register the connection before starting its goroutine.
	// Add and the draining check use the same lock.
	m.active[conn] = struct{}{}
	m.handlers.Add(1)

	m.mu.Unlock()

	// 3. Run the existing handler and remove it on completion.
	go func() {
		defer m.handlers.Done()

		defer func() {
			m.mu.Lock()
			delete(m.active, conn)
			m.mu.Unlock()
		}()

		handleConnection(conn)
	}()

	return true
}

// BeginDrain permanently disables new session admission.
//
// Calling it more than once is safe. Existing sessions continue.
func (m *sessionManager) BeginDrain() {
	// 1. Change admission state under the same lock used by Start.
	m.mu.Lock()
	m.draining = true
	m.mu.Unlock()
}

// Wait blocks until all previously admitted handlers have exited.
//
// BeginDrain must be called before Wait so no new handlers can
// be registered while shutdown waits for completion.
func (m *sessionManager) Wait() {
	m.handlers.Wait()
}

// ActiveCount reports the number of currently tracked connections.
//
// The count is a local snapshot, not a durable or global call count.
func (m *sessionManager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.active)
}

// CloseActive closes the currently registered call connections.
//
// This is intended for forced shutdown after BeginDrain.
// Closing a net.Conn unblocks pending Read and Write operations,
// allowing each handler to run its deferred cleanup.
//
// The manager never reads or writes media through these connections.
func (m *sessionManager) CloseActive() int {
	// 1. Take a snapshot of registered connections.
	//
	// Holding the mutex prevents concurrent map iteration and
	// modification by session handler goroutines.
	m.mu.Lock()

	conns := make([]net.Conn, 0, len(m.active))

	for conn := range m.active {
		conns = append(conns, conn)
	}

	m.mu.Unlock()

	// 2. Close connections outside the mutex.
	//
	// This avoids holding the registry lock while performing
	// socket operations. Handlers must be free to unregister.
	for _, conn := range conns {
		_ = conn.Close()
	}

	// 3. Report how many connections were targeted.
	//
	// This is not a count of confirmed handler completions.
	return len(conns)
}
