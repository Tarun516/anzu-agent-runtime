package main

import (
	"net"
	"sync"
)

// sessionManager tracks active AudioSocket connections during this
// process's lifetime 
//
// The manager owns admission and handler tracking
// Each handleConnection invocation owns and closes its connection
// The registry is in memory and does not persist call history.


type sessionManager struct {
	mu sync.Mutex
	draining bool
	active map[net.Conn]struct{}
	handlers sync.WaitGroup
}

// newSessionManager creates an empty registry
func newSessionManager() *sessionManager{
	return &sessionManager{
		active:make(map[net.Conn]struct{}),
	}
}

// Start admits one connection and starts its session handler.
//
// It returns false if draining has started or this connection is
// already registered. A connection rejected during draining is closed.

func(m *sessionManager) Start(conn net.Conn) bool {
	// 1. Searilalize admission with shutdown.
	m.mu.Lock()

	if m.draining {
		m.mu.Unlock()
		_= conn.Close()
		return false
	}

	if _, exists := m.active[conn]; exists{
		m.mu.Unlock()
		return  false
	}

	// 2. Register the connection before starting its goroutine.
	// Add and the draining check use the same lock.

	m.active[conn] = struct{}{}
	m.handlers.Add(1)

	m.mu.Unlock()

	// 3. Run the existing handler and remove it on completion
	go func(){
		defer m.handlers.Done()

		defer func(){
			m.mu.Lock()
			delete(m.active, conn)
			m.mu.Unlock()
		}()
		handleConnection(conn)
	}()

	return true
}

// BigEndian until all previously admitted handlers have excited.
//
// BeginDrain must be called before wait so no new handlers can
// be registered while shutdown waits for completion.

func(m *sessionManager)Wait(){
	m.handlers.Wait()

}

// ActiveCount reports the number of currently tracked connections.
//
// The count is a local snapshot, not a durable or global call count.

func(m *sessionManager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.active)
}