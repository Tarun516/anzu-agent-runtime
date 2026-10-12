package main

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket"
)

// TestSessionManagerDrainWaitsForActiveCalls verifies that draining
// rejects new connections while allowing existing calls to finish.
//
// The test owns the client endpoints. Session handlers own the
// server endpoints and must exit before Wait returns.
func TestSessionManagerDrainWaitsForActiveCalls(t *testing.T) {
	// 1. Create two independent call connections.
	manager := newSessionManager()

	serverA, clientA := net.Pipe()
	serverB, clientB := net.Pipe()

	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
		_ = serverA.Close()
		_ = serverB.Close()
	})

	if !manager.Start(serverA) {
		t.Fatal("Call A was not admitted")
	}

	if !manager.Start(serverB) {
		t.Fatal("Call B was not admitted")
	}

	// 2. Initialize both calls with distinct UUIDs.
	sendSessionFrame(t, clientA, audiosocket.Frame{
		Type:    audiosocket.TypeUUID,
		Payload: bytes.Repeat([]byte{0xA1}, 16),
	})

	sendSessionFrame(t, clientB, audiosocket.Frame{
		Type:    audiosocket.TypeUUID,
		Payload: bytes.Repeat([]byte{0xB2}, 16),
	})

	if got := manager.ActiveCount(); got != 2 {
		t.Fatalf("active connections = %d, want 2", got)
	}

	// 3. Start draining and reject any subsequent connection.
	manager.BeginDrain()

	serverC, clientC := net.Pipe()
	defer clientC.Close()

	if manager.Start(serverC) {
		t.Fatal("new connection admitted during draining")
	}

	// Confirm that the rejected connection was closed.
	if err := clientC.SetReadDeadline(
		time.Now().Add(sessionTestTimeout),
	); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("set rejected client deadline: %v", err)
	}

	var buf [1]byte
	if _, err := clientC.Read(buf[:]); err != io.EOF {
		t.Fatalf("rejected client read = %v, want EOF", err)
	}

	// 4. Begin waiting while both calls are still active.
	waitDone := make(chan struct{})

	go func() {
		manager.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("shutdown completed with active calls")
	default:
	}

	// 5. End Call A, but leave Call B running.
	sendSessionFrame(t, clientA, audiosocket.Frame{
		Type: audiosocket.TypeHangup,
	})

	// Wait until Call A has actually been removed.
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(sessionTestTimeout)

	for manager.ActiveCount() != 1 {
		select {
		case <-ticker.C:
		case <-timeout:
			t.Fatal("Call A was not removed")
		}
	}

	select {
	case <-waitDone:
		t.Fatal("shutdown completed while Call B was active")
	default:
	}

	// 6. End Call B. Shutdown should now complete.
	sendSessionFrame(t, clientB, audiosocket.Frame{
		Type: audiosocket.TypeHangup,
	})

	select {
	case <-waitDone:
	case <-time.After(sessionTestTimeout):
		t.Fatal("shutdown did not finish after both calls ended")
	}

	// 7. Verify the registry has no remaining connections.
	if got := manager.ActiveCount(); got != 0 {
		t.Fatalf("active connections = %d, want 0", got)
	}
}

// TestSessionManagerCloseActiveUnblocksCall verifies that forced
// closure releases a session blocked waiting for additional audio.
//
// The test owns the client endpoint. The session handler owns
// the server endpoint, while the manager coordinates shutdown.
func TestSessionManagerCloseActiveUnblocksCall(t *testing.T) {
	// 1. Create a manager and a simulated telephone connection.
	manager := newSessionManager()

	server, client := net.Pipe()

	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	// 2. Register the session and complete its UUID handshake.
	if !manager.Start(server) {
		t.Fatal("session was not admitted")
	}

	sendSessionUUID(t, client)

	if got := manager.ActiveCount(); got != 1 {
		t.Fatalf("active sessions = %d, want 1", got)
	}

	// 3. Begin draining while the call remains active.
	manager.BeginDrain()

	waitDone := make(chan struct{})

	go func() {
		manager.Wait()
		close(waitDone)
	}()

	// The call has not sent a hangup frame.
	// Shutdown should still be waiting.
	select {
	case <-waitDone:
		t.Fatal("shutdown completed with an active call")
	default:
	}

	// 4. Force-close the registered connection.
	closed := manager.CloseActive()

	if closed != 1 {
		t.Fatalf("closed %d connections, want 1", closed)
	}

	// 5. Verify the blocked handler exits after socket closure.
	select {
	case <-waitDone:
	case <-time.After(sessionTestTimeout):
		t.Fatal("handler did not exit after forced closure")
	}

	// 6. Verify there are no registered sessions remaining.
	if got := manager.ActiveCount(); got != 0 {
		t.Fatalf("active sessions = %d, want 0", got)
	}
}
