package main

import (
	"net"
	"testing"
	"time"
)

// TestDrainSessionsForcesCleanupAfterGrace verifies that shutdown
// closes a connection when its session does not finish naturally.
//
// The test owns the client endpoint. The session handler owns
// the server endpoint, and the manager coordinates termination.
func TestDrainSessionsForcesCleanupAfterGrace(t *testing.T) {
	// 1. Start one session that will remain connected.
	manager := newSessionManager()

	server, client := net.Pipe()

	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	if !manager.Start(server) {
		t.Fatal("session was not admitted")
	}

	sendSessionUUID(t, client)

	// 2. Begin shutdown with a short test-only grace period.
	//
	// We deliberately do not send an AudioSocket hangup.
	const grace = 50 * time.Millisecond
	const cleanup = time.Second

	start := time.Now()

	err := drainSessions(manager, grace, cleanup)
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// 3. Verify the session was not closed before grace elapsed.
	if elapsed := time.Since(start); elapsed < grace {
		t.Fatalf(
			"shutdown finished before grace period: %v",
			elapsed,
		)
	}

	// 4. Verify forced connection closure released the handler.
	if got := manager.ActiveCount(); got != 0 {
		t.Fatalf("active sessions = %d, want 0", got)
	}
}

// TestDrainSessionsCompletesBeforeGrace verifies that an active
// call can finish normally without waiting for the full grace period.
//
// The test owns the client endpoint. The session handler owns the
// server endpoint and exits after receiving an AudioSocket hangup.
func TestDrainSessionsCompletesBeforeGrace(t *testing.T) {
	// 1. Start an active session.
	manager := newSessionManager()

	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()

	if !manager.Start(server) {
		t.Fatal("session was not admitted")
	}

	sendSessionUUID(t, client)

	// 2. Begin draining with a two-second grace period.
	done := make(chan error, 1)

	go func() {
		done <- drainSessions(
			manager,
			2*time.Second,
			time.Second,
		)
	}()

	// 3. Ensure shutdown does not finish while the call is active.
	select {
	case err := <-done:
		t.Fatalf("shutdown returned with active call: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	// 4. Send an AudioSocket hangup frame.
	//
	// Type 0x00, followed by a two-byte zero payload length.
	if _, err := client.Write([]byte{0x00, 0x00, 0x00}); err != nil {
		t.Fatalf("send hangup: %v", err)
	}

	// 5. Shutdown should finish before the grace period expires.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("graceful shutdown failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish promptly after hangup")
	}

	// 6. Verify that all sessions have been unregistered.
	if got := manager.ActiveCount(); got != 0 {
		t.Fatalf("active sessions = %d, want 0", got)
	}
}

// ignoreCloseConn simulates a connection that does not respond
// to a shutdown close request.
//
// It is deliberately broken and must only be used in tests.
// The embedded connection's real Close method remains available
// for test cleanup.
type ignoreCloseConn struct {
	net.Conn
}

func (c *ignoreCloseConn) Close() error {
	return nil
}

// TestDrainSessionsReportsCleanupTimeout verifies that shutdown
// reports an error when a handler cannot terminate after the
// forced-close attempt.
//
// The test owns the real pipe endpoints and releases them explicitly
// so the simulated failure does not leave a goroutine behind.
func TestDrainSessionsReportsCleanupTimeout(t *testing.T) {
	// 1. Create a connection whose Close method does nothing.
	manager := newSessionManager()

	server, client := net.Pipe()
	stubborn := &ignoreCloseConn{Conn: server}

	// 2. Always release the real connection after the test.
	//
	// The wrapper ignores Close, but closing the underlying
	// pipe directly still interrupts the blocked handler.
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()

		finished := make(chan struct{})
		go func() {
			manager.Wait()
			close(finished)
		}()

		select {
		case <-finished:
		case <-time.After(sessionTestTimeout):
			t.Error("handler did not exit during test cleanup")
		}
	})

	if !manager.Start(stubborn) {
		t.Fatal("session was not admitted")
	}

	sendSessionUUID(t, client)

	// 3. Use short deadlines to exercise both timeout stages.
	const grace = 30 * time.Millisecond
	const cleanup = 30 * time.Millisecond

	err := drainSessions(manager, grace, cleanup)

	// 4. The handler cannot stop because Close did nothing.
	// The coordinator must therefore report failure.
	if err == nil {
		t.Fatal("expected cleanup timeout error, got nil")
	}

	// 5. Verify the handler is still tracked.
	//
	// An unsuccessful Close must not be mistaken for
	// successful session termination.
	if got := manager.ActiveCount(); got != 1 {
		t.Fatalf("active sessions = %d, want 1", got)
	}

	t.Logf("expected shutdown error: %v", err)
}
