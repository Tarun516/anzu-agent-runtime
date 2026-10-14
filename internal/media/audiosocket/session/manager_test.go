package session

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// TestManagerDrainAndForceClose checks that the reusable manager
// rejects new sessions, closes active connections on request,
// and waits until its handler has actually exited.
func TestManagerDrainAndForceClose(t *testing.T) {
	// 1. Inject a deliberately simple handler.
	// It waits for data or socket closure, then exits.
	manager, err := NewManager(func(conn net.Conn) {
		defer conn.Close()

		var b [1]byte
		_, _ = conn.Read(b[:])
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Admit one existing session.
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	if !manager.Start(server) {
		t.Fatal("expected connection to be admitted")
	}

	if got := manager.ActiveCount(); got != 1 {
		t.Fatalf("active count = %d, want 1", got)
	}

	// 3. Seal admission before waiting for handlers.
	manager.BeginDrain()

	// 4. New sessions must now be rejected and closed.
	rejectedServer, rejectedClient := net.Pipe()
	defer rejectedClient.Close()

	if manager.Start(rejectedServer) {
		t.Fatal("connection admitted after draining began")
	}

	if err := rejectedClient.SetReadDeadline(
		time.Now().Add(2 * time.Second),
	); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}

	var b [1]byte
	_, err = rejectedClient.Read(b[:])
	if !errors.Is(err, io.EOF) {
		t.Fatalf("rejected connection: got %v, want EOF", err)
	}

	// 5. Force-close the original session.
	if closed := manager.CloseActive(); closed != 1 {
		t.Fatalf("closed %d connections, want 1", closed)
	}

	// 6. Wait for the handler to exit, not merely for Close()
	// to have been called.
	done := make(chan struct{})

	go func() {
		manager.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit after forced close")
	}

	// 7. The session registry must be empty after completion.
	if got := manager.ActiveCount(); got != 0 {
		t.Fatalf("active count = %d, want 0", got)
	}
}

// TestManagerWaitsForNaturalCompletion verifies that draining
// does not interrupt an existing session.
//
// The manager must wait until the injected handler returns.
func TestManagerWaitsForNaturalCompletion(t *testing.T) {
	started := make(chan struct{})

	// 1. Create a handler representing an active call.
	manager, err := NewManager(func(conn net.Conn) {
		defer conn.Close()

		close(started)

		// Wait until the simulated caller sends data.
		var buf [1]byte
		_, _ = conn.Read(buf[:])
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Create a simulated connection.
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	if !manager.Start(server) {
		t.Fatal("session was not admitted")
	}

	// 3. Confirm the handler has started.
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	// 4. Stop admission of new sessions.
	manager.BeginDrain()

	// 5. Begin waiting for existing sessions.
	done := make(chan struct{})

	go func() {
		manager.Wait()
		close(done)
	}()

	// 6. Shutdown must not finish while the call is active.
	select {
	case <-done:
		t.Fatal("Wait returned while session was active")
	case <-time.After(20 * time.Millisecond):
		// Expected: handler is still active.
	}

	// 7. Simulate the call finishing naturally.
	if _, err := client.Write([]byte{0x01}); err != nil {
		t.Fatal(err)
	}

	// 8. Wait should now finish.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after handler exited")
	}

	// 9. Verify complete cleanup.
	if got := manager.ActiveCount(); got != 0 {
		t.Fatalf("active count = %d, want 0", got)
	}
}

// TestManagerConcurrentAdmissionAndDrain checks that concurrent
// Start calls cannot bypass the drain admission barrier.
func TestManagerConcurrentAdmissionAndDrain(t *testing.T) {
	manager, err := NewManager(func(conn net.Conn) {
		defer conn.Close()

		// Keep admitted connections active until forced closed.
		var buf [1]byte
		_, _ = conn.Read(buf[:])
	})
	if err != nil {
		t.Fatal(err)
	}

	const attempts = 32

	start := make(chan struct{})
	results := make(chan bool, attempts)
	clients := make([]net.Conn, 0, attempts)

	// 1. Prepare 32 independent simulated connections.
	for i := 0; i < attempts; i++ {
		server, client := net.Pipe()
		clients = append(clients, client)

		go func(conn net.Conn) {
			<-start
			results <- manager.Start(conn)
		}(server)
	}

	// Ensure clients are released even if the test fails.
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()

	// 2. Schedule draining to compete with admission.
	drained := make(chan struct{})

	go func() {
		<-start
		manager.BeginDrain()
		close(drained)
	}()

	// 3. Release all goroutines at approximately the same time.
	close(start)

	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("BeginDrain did not finish")
	}

	// 4. Count successful admissions.
	admitted := 0

	for i := 0; i < attempts; i++ {
		select {
		case ok := <-results:
			if ok {
				admitted++
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a Start call did not complete")
		}
	}

	// 5. Every accepted session must be registered.
	if got := manager.ActiveCount(); got != admitted {
		t.Fatalf(
			"active = %d, admitted = %d",
			got, admitted,
		)
	}

	// 6. Once draining finishes, new admissions must fail.
	lateServer, lateClient := net.Pipe()
	defer lateClient.Close()

	if manager.Start(lateServer) {
		t.Fatal("session admitted after BeginDrain completed")
	}

	// 7. Force-close registered sessions.
	if got := manager.CloseActive(); got != admitted {
		t.Fatalf(
			"closed = %d, admitted = %d",
			got, admitted,
		)
	}

	// 8. Verify that all handlers finish.
	done := make(chan struct{})

	go func() {
		manager.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("session handlers did not finish")
	}

	if got := manager.ActiveCount(); got != 0 {
		t.Fatalf("active sessions = %d, want 0", got)
	}
}
