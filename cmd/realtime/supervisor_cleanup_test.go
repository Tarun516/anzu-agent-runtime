package main

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSupervisorReportsCleanupTimeout verifies that the
// supervisor returns an error when a session handler
// refuses to terminate even after its socket is closed.
//
// A connection closure can interrupt socket I/O,
// but it cannot terminate arbitrary Go code.
func TestSupervisorReportsCleanupTimeout(t *testing.T) {
	// 1. Coordinate the test handler's lifetime.
	started := make(chan struct{})
	release := make(chan struct{})

	var releaseOnce sync.Once

	releaseHandler := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}

	// Ensure the handler is eventually released,
	// even if an assertion fails.
	defer releaseHandler()

	// 2. Create a deliberately uncooperative handler.
	//
	// It ignores socket closure and waits until
	// the test explicitly allows it to return.
	handler := func(conn net.Conn) {
		defer conn.Close()

		close(started)
		<-release
	}

	// 3. Start the real supervisor with a short grace.
	//
	// Our existing helper uses a 1-second
	// post-grace cleanup timeout.
	runtime := startDrainTestRuntime(
		t,
		100*time.Millisecond,
		handler,
	)

	// 4. Connect a real TCP client.
	conn, err := net.DialTimeout(
		"tcp",
		runtime.audioAddr,
		drainTestTimeout,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 5. Prove the handler is registered before shutdown.
	select {
	case <-started:
	case <-time.After(drainTestTimeout):
		t.Fatal("handler did not start")
	}

	if got := runtime.manager.ActiveCount(); got != 1 {
		t.Fatalf("active sessions = %d, want 1", got)
	}

	// 6. Request application shutdown.
	runtime.cancel()

	// 7. The supervisor must eventually return an error,
	// rather than wait forever or claim success.
	var shutdownErr error

	select {
	case shutdownErr = <-runtime.result:
	case <-time.After(drainTestTimeout):
		t.Fatal("supervisor did not report cleanup timeout")
	}

	if shutdownErr == nil {
		t.Fatal("expected cleanup timeout error, got nil")
	}

	// 8. Verify the error identifies the failed operation.
	if !strings.Contains(
		shutdownErr.Error(),
		"cleanup timed out",
	) {
		t.Fatalf(
			"expected cleanup timeout, got: %v",
			shutdownErr,
		)
	}

	// 9. The handler must still be registered.
	//
	// Force-closing a socket does NOT prove that
	// the goroutine using it has terminated.
	if got := runtime.manager.ActiveCount(); got != 1 {
		t.Fatalf(
			"active sessions = %d, want 1",
			got,
		)
	}

	// 10. The supervisor should still release
	// both listening sockets despite the error.
	assertDrainPortsReleased(t, runtime)

	// 11. Let the deliberately stuck handler exit.
	releaseHandler()

	// 12. Ensure test cleanup leaves no handler running.
	waitDone := make(chan struct{})

	go func() {
		runtime.manager.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(drainTestTimeout):
		t.Fatal("handler did not finish after release")
	}

	if got := runtime.manager.ActiveCount(); got != 0 {
		t.Fatalf(
			"remaining sessions = %d",
			got,
		)
	}
}
