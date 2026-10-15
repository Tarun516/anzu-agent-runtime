package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/config"
	"github.com/Tarun516/anzu-agent-runtime/internal/health"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/session"
)

const drainTestTimeout = 5 * time.Second

// drainTestRuntime holds the components created for one
// realtime-supervisor shutdown integration test.
type drainTestRuntime struct {
	listeners *boundListeners
	manager   *session.Manager

	httpAddr  string
	audioAddr string

	cancel   context.CancelFunc
	result   chan error
	finished chan struct{}
}

// startDrainTestRuntime starts the real supervisor with
// two loopback TCP listeners and an injected test handler.
//
// PostgreSQL and Asterisk are intentionally excluded:
// this test focuses on connection ownership and shutdown.
func startDrainTestRuntime(
	t *testing.T,
	gracePeriod time.Duration,
	handler session.Handler,
) *drainTestRuntime {
	t.Helper()

	// 1. Enable media with a configurable grace period.
	audioCfg := config.AudioSocket{
		Enabled:        true,
		ListenAddress:  "127.0.0.1:0",
		GracePeriod:    gracePeriod,
		CleanupTimeout: time.Second,
	}

	// 2. Bind both listening sockets.
	listeners, err := bindListeners("127.0.0.1:0", audioCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listeners.Close() })

	// 3. Construct the session manager with our test handler.
	manager, err := session.NewManager(handler)
	if err != nil {
		t.Fatal(err)
	}

	httpAddr := listeners.http.Addr().String()

	logger := slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)

	server := health.NewServer(
		"realtime-drain-test",
		httpAddr,
		time.Second,
		logger,
		nil,
	)

	// 4. Create a cancellable root context.
	ctx, cancel := context.WithCancel(context.Background())

	runtime := &drainTestRuntime{
		listeners: listeners,
		manager:   manager,
		httpAddr:  httpAddr,
		audioAddr: listeners.audio.Addr().String(),
		cancel:    cancel,
		result:    make(chan error, 1),
		finished:  make(chan struct{}),
	}

	// 5. Start the supervisor.
	go func() {
		defer close(runtime.finished)

		runtime.result <- superviseListeners(
			ctx,
			server,
			listeners,
			manager,
			audioCfg,
		)
	}()

	// 6. Provide emergency cleanup if an assertion fails.
	t.Cleanup(func() {
		cancel()
		manager.BeginDrain()
		manager.CloseActive()
		_ = listeners.Close()

		select {
		case <-runtime.finished:
		case <-time.After(drainTestTimeout):
			t.Error("supervisor did not finish during cleanup")
		}
	})

	// 7. Wait for actual HTTP readiness.
	client := &http.Client{
		Timeout: 200 * time.Millisecond,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	t.Cleanup(client.CloseIdleConnections)

	deadline := time.Now().Add(drainTestTimeout)

	for {
		resp, requestErr := client.Get(
			"http://" + httpAddr + "/readyz",
		)

		if requestErr == nil {
			_ = resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				break
			}
		}

		select {
		case err := <-runtime.result:
			t.Fatalf("supervisor exited before readiness: %v", err)
		default:
		}

		if time.Now().After(deadline) {
			t.Fatal("supervisor never became ready")
		}

		time.Sleep(10 * time.Millisecond)
	}

	return runtime
}

// assertDrainPortsReleased verifies that both listeners
// have relinquished their OS listening sockets.
func assertDrainPortsReleased(
	t *testing.T,
	runtime *drainTestRuntime,
) {
	t.Helper()

	for _, address := range []string{
		runtime.httpAddr,
		runtime.audioAddr,
	} {
		probe, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("listener leaked at %s: %v", address, err)
		}
		_ = probe.Close()
	}
}

// TestSupervisorDrainsNaturalCompletion proves that
// an admitted call can continue using its connection
// after application shutdown begins.
func TestSupervisorDrainsNaturalCompletion(t *testing.T) {
	started := make(chan struct{})

	// 1. Handler reads one byte and sends it back.
	handler := func(conn net.Conn) {
		defer conn.Close()

		close(started)

		var data [1]byte
		if _, err := io.ReadFull(conn, data[:]); err != nil {
			return
		}

		_, _ = conn.Write(data[:])
	}

	// A generous grace period avoids timing flakiness.
	runtime := startDrainTestRuntime(
		t,
		2*time.Second,
		handler,
	)

	// 2. Establish a real AudioSocket TCP connection.
	conn, err := net.DialTimeout(
		"tcp",
		runtime.audioAddr,
		drainTestTimeout,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(
		time.Now().Add(drainTestTimeout),
	); err != nil {
		t.Fatal(err)
	}

	// 3. Wait until the manager has admitted the call.
	select {
	case <-started:
	case <-time.After(drainTestTimeout):
		t.Fatal("handler did not start")
	}

	if got := runtime.manager.ActiveCount(); got != 1 {
		t.Fatalf("active sessions = %d, want 1", got)
	}

	// 4. Begin application shutdown while call is active.
	runtime.cancel()

	// The supervisor must not exit while a call is
	// still active and within its grace period.
	select {
	case err := <-runtime.result:
		t.Fatalf("supervisor exited too early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// 5. Exchange data AFTER shutdown was requested.
	// This proves an admitted socket is still usable.
	if _, err := conn.Write([]byte{0x42}); err != nil {
		t.Fatalf("write during drain: %v", err)
	}

	var echoed [1]byte
	if _, err := io.ReadFull(conn, echoed[:]); err != nil {
		t.Fatalf("read during drain: %v", err)
	}

	if echoed[0] != 0x42 {
		t.Fatalf("echo = 0x%02x, want 0x42", echoed[0])
	}

	// 6. Handler finishes; supervisor should now exit.
	select {
	case err := <-runtime.result:
		if err != nil {
			t.Fatalf("graceful shutdown: %v", err)
		}
	case <-time.After(drainTestTimeout):
		t.Fatal("supervisor did not finish")
	}

	if got := runtime.manager.ActiveCount(); got != 0 {
		t.Fatalf("remaining sessions = %d", got)
	}

	assertDrainPortsReleased(t, runtime)
}

// TestSupervisorForcesStuckSession verifies that a call
// blocked in socket I/O cannot prevent shutdown forever.
func TestSupervisorForcesStuckSession(t *testing.T) {
	started := make(chan struct{})
	readFinished := make(chan error, 1)

	// 1. The handler deliberately waits for incoming data.
	// The test client never sends any data.
	handler := func(conn net.Conn) {
		defer conn.Close()

		close(started)

		var data [1]byte
		_, err := conn.Read(data[:])

		readFinished <- err
	}

	const gracePeriod = 200 * time.Millisecond

	runtime := startDrainTestRuntime(
		t,
		gracePeriod,
		handler,
	)

	// 2. Connect and keep the connection open.
	conn, err := net.DialTimeout(
		"tcp",
		runtime.audioAddr,
		drainTestTimeout,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	select {
	case <-started:
	case <-time.After(drainTestTimeout):
		t.Fatal("handler did not start")
	}

	if got := runtime.manager.ActiveCount(); got != 1 {
		t.Fatalf("active sessions = %d, want 1", got)
	}

	// 3. Request shutdown while Read is blocked.
	shutdownStarted := time.Now()
	runtime.cancel()

	// 4. After grace expires, Drain must force-close
	// the connection and finish successfully.
	select {
	case err := <-runtime.result:
		if err != nil {
			t.Fatalf("forced shutdown failed: %v", err)
		}
	case <-time.After(drainTestTimeout):
		t.Fatal("supervisor did not finish bounded shutdown")
	}

	// A very early exit would indicate that we did not
	// actually wait through the configured grace period.
	if elapsed := time.Since(shutdownStarted); elapsed < gracePeriod/2 {
		t.Fatalf("shutdown ended before grace: %v", elapsed)
	}

	// 5. The blocked socket Read must have returned.
	select {
	case err := <-readFinished:
		if err == nil {
			t.Fatal("expected socket read to be interrupted")
		}
	case <-time.After(drainTestTimeout):
		t.Fatal("blocked handler never exited")
	}

	// 6. No sessions should remain registered.
	if got := runtime.manager.ActiveCount(); got != 0 {
		t.Fatalf("remaining sessions = %d", got)
	}

	// 7. Both listening ports must be released.
	assertDrainPortsReleased(t, runtime)
}
