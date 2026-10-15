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

// TestSupervisorHTTPOnlyLifecycle verifies that the supervisor
// can serve HTTP, report readiness, shut down on cancellation,
// and release the listening port.
func TestSupervisorHTTPOnlyLifecycle(t *testing.T) {
	// 1. Disable AudioSocket for this test.
	audioCfg := config.AudioSocket{}

	// 2. Bind an available loopback HTTP port.
	listeners, err := bindListeners(
		"127.0.0.1:0",
		audioCfg,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer listeners.Close()

	httpAddr := listeners.http.Addr().String()

	// 3. Create the required session manager.
	manager, err := session.NewManager(
		func(conn net.Conn) {
			_ = conn.Close()
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	// 4. Create a health server without external dependencies.
	// PostgreSQL is intentionally excluded from this test.
	logger := slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)

	server := health.NewServer(
		"realtime-test",
		httpAddr,
		time.Second,
		logger,
		nil,
	)

	// 5. Create a cancellable application context.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 6. Start the supervisor independently.
	// The buffered channel receives its final result.
	done := make(chan error, 1)

	go func() {
		done <- superviseListeners(
			ctx,
			server,
			listeners,
			manager,
			audioCfg,
		)
	}()

	// 7. Poll readiness until HTTP is serving.
	client := &http.Client{
		Timeout: 200 * time.Millisecond,
	}
	defer client.CloseIdleConnections()

	deadline := time.Now().Add(3 * time.Second)

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

		// Fail early if the supervisor already terminated.
		select {
		case err := <-done:
			t.Fatalf("supervisor exited early: %v", err)
		default:
		}

		if time.Now().After(deadline) {
			t.Fatal("HTTP server never became ready")
		}

		time.Sleep(10 * time.Millisecond)
	}

	// 8. Simulate an application shutdown signal.
	cancel()

	// 9. Verify bounded and successful termination.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("supervisor shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not stop")
	}

	// 10. Verify the HTTP port was released.
	reopened, err := net.Listen("tcp", httpAddr)
	if err != nil {
		t.Fatalf("HTTP listener leaked: %v", err)
	}
	defer reopened.Close()
}
