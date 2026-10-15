package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/config"
	"github.com/Tarun516/anzu-agent-runtime/internal/health"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/echo"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/session"
)

// TestSupervisorStopsOnAudioSocketFailure verifies that an
// unexpected media listener failure stops the entire runtime.
//
// The root context is deliberately NOT cancelled before
// the failure. Otherwise the listener's exit would be normal.
func TestSupervisorStopsOnAudioSocketFailure(t *testing.T) {
	const timeout = 3 * time.Second

	// 1. Enable AudioSocket with short shutdown budgets.
	audioCfg := config.AudioSocket{
		Enabled:        true,
		ListenAddress:  "127.0.0.1:0",
		GracePeriod:    200 * time.Millisecond,
		CleanupTimeout: time.Second,
	}

	// 2. Bind both listening sockets.
	listeners, err := bindListeners("127.0.0.1:0", audioCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer listeners.Close()

	httpAddr := listeners.http.Addr().String()
	audioAddr := listeners.audio.Addr().String()

	// 3. Create the normal AudioSocket session manager.
	manager, err := session.NewManager(echo.HandleConnection)
	if err != nil {
		t.Fatal(err)
	}

	// 4. Create the HTTP health server.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	server := health.NewServer(
		"realtime-test",
		httpAddr,
		time.Second,
		logger,
		nil,
	)

	// 5. Start the supervisor.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := make(chan struct{})

	go func() {
		defer close(finished)

		done <- superviseListeners(
			ctx,
			server,
			listeners,
			manager,
			audioCfg,
		)
	}()

	// Ensure resources are cleaned even if a test fails.
	defer func() {
		cancel()
		manager.BeginDrain()
		manager.CloseActive()
		_ = listeners.Close()

		select {
		case <-finished:
		case <-time.After(timeout):
			t.Error("supervisor did not finish during cleanup")
		}
	}()

	// 6. Wait for the HTTP readiness endpoint.
	client := &http.Client{Timeout: 200 * time.Millisecond}
	defer client.CloseIdleConnections()

	readyURL := "http://" + httpAddr + "/readyz"
	deadline := time.Now().Add(timeout)

	for {
		resp, requestErr := client.Get(readyURL)

		if requestErr == nil {
			_ = resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				break
			}
		}

		select {
		case err := <-done:
			t.Fatalf("supervisor exited early: %v", err)
		default:
		}

		if time.Now().After(deadline) {
			t.Fatal("realtime service never became ready")
		}

		time.Sleep(10 * time.Millisecond)
	}

	// 7. Simulate an unexpected media listener failure.
	//
	// We intentionally close the listening socket while
	// the root application context is still active.
	if err := listeners.audio.Close(); err != nil {
		t.Fatalf("close AudioSocket listener: %v", err)
	}

	// 8. The supervisor must detect the failure and exit.
	var supervisorErr error

	select {
	case supervisorErr = <-done:
	case <-time.After(timeout):
		t.Fatal("supervisor ignored AudioSocket failure")
	}

	// 9. Unexpected failure must NOT be reported as success.
	if supervisorErr == nil {
		t.Fatal("expected a supervisor error, got nil")
	}

	// 10. Check that the error identifies AudioSocket.
	if !strings.Contains(
		strings.ToLower(supervisorErr.Error()),
		"audiosocket",
	) {
		t.Fatalf(
			"expected AudioSocket failure, got: %v",
			supervisorErr,
		)
	}

	// 11. Confirm that the original closed-listener error
	// is still accessible through the wrapping chain.
	if !errors.Is(supervisorErr, net.ErrClosed) {
		t.Fatalf(
			"expected wrapped net.ErrClosed, got: %v",
			supervisorErr,
		)
	}

	// 12. HTTP must no longer advertise readiness.
	//
	// Either an HTTP 503 or a refused connection is fine:
	// the supervisor may already have stopped HTTP entirely.
	resp, requestErr := client.Get(readyURL)
	if requestErr == nil {
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			t.Fatal("HTTP remained ready after media failure")
		}
	}

	// 13. Both listening ports must be reusable.
	for _, address := range []string{httpAddr, audioAddr} {
		probe, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf(
				"listener leaked at %s: %v",
				address,
				err,
			)
		}
		_ = probe.Close()
	}
}
