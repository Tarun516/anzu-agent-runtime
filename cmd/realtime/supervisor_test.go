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

const supervisorTestTimeout = 5 * time.Second

// supervisorTestRuntime owns a real, local supervisor process lifecycle.
// Its helper methods let tests focus on behavior instead of repeating setup.
type supervisorTestRuntime struct {
	listeners *boundListeners
	manager   *session.Manager
	httpAddr  string
	audioAddr string
	cancel    context.CancelFunc
	result    chan error
	finished  chan struct{}
	client    *http.Client
}

// startSupervisorForTest starts HTTP and, optionally, AudioSocket on OS-assigned
// loopback ports. No PostgreSQL or Asterisk service is required for these tests.
func startSupervisorForTest(t *testing.T, audioCfg config.AudioSocket, handler session.Handler) *supervisorTestRuntime {
	t.Helper()

	// 1. Bind both listening sockets before starting either server.
	listeners, err := bindListeners("127.0.0.1:0", audioCfg)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Create the injected per-connection handler and HTTP readiness server.
	manager, err := session.NewManager(handler)
	if err != nil {
		_ = listeners.Close()
		t.Fatal(err)
	}

	httpAddr := listeners.http.Addr().String()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	healthServer := health.NewServer("realtime-test", httpAddr, time.Second, logger, nil)

	ctx, cancel := context.WithCancel(context.Background())
	runtime := &supervisorTestRuntime{
		listeners: listeners,
		manager:   manager,
		httpAddr:  httpAddr,
		cancel:    cancel,
		result:    make(chan error, 1),
		finished:  make(chan struct{}),
		client: &http.Client{
			Timeout: 250 * time.Millisecond,
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
		},
	}
	if listeners.audio != nil {
		runtime.audioAddr = listeners.audio.Addr().String()
	}

	// 3. Run the real supervisor asynchronously and capture its terminal result.
	go func() {
		defer close(runtime.finished)
		runtime.result <- superviseListeners(ctx, healthServer, listeners, manager, audioCfg)
	}()

	// 4. Always unblock the supervisor and release ports, even on test failure.
	t.Cleanup(func() {
		cancel()
		manager.BeginDrain()
		manager.CloseActive()
		_ = listeners.Close()
		runtime.client.CloseIdleConnections()

		select {
		case <-runtime.finished:
		case <-time.After(supervisorTestTimeout):
			t.Error("supervisor did not finish during cleanup")
		}
	})

	// 5. Poll the actual HTTP endpoint instead of assuming a startup delay.
	deadline := time.Now().Add(supervisorTestTimeout)
	for {
		resp, requestErr := runtime.client.Get("http://" + httpAddr + "/readyz")
		if requestErr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return runtime
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
}

// awaitSupervisorResult observes the server's terminal error with a hard timeout.
func awaitSupervisorResult(t *testing.T, runtime *supervisorTestRuntime) error {
	t.Helper()
	select {
	case err := <-runtime.result:
		return err
	case <-time.After(supervisorTestTimeout):
		t.Fatal("supervisor failed to terminate")
		return nil
	}
}

// assertSupervisorPortsReleased verifies that listening ports are reusable.
// It does not assert anything about accepted connections still in flight.
func assertSupervisorPortsReleased(t *testing.T, runtime *supervisorTestRuntime) {
	t.Helper()
	for _, address := range []string{runtime.httpAddr, runtime.audioAddr} {
		if address == "" {
			continue
		}
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("listener leaked at %s: %v", address, err)
		}
		_ = listener.Close()
	}
}

// TestSupervisorHTTPOnlyLifecycle proves the optional-media mode can serve
// readiness and release the HTTP listener after root cancellation.
func TestSupervisorHTTPOnlyLifecycle(t *testing.T) {
	runtime := startSupervisorForTest(t, config.AudioSocket{}, func(conn net.Conn) {
		_ = conn.Close()
	})

	runtime.cancel()
	if err := awaitSupervisorResult(t, runtime); err != nil {
		t.Fatalf("HTTP-only shutdown failed: %v", err)
	}
	assertSupervisorPortsReleased(t, runtime)
}

// TestSupervisorStopsOnAudioSocketFailure proves a media listener failure is
// fatal to the combined service, rather than leaving HTTP falsely ready.
func TestSupervisorStopsOnAudioSocketFailure(t *testing.T) {
	runtime := startSupervisorForTest(t, config.AudioSocket{
		Enabled:        true,
		ListenAddress:  "127.0.0.1:0",
		GracePeriod:    200 * time.Millisecond,
		CleanupTimeout: time.Second,
	}, echo.HandleConnection)

	// Root context is intentionally still active: this is not normal shutdown.
	if err := runtime.listeners.audio.Close(); err != nil {
		t.Fatalf("close AudioSocket listener: %v", err)
	}

	failure := awaitSupervisorResult(t, runtime)
	if failure == nil || !strings.Contains(strings.ToLower(failure.Error()), "audiosocket") {
		t.Fatalf("expected fatal AudioSocket error, got: %v", failure)
	}
	if !errors.Is(failure, net.ErrClosed) {
		t.Fatalf("expected wrapped net.ErrClosed, got: %v", failure)
	}

	// HTTP may return 503 or already be stopped. It must never remain ready.
	resp, err := runtime.client.Get("http://" + runtime.httpAddr + "/readyz")
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("HTTP remained ready after a fatal media error")
		}
	}
	assertSupervisorPortsReleased(t, runtime)
}
