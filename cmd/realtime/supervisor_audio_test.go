package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/config"
	"github.com/Tarun516/anzu-agent-runtime/internal/health"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/echo"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/session"
)

// TestSupervisorAudioSocketPCM verifies that the realtime
// supervisor serves HTTP and AudioSocket concurrently.
// It exchanges real framed PCM bytes over loopback TCP,
// processes hangup, and releases both listening ports.
func TestSupervisorAudioSocketPCM(t *testing.T) {
	const timeout = 3 * time.Second

	// 1. Enable AudioSocket with a short test shutdown policy.
	audioCfg := config.AudioSocket{
		Enabled:        true,
		ListenAddress:  "127.0.0.1:0",
		GracePeriod:    200 * time.Millisecond,
		CleanupTimeout: time.Second,
	}

	// 2. Bind independent HTTP and AudioSocket ports.
	listeners, err := bindListeners("127.0.0.1:0", audioCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer listeners.Close()

	httpAddr := listeners.http.Addr().String()
	audioAddr := listeners.audio.Addr().String()

	// 3. Use the actual reusable AudioSocket echo handler.
	manager, err := session.NewManager(echo.HandleConnection)
	if err != nil {
		t.Fatal(err)
	}

	// 4. Construct health endpoints without requiring PostgreSQL.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	server := health.NewServer(
		"realtime-test",
		httpAddr,
		time.Second,
		logger,
		nil,
	)

	// 5. Start the combined supervisor.
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

	// Always clean up, including when an assertion fails.
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

	// 6. Wait until the HTTP readiness endpoint responds.
	client := &http.Client{Timeout: 250 * time.Millisecond}
	defer client.CloseIdleConnections()

	deadline := time.Now().Add(timeout)

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
		case err := <-done:
			t.Fatalf("supervisor exited early: %v", err)
		default:
		}

		if time.Now().After(deadline) {
			t.Fatal("realtime service never became ready")
		}

		time.Sleep(10 * time.Millisecond)
	}

	// 7. Connect to the actual AudioSocket TCP listener.
	conn, err := net.DialTimeout("tcp", audioAddr, timeout)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatal(err)
	}

	// 8. Send the mandatory 16-byte AudioSocket UUID.
	uuid := make([]byte, 16)
	for i := range uuid {
		uuid[i] = byte(i + 1)
	}

	if err := audiosocket.WriteFrame(conn, audiosocket.Frame{
		Type:    audiosocket.TypeUUID,
		Payload: uuid,
	}); err != nil {
		t.Fatalf("send UUID: %v", err)
	}

	// 9. Send one 320-byte PCM16 frame.
	// At 8 kHz mono, 320 bytes represent 20 ms of audio.
	pcm := make([]byte, 320)
	for i := range pcm {
		pcm[i] = byte(i)
	}

	if err := audiosocket.WriteFrame(conn, audiosocket.Frame{
		Type:    audiosocket.TypeAudio,
		Payload: pcm,
	}); err != nil {
		t.Fatalf("send PCM: %v", err)
	}

	// 10. Read the returned AudioSocket frame.
	echoed, err := audiosocket.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read echoed PCM: %v", err)
	}

	if echoed.Type != audiosocket.TypeAudio {
		t.Fatalf("unexpected frame type: 0x%02x", echoed.Type)
	}

	if !bytes.Equal(echoed.Payload, pcm) {
		t.Fatal("echoed PCM differs from the input")
	}

	// 11. Send a protocol-level call hangup.
	if err := audiosocket.WriteFrame(conn, audiosocket.Frame{
		Type: audiosocket.TypeHangup,
	}); err != nil {
		t.Fatalf("send hangup: %v", err)
	}

	// 12. Verify the session handler has finished.
	deadline = time.Now().Add(timeout)

	for manager.ActiveCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("AudioSocket session was not released")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 13. Request application shutdown.
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("supervisor shutdown: %v", err)
		}
	case <-time.After(timeout):
		t.Fatal("supervisor shutdown timed out")
	}

	// 14. Confirm both listening sockets were released.
	for _, address := range []string{httpAddr, audioAddr} {
		probe, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("listener leaked at %s: %v", address, err)
		}
		_ = probe.Close()
	}
}
