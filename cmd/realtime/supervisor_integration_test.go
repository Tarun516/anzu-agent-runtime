package main

import (
	"bytes"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/config"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/echo"
)

// mediaTestConfig returns valid settings for an optional local media listener.
func mediaTestConfig(grace time.Duration) config.AudioSocket {
	return config.AudioSocket{
		Enabled:        true,
		ListenAddress:  "127.0.0.1:0",
		GracePeriod:    grace,
		CleanupTimeout: time.Second,
	}
}

// dialSupervisorAudio connects to the actual TCP media socket and sets a
// deadline, preventing accidental hangs in a failing test.
func dialSupervisorAudio(t *testing.T, runtime *supervisorTestRuntime) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", runtime.audioAddr, supervisorTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(supervisorTestTimeout)); err != nil {
		t.Fatal(err)
	}
	return conn
}

// waitForActiveCount synchronizes with handler completion without sleeping for
// an assumed amount of time. The manager count is protected by its own mutex.
func waitForActiveCount(t *testing.T, runtime *supervisorTestRuntime, want int) {
	t.Helper()
	deadline := time.Now().Add(supervisorTestTimeout)
	for runtime.manager.ActiveCount() != want {
		if time.Now().After(deadline) {
			t.Fatalf("active sessions = %d, want %d", runtime.manager.ActiveCount(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSupervisorAudioSocketPCM covers HTTP readiness, the UUID handshake, a
// framed PCM16 echo, normal hangup, cleanup, and closure of both TCP listeners.
func TestSupervisorAudioSocketPCM(t *testing.T) {
	runtime := startSupervisorForTest(t, mediaTestConfig(200*time.Millisecond), echo.HandleConnection)
	conn := dialSupervisorAudio(t, runtime)

	// 1. Every AudioSocket connection must start with a 16-byte UUID frame.
	uuid := make([]byte, 16)
	for i := range uuid {
		uuid[i] = byte(i + 1)
	}
	if err := audiosocket.WriteFrame(conn, audiosocket.Frame{
		Type: audiosocket.TypeUUID, Payload: uuid,
	}); err != nil {
		t.Fatalf("send UUID: %v", err)
	}

	// 2. The handler must echo one 320-byte / 20ms PCM16 payload exactly.
	pcm := make([]byte, 320)
	for i := range pcm {
		pcm[i] = byte(i)
	}
	if err := audiosocket.WriteFrame(conn, audiosocket.Frame{
		Type: audiosocket.TypeAudio, Payload: pcm,
	}); err != nil {
		t.Fatalf("send PCM: %v", err)
	}
	echoed, err := audiosocket.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read echoed PCM: %v", err)
	}
	if echoed.Type != audiosocket.TypeAudio || !bytes.Equal(echoed.Payload, pcm) {
		t.Fatal("AudioSocket handler did not echo the original PCM16 frame")
	}

	// 3. Explicit hangup must release the registered handler.
	if err := audiosocket.WriteFrame(conn, audiosocket.Frame{Type: audiosocket.TypeHangup}); err != nil {
		t.Fatalf("send hangup: %v", err)
	}
	waitForActiveCount(t, runtime, 0)

	runtime.cancel()
	if err := awaitSupervisorResult(t, runtime); err != nil {
		t.Fatalf("supervisor shutdown: %v", err)
	}
	assertSupervisorPortsReleased(t, runtime)
}

// TestSupervisorDrainsNaturalCompletion proves that an already-admitted call
// remains usable after new-call admission has stopped.
func TestSupervisorDrainsNaturalCompletion(t *testing.T) {
	started := make(chan struct{})
	runtime := startSupervisorForTest(t, mediaTestConfig(2*time.Second), func(conn net.Conn) {
		defer conn.Close()
		close(started)
		var b [1]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return
		}
		_, _ = conn.Write(b[:])
	})
	conn := dialSupervisorAudio(t, runtime)

	select {
	case <-started:
	case <-time.After(supervisorTestTimeout):
		t.Fatal("session handler did not start")
	}
	waitForActiveCount(t, runtime, 1)

	runtime.cancel()
	select {
	case err := <-runtime.result:
		t.Fatalf("supervisor exited with an active call: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Accepted connections belong to their handlers, not the listener.
	if _, err := conn.Write([]byte{0x42}); err != nil {
		t.Fatalf("write during drain: %v", err)
	}
	var echoed [1]byte
	if _, err := io.ReadFull(conn, echoed[:]); err != nil {
		t.Fatalf("read during drain: %v", err)
	}
	if echoed[0] != 0x42 {
		t.Fatalf("echoed byte = %02x, want 42", echoed[0])
	}

	if err := awaitSupervisorResult(t, runtime); err != nil {
		t.Fatalf("natural drain failed: %v", err)
	}
	waitForActiveCount(t, runtime, 0)
	assertSupervisorPortsReleased(t, runtime)
}

// TestSupervisorForcesStuckSession verifies the grace timer force-closes an
// accepted socket to interrupt an otherwise indefinitely blocked read.
func TestSupervisorForcesStuckSession(t *testing.T) {
	started := make(chan struct{})
	readFinished := make(chan error, 1)
	runtime := startSupervisorForTest(t, mediaTestConfig(200*time.Millisecond), func(conn net.Conn) {
		defer conn.Close()
		close(started)
		var b [1]byte
		_, err := conn.Read(b[:])
		readFinished <- err
	})
	_ = dialSupervisorAudio(t, runtime) // Keep client open without sending bytes.

	select {
	case <-started:
	case <-time.After(supervisorTestTimeout):
		t.Fatal("session handler did not start")
	}
	waitForActiveCount(t, runtime, 1)

	startedAt := time.Now()
	runtime.cancel()
	if err := awaitSupervisorResult(t, runtime); err != nil {
		t.Fatalf("forced drain failed: %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed < 100*time.Millisecond {
		t.Fatalf("shutdown ended before grace period: %s", elapsed)
	}
	select {
	case err := <-readFinished:
		if err == nil {
			t.Fatal("socket Read unexpectedly succeeded")
		}
	case <-time.After(supervisorTestTimeout):
		t.Fatal("forced close failed to unblock handler")
	}
	waitForActiveCount(t, runtime, 0)
	assertSupervisorPortsReleased(t, runtime)
}

// TestSupervisorReportsCleanupTimeout ensures a stubborn handler that ignores
// socket closure does not cause an unbounded shutdown or false success.
func TestSupervisorReportsCleanupTimeout(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	releaseHandler := func() { once.Do(func() { close(release) }) }
	defer releaseHandler() // Do not leak a deliberately stuck handler on failure.

	runtime := startSupervisorForTest(t, mediaTestConfig(100*time.Millisecond), func(conn net.Conn) {
		defer conn.Close()
		close(started)
		<-release // Intentionally ignores the closed network connection.
	})
	_ = dialSupervisorAudio(t, runtime)

	select {
	case <-started:
	case <-time.After(supervisorTestTimeout):
		t.Fatal("session handler did not start")
	}
	waitForActiveCount(t, runtime, 1)

	runtime.cancel()
	err := awaitSupervisorResult(t, runtime)
	if err == nil || !strings.Contains(err.Error(), "cleanup timed out") {
		t.Fatalf("expected bounded cleanup timeout, got: %v", err)
	}
	waitForActiveCount(t, runtime, 1)
	assertSupervisorPortsReleased(t, runtime)

	// Now release the synthetic handler so the test itself leaves no goroutines.
	releaseHandler()
	waitDone := make(chan struct{})
	go func() {
		runtime.manager.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(supervisorTestTimeout):
		t.Fatal("handler still running after test release")
	}
	waitForActiveCount(t, runtime, 0)
}
