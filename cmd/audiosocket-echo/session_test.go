package main

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket"
)

const sessionTestTimeout = 3 * time.Second

// startSessionTest creates an in-memory connection to the real session handler.
//
// The returned client represents Asterisk; handleConnection owns the server end.
// Cleanup closes both endpoints and verifies that the handler eventually stops.
func startSessionTest(t *testing.T) (net.Conn, <-chan struct{}) {
	t.Helper()

	// 1. Create an in-memory full-duplex connection without opening a real port.
	server, client := net.Pipe()

	// 2. Bound reads and writes so a broken protocol loop cannot hang the test.
	if err := client.SetDeadline(time.Now().Add(sessionTestTimeout)); err != nil {
		server.Close()
		client.Close()
		t.Fatalf("set client deadline: %v", err)
	}

	// 3. Run the same connection handler used by the TCP listener.
	// The channel provides a deterministic completion signal to assertions.
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnection(server)
	}()

	// 4. Close both endpoints and check that the session goroutine exits.
	// This also runs when an earlier t.Fatal stops the test unexpectedly.
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()

		select {
		case <-done:
		case <-time.After(sessionTestTimeout):
			t.Error("session goroutine did not exit during cleanup")
		}
	})

	return client, done
}

// sendSessionFrame writes exactly one valid AudioSocket frame as the test client.
//
// Reusing the production encoder isolates each test's intent and avoids manually
// constructing binary headers, except in tests deliberately sending bad bytes.
func sendSessionFrame(t *testing.T, conn net.Conn, frame audiosocket.Frame) {
	t.Helper()

	// 1. Send a frame through the actual framing implementation.
	if err := audiosocket.WriteFrame(conn, frame); err != nil {
		t.Fatalf("send frame type 0x%02x: %v", frame.Type, err)
	}
}

// sendSessionUUID initializes a test call with a valid 16-byte binary UUID.
//
// A UUID identifies the logical call but does not authenticate the TCP peer.
func sendSessionUUID(t *testing.T, conn net.Conn) {
	t.Helper()

	// 1. Supply the expected first message before sending any PCM frames.
	sendSessionFrame(t, conn, audiosocket.Frame{
		Type: audiosocket.TypeUUID,
		Payload: []byte{
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
			0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		},
	})
}

// requireSessionStopped waits for a handler to finish after hangup or failure.
//
// A timeout indicates a possible stuck read, blocked write, or goroutine leak.
func requireSessionStopped(t *testing.T, done <-chan struct{}) {
	t.Helper()

	// 1. Verify termination without relying on arbitrary sleeps.
	select {
	case <-done:
	case <-time.After(sessionTestTimeout):
		t.Fatal("session did not stop within the test deadline")
	}
}

// TestHandleConnectionEchoAndHangup checks real session initialization,
// multiple PCM round trips, and cleanup after a hangup message.
func TestHandleConnectionEchoAndHangup(t *testing.T) {
	// 1. Start the handler and establish a correctly identified session.
	client, done := startSessionTest(t)
	sendSessionUUID(t, client)

	// 2. Exercise multiple PCM frames with different sample bytes.
	// At 8 kHz, 160 mono PCM16 samples represent 20 ms (320 bytes).
	payloads := [][]byte{
		bytes.Repeat([]byte{0x34, 0x12}, 160),
		bytes.Repeat([]byte{0x78, 0x56}, 160),
		bytes.Repeat([]byte{0x00, 0x00}, 160),
	}

	for i, pcm := range payloads {
		// 3. Send one frame, then read its echo before sending the next.
		// net.Pipe has no internal buffering; this avoids a test-only deadlock.
		sendSessionFrame(t, client, audiosocket.Frame{
			Type:    audiosocket.TypeAudio,
			Payload: pcm,
		})

		echoed, err := audiosocket.ReadFrame(client)
		if err != nil {
			t.Fatalf("read echoed frame %d: %v", i, err)
		}

		// 4. Verify the handler preserved both the type and all PCM bytes.
		if echoed.Type != audiosocket.TypeAudio {
			t.Fatalf(
				"echoed frame %d has type 0x%02x, want 0x10",
				i,
				echoed.Type,
			)
		}
		if !bytes.Equal(echoed.Payload, pcm) {
			t.Fatalf("echoed PCM differs in frame %d", i)
		}
	}

	// 5. End the call using the protocol's termination message.
	sendSessionFrame(t, client, audiosocket.Frame{Type: audiosocket.TypeHangup})
	requireSessionStopped(t, done)
}

// TestHandleConnectionRejectsAudioBeforeUUID enforces the session handshake.
// A connected TCP socket is not yet a valid, identified AudioSocket call.
func TestHandleConnectionRejectsAudioBeforeUUID(t *testing.T) {
	// 1. Establish a connection without the required UUID frame.
	client, done := startSessionTest(t)

	// 2. Send otherwise-valid audio as the first application message.
	sendSessionFrame(t, client, audiosocket.Frame{
		Type:    audiosocket.TypeAudio,
		Payload: make([]byte, 320),
	})

	// 3. The handler must reject this call rather than echo the audio.
	requireSessionStopped(t, done)
}

// TestHandleConnectionRejectsDuplicateUUID prevents an established session
// from silently changing its identity when a second UUID arrives.
func TestHandleConnectionRejectsDuplicateUUID(t *testing.T) {
	// 1. Complete the normal UUID handshake.
	client, done := startSessionTest(t)
	sendSessionUUID(t, client)

	// 2. Try to initialize the same connection a second time.
	sendSessionFrame(t, client, audiosocket.Frame{
		Type:    audiosocket.TypeUUID,
		Payload: bytes.Repeat([]byte{0x42}, 16),
	})

	// 3. The session must end rather than silently replace its call ID.
	requireSessionStopped(t, done)
}

// TestHandleConnectionIgnoresDTMF checks that keypad messages are not
// mistaken for audio, while a following real PCM frame still echoes.
func TestHandleConnectionIgnoresDTMF(t *testing.T) {
	// 1. Establish a legitimate session.
	client, done := startSessionTest(t)
	sendSessionUUID(t, client)

	// 2. Deliver a keypad event. It must not be written back as PCM.
	sendSessionFrame(t, client, audiosocket.Frame{
		Type:    audiosocket.TypeDTMF,
		Payload: []byte{'5'},
	})

	// 3. Deliver audio and verify the NEXT returned frame is that audio.
	pcm := bytes.Repeat([]byte{0x11, 0x22}, 160)
	sendSessionFrame(t, client, audiosocket.Frame{
		Type:    audiosocket.TypeAudio,
		Payload: pcm,
	})

	echoed, err := audiosocket.ReadFrame(client)
	if err != nil {
		t.Fatalf("read audio after DTMF: %v", err)
	}
	if echoed.Type != audiosocket.TypeAudio || !bytes.Equal(echoed.Payload, pcm) {
		t.Fatal("expected unchanged PCM audio after DTMF event")
	}

	// 4. Terminate normally and ensure the handler exits.
	sendSessionFrame(t, client, audiosocket.Frame{Type: audiosocket.TypeHangup})
	requireSessionStopped(t, done)
}

// TestHandleConnectionHandlesPeerDisconnect verifies that an abrupt TCP close
// ends the call instead of keeping an abandoned session goroutine alive.
func TestHandleConnectionHandlesPeerDisconnect(t *testing.T) {
	// 1. Establish a legitimate call, then disconnect without a hangup frame.
	client, done := startSessionTest(t)
	sendSessionUUID(t, client)
	if err := client.Close(); err != nil {
		t.Fatalf("close test client: %v", err)
	}

	// 2. EOF must cause the handler to clean up.
	requireSessionStopped(t, done)
}

// TestHandleConnectionRejectsMalformedPCM ensures that protocol validation
// stops a single malformed session without needing a real Asterisk server.
func TestHandleConnectionRejectsMalformedPCM(t *testing.T) {
	// 1. Start a normal session.
	client, done := startSessionTest(t)
	sendSessionUUID(t, client)

	// 2. Send raw malformed bytes that WriteFrame would correctly reject.
	// 0x10 = PCM16, length 3 is invalid because a sample needs 2 bytes.
	malformed := []byte{0x10, 0x00, 0x03, 0x11, 0x22, 0x33}
	if _, err := client.Write(malformed); err != nil {
		t.Fatalf("send malformed PCM: %v", err)
	}

	// 3. The handler must reject the frame and terminate.
	requireSessionStopped(t, done)
}

// TestHandleConnectionHandlesProtocolError checks that an error reported by
// the peer ends this call without forwarding that error payload as audio.
func TestHandleConnectionHandlesProtocolError(t *testing.T) {
	// 1. Initialize the session and send an AudioSocket error indication.
	client, done := startSessionTest(t)
	sendSessionUUID(t, client)
	sendSessionFrame(t, client, audiosocket.Frame{
		Type:    audiosocket.TypeError,
		Payload: []byte{0x01},
	})

	// 2. The handler should stop after receiving the error message.
	requireSessionStopped(t, done)
}

// TestFormatCallUUID verifies that a binary UUID is rendered in the standard
// 8-4-4-4-12 hexadecimal form used to correlate Asterisk and Go logs.
func TestFormatCallUUID(t *testing.T) {
	// 1. Start from a known byte sequence rather than using random input.
	id := []byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	}

	// 2. Check exact text formatting, not merely string length.
	const want = "00010203-0405-0607-0809-0a0b0c0d0e0f"
	if got := formatCallUUID(id); got != want {
		t.Fatalf("formatCallUUID() = %q, want %q", got, want)
	}
}
