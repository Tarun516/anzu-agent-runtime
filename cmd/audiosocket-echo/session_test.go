package main

import (
	"bytes"
	"fmt"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket"
	"net"
	"testing"
	"time"
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

// TestHandleConnectionStopsOnTruncatedPayload verifies that a peer
// disconnecting mid-frame cannot leave the session handler blocked.
//
// The handler owns the server-side connection and must return
// after detecting the incomplete payload.
func TestHandleConnectionStopsOnTruncatedPayload(t *testing.T) {
	// 1. Start a real session handler over an in-memory connection.
	client, done := startSessionTest(t)

	// 2. Complete the required UUID handshake.
	sendSessionUUID(t, client)

	// 3. Claim 320 bytes of PCM16 but provide only two.
	//
	// 0x10 = PCM16 frame.
	// 0x01 0x40 = 320-byte payload length.
	incomplete := []byte{
		0x10, 0x01, 0x40,
		0x12, 0x34,
	}

	if _, err := client.Write(incomplete); err != nil {
		t.Fatalf("write incomplete audio frame: %v", err)
	}

	// 4. Simulate an unexpected TCP disconnect.
	//
	// ReadFrame should encounter io.ErrUnexpectedEOF
	// because the declared payload was never completed.
	if err := client.Close(); err != nil {
		t.Fatalf("close test client: %v", err)
	}

	// 5. Verify that the handler exits within the test deadline.
	//
	// A stuck handler could otherwise retain a goroutine
	// and network resources after the caller disappears.
	requireSessionStopped(t, done)
}

// TestHandleConnectionAcceptsFragmentedAudio verifies that a valid
// AudioSocket message remains decodable when delivered byte by byte.
//
// The handler owns the server connection. The test owns the client
// connection and checks the returned PCM before ending the call.
func TestHandleConnectionAcceptsFragmentedAudio(t *testing.T) {
	// 1. Start a session and complete its UUID handshake.
	client, done := startSessionTest(t)
	sendSessionUUID(t, client)

	// 2. Construct a valid 20 ms PCM16 audio frame.
	pcm := bytes.Repeat([]byte{0x34, 0x12}, 160)

	var encoded bytes.Buffer

	if err := audiosocket.WriteFrame(&encoded, audiosocket.Frame{
		Type:    audiosocket.TypeAudio,
		Payload: pcm,
	}); err != nil {
		t.Fatalf("encode audio frame: %v", err)
	}

	// 3. Deliver the encoded message one byte at a time.
	//
	// This deliberately stresses the decoder's handling of
	// fragmented transport reads.
	for _, b := range encoded.Bytes() {
		if _, err := client.Write([]byte{b}); err != nil {
			t.Fatalf("write fragmented byte: %v", err)
		}
	}

	// 4. Read the echoed frame and verify exact PCM preservation.
	echoed, err := audiosocket.ReadFrame(client)
	if err != nil {
		t.Fatalf("read echoed frame: %v", err)
	}

	if echoed.Type != audiosocket.TypeAudio {
		t.Fatalf("unexpected echoed type: 0x%02x", echoed.Type)
	}

	if !bytes.Equal(echoed.Payload, pcm) {
		t.Fatal("fragmented audio was not echoed correctly")
	}

	// 5. End the call and verify that its handler exits.
	sendSessionFrame(t, client, audiosocket.Frame{
		Type: audiosocket.TypeHangup,
	})

	requireSessionStopped(t, done)
}

// TestHandleConnectionAcceptsCoalescedFrames verifies that multiple
// AudioSocket messages submitted in one write remain independent.
//
// The test owns the client connection. The session handler owns
// the server connection and must echo both frames before exiting.
func TestHandleConnectionAcceptsCoalescedFrames(t *testing.T) {
	// 1. Start the session and complete the UUID handshake.
	client, done := startSessionTest(t)
	sendSessionUUID(t, client)

	// 2. Construct two distinguishable PCM16 audio frames.
	payloads := [][]byte{
		bytes.Repeat([]byte{0x34, 0x12}, 160),
		bytes.Repeat([]byte{0x78, 0x56}, 160),
	}

	// 3. Encode both frames into one continuous byte stream.
	//
	// Append a hangup frame so the handler can terminate
	// after processing the two audio messages.
	var stream bytes.Buffer

	for _, pcm := range payloads {
		if err := audiosocket.WriteFrame(&stream, audiosocket.Frame{
			Type:    audiosocket.TypeAudio,
			Payload: pcm,
		}); err != nil {
			t.Fatalf("encode audio: %v", err)
		}
	}

	if err := audiosocket.WriteFrame(&stream, audiosocket.Frame{
		Type: audiosocket.TypeHangup,
	}); err != nil {
		t.Fatalf("encode hangup: %v", err)
	}

	// 4. Submit the entire stream in one Write.
	//
	// net.Pipe is unbuffered. The session may block while
	// echoing audio until the client reads it.
	//
	// A goroutine lets sending and receiving proceed
	// concurrently without creating a test-only deadlock.
	writeDone := make(chan error, 1)

	go func() {
		_, err := client.Write(stream.Bytes())
		writeDone <- err
	}()

	// 5. Read both responses and verify each payload.
	for i, expected := range payloads {
		echoed, err := audiosocket.ReadFrame(client)
		if err != nil {
			t.Fatalf("read echoed frame %d: %v", i, err)
		}

		if echoed.Type != audiosocket.TypeAudio {
			t.Fatalf(
				"frame %d: unexpected type 0x%02x",
				i,
				echoed.Type,
			)
		}

		if !bytes.Equal(echoed.Payload, expected) {
			t.Fatalf("frame %d: PCM payload mismatch", i)
		}
	}

	// 6. Confirm the combined write completed.
	//
	// The existing test timeout also bounds client I/O.
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write combined frames: %v", err)
		}
	case <-time.After(sessionTestTimeout):
		t.Fatal("combined frame write did not complete")
	}

	// 7. The handler must process hangup and exit.
	requireSessionStopped(t, done)
}

// TestHandleConnectionStopsOnBlockedWrite verifies that a peer
// which stops reading echoed audio cannot block a session forever.
//
// The test owns the client side of the pipe; handleConnection
// owns the server side and must exit when its write deadline expires.
func TestHandleConnectionStopsOnBlockedWrite(t *testing.T) {
	// 1. Start an identified AudioSocket session.
	client, done := startSessionTest(t)
	sendSessionUUID(t, client)

	// 2. Send one valid PCM16 frame.
	//
	// net.Pipe has no internal buffer. If the client does not
	// read the echo, the server's write remains blocked.
	pcm := bytes.Repeat([]byte{0x34, 0x12}, 160)

	sendSessionFrame(t, client, audiosocket.Frame{
		Type:    audiosocket.TypeAudio,
		Payload: pcm,
	})

	// 3. Deliberately do not read the returned audio.
	//
	// Wait longer than the production write deadline.
	// Without that deadline, this session would remain stuck.
	select {
	case <-done:
		// The handler exited after its write failed.

	case <-time.After(frameWriteTimeout + sessionTestTimeout):
		t.Fatal("session remained blocked beyond its write deadline")
	}
}

// TestHandleConnectionStopsOnHandshakeTimeout verifies that a client
// which connects but sends no UUID cannot hold a session indefinitely.
//
// The test owns the client connection. The handler owns the server
// connection and must exit after the initial read deadline.
func TestHandleConnectionStopsOnHandshakeTimeout(t *testing.T) {
	// 1. Start a session without sending any data.
	start := time.Now()
	_, done := startSessionTest(t)

	// 2. Wait for the real production handshake deadline.
	//
	// The client's test deadline does not terminate the
	// server's independent read deadline.
	select {
	case <-done:
		// 3. Verify the session did not terminate prematurely.
		elapsed := time.Since(start)

		if elapsed < initialFrameTimeout-time.Second {
			t.Fatalf(
				"session stopped too early: %v",
				elapsed,
			)
		}

	case <-time.After(initialFrameTimeout + sessionTestTimeout):
		t.Fatal("session remained blocked after handshake timeout")
	}
}

// TestHandleConnectionIsolatesConcurrentCalls verifies that two
// independent AudioSocket sessions preserve their own PCM and lifetime.
//
// Each handler owns its server-side connection. The test owns both
// client connections and ends one call while the other remains active.
func TestHandleConnectionIsolatesConcurrentCalls(t *testing.T) {
	// 1. Start two independent sessions.
	callA, doneA := startSessionTest(t)
	callB, doneB := startSessionTest(t)

	// 2. Initialize each session with a distinct binary UUID.
	sendSessionFrame(t, callA, audiosocket.Frame{
		Type:    audiosocket.TypeUUID,
		Payload: bytes.Repeat([]byte{0xA1}, 16),
	})

	sendSessionFrame(t, callB, audiosocket.Frame{
		Type:    audiosocket.TypeUUID,
		Payload: bytes.Repeat([]byte{0xB2}, 16),
	})

	// 3. Give each call different PCM data.
	pcmA := bytes.Repeat([]byte{0x11, 0x22}, 160)
	pcmB := bytes.Repeat([]byte{0x33, 0x44}, 160)

	// Verify that a session echoes its own exact PCM bytes.
	assertEcho := func(conn net.Conn, pcm []byte) {
		t.Helper()

		sendSessionFrame(t, conn, audiosocket.Frame{
			Type:    audiosocket.TypeAudio,
			Payload: pcm,
		})

		echoed, err := audiosocket.ReadFrame(conn)
		if err != nil {
			t.Fatalf("read echo: %v", err)
		}

		if echoed.Type != audiosocket.TypeAudio ||
			!bytes.Equal(echoed.Payload, pcm) {
			t.Fatal("incorrect or cross-session PCM echo")
		}
	}

	// 4. Confirm both sessions can independently echo audio.
	assertEcho(callA, pcmA)
	assertEcho(callB, pcmB)

	// 5. Hang up Call A while Call B remains connected.
	sendSessionFrame(t, callA, audiosocket.Frame{
		Type: audiosocket.TypeHangup,
	})

	requireSessionStopped(t, doneA)

	// 6. Verify Call B continues working after Call A ends.
	assertEcho(callB, pcmB)

	select {
	case <-doneB:
		t.Fatal("Call B terminated when Call A hung up")
	default:
		// Call B is still active, as expected.
	}

	// 7. End Call B independently and verify cleanup.
	sendSessionFrame(t, callB, audiosocket.Frame{
		Type: audiosocket.TypeHangup,
	})

	requireSessionStopped(t, doneB)
}

// TestHandleConnectionConcurrentSessionStress exercises multiple
// independent call handlers under concurrent execution.
//
// Each subtest owns its client connection. Its handler owns the
// server connection and must preserve PCM data and exit on hangup.
func TestHandleConnectionConcurrentSessionStress(t *testing.T) {
	const callCount = 16
	const framesPerCall = 10

	// 1. Create independently scheduled call scenarios.
	for i := 0; i < callCount; i++ {
		i := i

		t.Run(fmt.Sprintf("call-%02d", i+1), func(t *testing.T) {
			// 2. Allow multiple subtests to execute concurrently.
			t.Parallel()

			client, done := startSessionTest(t)

			// 3. Assign each call a distinct, deterministic UUID.
			//
			// Set version 4 and RFC 4122 variant bits.
			id := make([]byte, 16)
			id[6] = 0x40
			id[8] = 0x80
			id[15] = byte(i + 1)

			sendSessionFrame(t, client, audiosocket.Frame{
				Type:    audiosocket.TypeUUID,
				Payload: id,
			})

			// 4. Exchange multiple audio frames per call.
			//
			// The PCM differs across calls and frame indices,
			// making accidental cross-session data detectable.
			for j := 0; j < framesPerCall; j++ {
				pcm := bytes.Repeat(
					[]byte{byte(i + 1), byte(j + 1)},
					160,
				)

				sendSessionFrame(t, client, audiosocket.Frame{
					Type:    audiosocket.TypeAudio,
					Payload: pcm,
				})

				echoed, err := audiosocket.ReadFrame(client)
				if err != nil {
					t.Fatalf("read echoed frame %d: %v", j, err)
				}

				if echoed.Type != audiosocket.TypeAudio ||
					!bytes.Equal(echoed.Payload, pcm) {
					t.Fatalf(
						"call %d frame %d: audio mismatch",
						i+1,
						j,
					)
				}
			}

			// 5. End each call and verify handler termination.
			sendSessionFrame(t, client, audiosocket.Frame{
				Type: audiosocket.TypeHangup,
			})

			requireSessionStopped(t, done)
		})
	}
}
