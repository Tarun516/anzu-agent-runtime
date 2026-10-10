package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket"
)

const (
	// Allow Asterisk enough time to send the initial UUID.
	initialFrameTimeout = 10 * time.Second

	// Bound how long a connected client can stop sending data.
	// This is a lab policy; long silent calls may need different handling.
	idleReadTimeout = 2 * time.Minute

	// Prevent a slow or unresponsive peer from blocking writes forever.
	frameWriteTimeout = 5 * time.Second
)

// handleConnection manages one AudioSocket connection for its lifetime.
//
// It expects a UUID before audio, returns valid PCM audio to the caller,
// and releases the connection when the call ends or encounters an error.
//
// The function owns this connection. Other call handlers must not
// read from or write to the same connection.
func handleConnection(conn net.Conn) {
	// 1. Release the connection whenever this handler returns.
	// This includes normal hangup, EOF, malformed data and timeouts.
	defer conn.Close()

	remote := conn.RemoteAddr().String()

	// 2. Bound the initial handshake.
	// A client that connects but sends nothing must not retain
	// a goroutine and socket indefinitely.
	if err := conn.SetReadDeadline(
		time.Now().Add(initialFrameTimeout),
	); err != nil {
		log.Printf("set handshake deadline: %v", err)
		return
	}

	// 3. Read the initial AudioSocket message.
	// Asterisk sends a 16-byte binary UUID to identify the call.
	first, err := audiosocket.ReadFrame(conn)
	if err != nil {
		log.Printf(
			"initial frame failed: remote=%s error=%v",
			remote,
			err,
		)
		return
	}

	// 4. Require the UUID before accepting any audio.
	// Receiving audio first means the peer did not follow
	// our expected session initialization protocol.
	if first.Type != audiosocket.TypeUUID {
		log.Printf(
			"expected UUID frame: remote=%s type=0x%02x",
			remote,
			first.Type,
		)
		return
	}

	// ReadFrame already validates the UUID's 16-byte length.
	callID := formatCallUUID(first.Payload)

	log.Printf(
		"call started: uuid=%s remote=%s",
		callID,
		remote,
	)

	// 5. Track successful echoed audio without logging
	// private PCM contents or sensitive keypad digits.
	var echoedFrames int64
	var echoedBytes int64
	var dtmfEvents int64

	defer func() {
		log.Printf(
			"call ended: uuid=%s frames=%d pcm_bytes=%d dtmf_events=%d",
			callID,
			echoedFrames,
			echoedBytes,
			dtmfEvents,
		)
	}()

	// 6. Process messages until hangup, disconnect or failure.
	// Each iteration reads exactly one complete AudioSocket frame.
	for {
		// Refresh the idle deadline for the next message.
		// A peer that stalls halfway through a frame will
		// eventually be disconnected instead of blocking forever.
		if err := conn.SetReadDeadline(
			time.Now().Add(idleReadTimeout),
		); err != nil {
			log.Printf("read deadline failed: uuid=%s error=%v", callID, err)
			return
		}

		frame, err := audiosocket.ReadFrame(conn)
		if err != nil {
			// EOF at a frame boundary means the remote peer
			// closed its side of the connection.
			if errors.Is(err, io.EOF) {
				log.Printf("peer disconnected: uuid=%s", callID)
				return
			}

			// A truncated frame, timeout or other I/O error
			// ends only this call, not the TCP listener.
			log.Printf(
				"read failed: uuid=%s error=%v",
				callID,
				err,
			)
			return
		}

		// 7. Dispatch by application-level message type.
		switch frame.Type {
		case audiosocket.TypeAudio:
			// Echo the original PCM bytes without decoding,
			// resampling or changing their representation.
			//
			// The AudioSocket application already provides
			// signed 16-bit, 8 kHz mono PCM to this boundary.

			// Prevent a blocked write from stalling forever.
			if err := conn.SetWriteDeadline(
				time.Now().Add(frameWriteTimeout),
			); err != nil {
				log.Printf(
					"write deadline failed: uuid=%s error=%v",
					callID,
					err,
				)
				return
			}

			// WriteFrame sends a complete framed message.
			// Only this goroutine writes to this connection,
			// so headers and payloads cannot interleave.
			if err := audiosocket.WriteFrame(conn, frame); err != nil {
				log.Printf(
					"audio echo failed: uuid=%s error=%v",
					callID,
					err,
				)
				return
			}

			// Count only audio successfully written back.
			echoedFrames++
			echoedBytes += int64(len(frame.Payload))

		case audiosocket.TypeDTMF:
			// DTMF represents a keypad event, not speech.
			// Avoid logging the actual digit because it
			// may be part of a PIN or another secret.
			dtmfEvents++

		case audiosocket.TypeHangup:
			// Asterisk has signaled connection termination.
			// Returning triggers deferred socket cleanup.
			log.Printf("hangup received: uuid=%s", callID)
			return

		case audiosocket.TypeError:
			// The peer has reported a protocol/application
			// error. The small diagnostic payload is not PCM.
			log.Printf(
				"peer reported error: uuid=%s code=%x",
				callID,
				frame.Payload,
			)
			return

		case audiosocket.TypeUUID:
			// A second UUID would unexpectedly change the
			// identity of an already initialized call.
			log.Printf("duplicate UUID: uuid=%s", callID)
			return

		default:
			// ReadFrame currently rejects unsupported types,
			// but retain an explicit defensive session boundary.
			log.Printf(
				"unsupported frame: uuid=%s type=0x%02x",
				callID,
				frame.Type,
			)
			return
		}
	}
}

// formatCallUUID converts a validated 16-byte binary UUID into
// the standard 8-4-4-4-12 hexadecimal representation.
//
// The formatted identifier makes it easier to correlate Go
// logs with the UUID supplied by the Asterisk dialplan.
func formatCallUUID(id []byte) string {
	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		id[0:4],
		id[4:6],
		id[6:8],
		id[8:10],
		id[10:16],
	)
}
