package audiosocket

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// Simulates TCP delivering one byte per Read.
type slowReader struct {
	io.Reader
}

func (r slowReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}

func TestAudioFrameRoundTrip(t *testing.T) {
	original := Frame{
		Type:    TypeAudio,
		Payload: make([]byte, 320),
	}

	var buf bytes.Buffer

	if err := WriteFrame(&buf, original); err != nil {
		t.Fatal(err)
	}

	// 3 header bytes + 320 audio bytes.
	if buf.Len() != 323 {
		t.Fatalf("got %d bytes, want 323", buf.Len())
	}

	decoded, err := ReadFrame(
		slowReader{Reader: &buf},
	)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Type != original.Type ||
		!bytes.Equal(decoded.Payload, original.Payload) {
		t.Fatal("frame changed during round trip")
	}
}

func TestMultipleFrames(t *testing.T) {
	var buf bytes.Buffer

	frames := []Frame{
		{Type: TypeUUID, Payload: make([]byte, 16)},
		{Type: TypeAudio, Payload: make([]byte, 320)},
		{Type: TypeHangup},
	}

	for _, f := range frames {
		if err := WriteFrame(&buf, f); err != nil {
			t.Fatal(err)
		}
	}

	for _, expected := range frames {
		actual, err := ReadFrame(&buf)
		if err != nil {
			t.Fatal(err)
		}

		if actual.Type != expected.Type ||
			!bytes.Equal(actual.Payload, expected.Payload) {
			t.Fatal("unexpected decoded frame")
		}
	}
}

func TestIncompleteAudio(t *testing.T) {
	// Header promises 320 bytes.
	// Only 2 payload bytes are provided.
	data := []byte{
		0x10, 0x01, 0x40,
		0x12, 0x34,
	}

	_, err := ReadFrame(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected truncated payload error")
	}
}

// countingWriter records how many Write calls the encoder makes.
//
// Its underlying bytes.Buffer accepts the entire message, allowing
// the test to verify that WriteFrame does not deliberately split
// the protocol header and payload into separate writes.
type countingWriter struct {
	writes int
	data   bytes.Buffer
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.data.Write(p)
}

// TestWriteFrameUsesSingleWrite verifies that a complete AudioSocket
// message is submitted in one Write when the writer accepts all bytes.
func TestWriteFrameUsesSingleWrite(t *testing.T) {
	// 1. Construct exactly 20 ms of PCM16 audio.
	pcm := bytes.Repeat([]byte{0x34, 0x12}, 160)

	writer := &countingWriter{}

	// 2. Encode and submit the message.
	err := WriteFrame(writer, Frame{
		Type:    TypeAudio,
		Payload: pcm,
	})
	if err != nil {
		t.Fatalf("write frame: %v", err)
	}

	// 3. Verify that header and payload were not deliberately
	// separated into multiple Write calls.
	if writer.writes != 1 {
		t.Fatalf("got %d writes, want 1", writer.writes)
	}

	// 4. Verify the exact message bytes.
	expected := append(
		[]byte{0x10, 0x01, 0x40},
		pcm...,
	)

	if !bytes.Equal(writer.data.Bytes(), expected) {
		t.Fatal("encoded AudioSocket message differs")
	}
}

// TestReadFrameRejectsTruncatedHeader verifies that a connection
// ending midway through the 3-byte header cannot produce a valid frame.
func TestReadFrameRejectsTruncatedHeader(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{
			name: "only type byte",
			data: []byte{TypeAudio},
		},
		{
			name: "type and first length byte",
			data: []byte{TypeAudio, 0x01},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Simulate the connection ending with a partial header.
			reader := bytes.NewReader(tc.data)

			// 2. Ask the real decoder to read a complete frame.
			_, err := ReadFrame(reader)

			// 3. A partially received header must be rejected.
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf(
					"got error %v, want io.ErrUnexpectedEOF",
					err,
				)
			}
		})
	}
}

// TestReadFrameRejectsOversizedPayload verifies that an untrusted
// length field cannot make the decoder allocate beyond its safety cap.
//
// Only the header is supplied because the decoder should reject
// the claimed length before attempting to read its payload.
func TestReadFrameRejectsOversizedPayload(t *testing.T) {
	cases := []struct {
		name   string
		header []byte
	}{
		{
			name:   "one byte above limit",
			header: []byte{TypeAudio, 0x10, 0x01}, // 4097 bytes
		},
		{
			name:   "maximum encoded length",
			header: []byte{TypeAudio, 0xFF, 0xFF}, // 65535 bytes
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Supply only the malicious three-byte header.
			reader := bytes.NewReader(tc.header)

			// 2. Attempt to decode the frame.
			_, err := ReadFrame(reader)

			// 3. Require a payload-size rejection, not an EOF
			// caused by trying to read the missing payload.
			if err == nil || !strings.Contains(
				err.Error(),
				"payload too large",
			) {
				t.Fatalf(
					"got error %v, want payload-size rejection",
					err,
				)
			}
		})
	}
}



