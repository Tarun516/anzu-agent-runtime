package audiosocket

import (
	"bytes"
	"io"
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
