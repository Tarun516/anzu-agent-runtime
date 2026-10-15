package audio

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestPCM16SignedLittleEndian(t *testing.T) {
	dec, err := NewPCM16Decoder(8000, 1)
	if err != nil {
		t.Fatal(err)
	}
	// 0, 32767, -32768, -1 (signed 16-bit, little endian).
	wire := []byte{0, 0, 0xff, 0x7f, 0, 0x80, 0xff, 0xff}
	f, err := dec.Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	if f.Sequence != 0 || f.Timestamp != 0 || f.SampleRate != 8000 || f.Channels != 1 {
		t.Fatalf("unexpected frame metadata: %+v", f)
	}
	if !reflect.DeepEqual(f.Samples, []int16{0, 32767, -32768, -1}) {
		t.Fatalf("decoded samples: %v", f.Samples)
	}
	if f.Duration() != 500*time.Microsecond {
		t.Fatalf("duration = %v, want 500us", f.Duration())
	}
	encoded, err := EncodePCM16LE(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, wire) {
		t.Fatalf("roundtrip differs: got %v", encoded)
	}
}

func TestPCM16StreamClockVariablePacketSizes(t *testing.T) {
	dec, err := NewPCM16Decoder(8000, 1)
	if err != nil {
		t.Fatal(err)
	}
	packets := []struct {
		bytes    int
		seq      uint64
		start    time.Duration
		duration time.Duration
	}{
		{320, 0, 0, 20 * time.Millisecond},
		{160, 1, 20 * time.Millisecond, 10 * time.Millisecond},
		{320, 2, 30 * time.Millisecond, 20 * time.Millisecond},
	}
	for _, tc := range packets {
		f, err := dec.Decode(make([]byte, tc.bytes))
		if err != nil {
			t.Fatal(err)
		}
		if f.Sequence != tc.seq || f.Timestamp != tc.start || f.Duration() != tc.duration {
			t.Fatalf("frame: seq=%d, start=%v, duration=%v", f.Sequence, f.Timestamp, f.Duration())
		}
	}
}

func TestPCM16DecoderRejectsInvalidWithoutAdvancing(t *testing.T) {
	dec, err := NewPCM16Decoder(8000, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, wire := range [][]byte{nil, {0}, {0, 0}} {
		if _, err := dec.Decode(wire); err == nil {
			t.Fatalf("expected invalid payload error for %v", wire)
		}
	}
	f, err := dec.Decode([]byte{1, 0, 2, 0}) // one sample per channel
	if err != nil {
		t.Fatal(err)
	}
	if f.Sequence != 0 || f.Timestamp != 0 || f.SamplesPerChannel() != 1 || f.Duration() != 125*time.Microsecond {
		t.Fatalf("unexpected stereo frame: %+v", f)
	}
}

func TestPCM16BufferOwnership(t *testing.T) {
	dec, err := NewPCM16Decoder(8000, 1)
	if err != nil {
		t.Fatal(err)
	}
	wire := []byte{0x34, 0x12}
	f, err := dec.Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	wire[0] = 0 // mutate network buffer after decoding
	if f.Samples[0] != 0x1234 {
		t.Fatalf("frame was aliased to input buffer: %v", f.Samples)
	}
	out, err := EncodePCM16LE(f)
	if err != nil {
		t.Fatal(err)
	}
	out[0] = 0 // mutate encoded output
	if f.Samples[0] != 0x1234 {
		t.Fatal("encoded output changed source frame")
	}
}

func TestPCM16FrameValidation(t *testing.T) {
	for _, f := range []AudioFrame{
		{SampleRate: 0, Channels: 1, Samples: []int16{1}},
		{SampleRate: 8000, Channels: 0, Samples: []int16{1}},
		{SampleRate: 8000, Channels: 1, Timestamp: -time.Second, Samples: []int16{1}},
		{SampleRate: 8000, Channels: 2, Samples: []int16{1}},
	} {
		if err := f.Validate(); err == nil {
			t.Fatalf("expected error for %+v", f)
		}
	}
}
