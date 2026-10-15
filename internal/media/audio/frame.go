// Package audio defines transport-independent audio data and timing.
package audio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// AudioFrame contains signed 16-bit PCM samples and their stream metadata.
// Samples are interleaved if Channels > 1. Timestamp is the start offset
// within ONE call's audio stream, not wall-clock time.
//
// Ownership: Decode copies its input into Samples. Treat Samples as owned by
// the frame; do not share and mutate it across goroutines without copying.
type AudioFrame struct {
	Sequence   uint64
	Timestamp  time.Duration
	SampleRate int
	Channels   int
	Samples    []int16
}

// Validate checks the invariants required to interpret sample timing.
func (f AudioFrame) Validate() error {
	if f.SampleRate <= 0 {
		return errors.New("sample rate must be positive")
	}
	if f.Channels <= 0 {
		return errors.New("channel count must be positive")
	}
	if f.Timestamp < 0 {
		return errors.New("timestamp cannot be negative")
	}
	if len(f.Samples) == 0 || len(f.Samples)%f.Channels != 0 {
		return fmt.Errorf("sample count %d is invalid for %d channels", len(f.Samples), f.Channels)
	}
	return nil
}

// SamplesPerChannel returns the number of time samples in a valid frame.
func (f AudioFrame) SamplesPerChannel() int {
	if f.Channels <= 0 {
		return 0
	}
	return len(f.Samples) / f.Channels
}

// Duration returns the frame's playtime, assuming valid frame metadata.
// It does not assume incoming AudioSocket frames are always 20 ms long.
func (f AudioFrame) Duration() time.Duration {
	if f.SampleRate <= 0 || f.Channels <= 0 {
		return 0
	}
	return time.Duration(f.SamplesPerChannel()) * time.Second / time.Duration(f.SampleRate)
}

// PCM16Decoder translates wire-format little-endian PCM into AudioFrames.
// Create one decoder per call; it is NOT safe for concurrent Decode calls.
// Sample-based timekeeping avoids accumulating per-packet rounding errors.
type PCM16Decoder struct {
	sampleRate   int
	channels     int
	nextSeq      uint64
	sampleOffset uint64 // samples PER CHANNEL since the stream began
}

// NewPCM16Decoder configures the rate and channel count of an input stream.
func NewPCM16Decoder(sampleRate, channels int) (*PCM16Decoder, error) {
	if sampleRate <= 0 || channels <= 0 {
		return nil, errors.New("sample rate and channels must be positive")
	}
	return &PCM16Decoder{sampleRate: sampleRate, channels: channels}, nil
}

// Decode converts one PCM16LE payload into an owned, timestamped frame.
// A malformed payload does not advance sequence or stream time.
func (d *PCM16Decoder) Decode(payload []byte) (AudioFrame, error) {
	if d == nil {
		return AudioFrame{}, errors.New("PCM16 decoder is nil")
	}
	if len(payload) == 0 || len(payload)%2 != 0 {
		return AudioFrame{}, fmt.Errorf("PCM16 payload has invalid byte length %d", len(payload))
	}
	count := len(payload) / 2
	if count%d.channels != 0 {
		return AudioFrame{}, fmt.Errorf("PCM16 sample count %d is not divisible by %d channels", count, d.channels)
	}

	// Always copy bytes into signed samples: network buffers may be reused.
	samples := make([]int16, count)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(payload[2*i:]))
	}

	// Compute timestamp from cumulative samples, NOT from time.Now().
	f := AudioFrame{
		Sequence:   d.nextSeq,
		Timestamp:  streamTimestamp(d.sampleOffset, d.sampleRate),
		SampleRate: d.sampleRate,
		Channels:   d.channels,
		Samples:    samples,
	}
	d.nextSeq++
	d.sampleOffset += uint64(count / d.channels)
	return f, nil
}

// EncodePCM16LE returns a NEW little-endian byte buffer for an audio frame.
// This is the inverse conversion for PCM16LE transports such as AudioSocket.
func EncodePCM16LE(f AudioFrame) ([]byte, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	out := make([]byte, len(f.Samples)*2)
	for i, sample := range f.Samples {
		binary.LittleEndian.PutUint16(out[2*i:], uint16(sample))
	}
	return out, nil
}

// streamTimestamp derives elapsed stream time from the cumulative sample count.
// Splitting seconds and remainder avoids overflow from samples * time.Second.
func streamTimestamp(sampleOffset uint64, sampleRate int) time.Duration {
	rate := uint64(sampleRate)
	seconds := sampleOffset / rate
	fractionalSamples := sampleOffset % rate
	return time.Duration(seconds)*time.Second +
		time.Duration(fractionalSamples)*time.Second/time.Duration(rate)
}
