package audio

import (
	"errors"
	"fmt"
)

// RateConverter converts a continuous PCM16 stream between 8 kHz and 16 kHz.
// It also supports a copied passthrough at either rate.
//
// This first implementation prioritizes an understandable, sample-accurate
// streaming clock: 8->16 kHz uses linear interpolation; 16->8 kHz averages
// each adjacent pair. Pair averaging is only a weak anti-aliasing filter;
// do not use this as a production-quality general-purpose resampler.
//
// One converter belongs to one call/stream and one goroutine. Feed and Finish
// are NOT safe for concurrent calls. Output samples never alias input frames.
// An output frame is returned only when at least one output sample is ready.
// Finish must be called once at end-of-stream to emit a trailing sample/pair.
type RateConverter struct {
	sourceRate int
	targetRate int
	channels   int

	nextInputSeq uint64
	inputOffset  uint64 // cumulative input samples PER CHANNEL
	nextOutSeq   uint64
	outputOffset uint64 // cumulative output samples PER CHANNEL

	previous []int16 // one pending sample per channel for lookahead/pairing
	finished bool
}

// NewRateConverter sets the exact supported conversion and channel layout.
func NewRateConverter(sourceRate, targetRate, channels int) (*RateConverter, error) {
	if channels < 1 || channels > 2 {
		return nil, fmt.Errorf("only mono/stereo are supported, got %d channels", channels)
	}
	if !((sourceRate == 8000 && targetRate == 16000) ||
		(sourceRate == 16000 && targetRate == 8000) ||
		(sourceRate == 8000 && targetRate == 8000) ||
		(sourceRate == 16000 && targetRate == 16000)) {
		return nil, fmt.Errorf("unsupported conversion: %d Hz to %d Hz", sourceRate, targetRate)
	}
	return &RateConverter{sourceRate: sourceRate, targetRate: targetRate, channels: channels}, nil
}

// Feed consumes one contiguous input frame and returns up to one output frame.
// If only a single unfinished source sample is available, ready will be false.
// Invalid input does not advance sequence, stream time or pending samples.
func (r *RateConverter) Feed(f AudioFrame) (out AudioFrame, ready bool, err error) {
	if r == nil {
		return AudioFrame{}, false, errors.New("rate converter is nil")
	}
	if r.finished {
		return AudioFrame{}, false, errors.New("rate converter already finished")
	}
	if err := f.Validate(); err != nil {
		return AudioFrame{}, false, err
	}
	if f.SampleRate != r.sourceRate || f.Channels != r.channels {
		return AudioFrame{}, false, fmt.Errorf("input format mismatch: %d Hz/%d channels", f.SampleRate, f.Channels)
	}
	if f.Sequence != r.nextInputSeq {
		return AudioFrame{}, false, fmt.Errorf("input sequence discontinuity: got %d, want %d", f.Sequence, r.nextInputSeq)
	}
	if expected := streamTimestamp(r.inputOffset, r.sourceRate); f.Timestamp != expected {
		return AudioFrame{}, false, fmt.Errorf("input timestamp discontinuity: got %v, want %v", f.Timestamp, expected)
	}

	// All validation happens before we mutate the converter's stream state.
	var samples []int16
	switch {
	case r.sourceRate == r.targetRate:
		// Copy passthrough to avoid retaining the caller's mutable slice.
		samples = append([]int16(nil), f.Samples...)

	case r.sourceRate == 8000 && r.targetRate == 16000:
		// Each adjacent pair yields [left, midpoint]. We retain the last
		// sample to interpolate across successive Feed calls without a seam.
		samples = make([]int16, 0, (len(f.Samples)+r.channels)*2)
		for i := 0; i < len(f.Samples); i += r.channels {
			current := f.Samples[i : i+r.channels]
			if r.previous != nil {
				samples = append(samples, r.previous...)
				for ch := 0; ch < r.channels; ch++ {
					samples = append(samples, averagePCM16(r.previous[ch], current[ch]))
				}
			}
			r.previous = append(r.previous[:0], current...)
		}

	case r.sourceRate == 16000 && r.targetRate == 8000:
		// Combine consecutive 16-kHz samples into one 8-kHz sample.
		// This mild smoothing is NOT a sufficient production anti-alias filter.
		samples = make([]int16, 0, (len(f.Samples)+r.channels)/2)
		for i := 0; i < len(f.Samples); i += r.channels {
			current := f.Samples[i : i+r.channels]
			if r.previous == nil {
				r.previous = append(make([]int16, 0, r.channels), current...)
				continue
			}
			for ch := 0; ch < r.channels; ch++ {
				samples = append(samples, averagePCM16(r.previous[ch], current[ch]))
			}
			r.previous = nil
		}
	}

	r.nextInputSeq++
	r.inputOffset += uint64(f.SamplesPerChannel())
	if len(samples) == 0 {
		return AudioFrame{}, false, nil
	}
	return r.emit(samples), true, nil
}

// Finish emits the remaining edge/pair. For 8->16 kHz it repeats the last
// source sample, producing exactly 2*N samples for N input samples. For
// 16->8 kHz, an unmatched last source sample is copied (ceil(N/2) output).
// That odd tail may extend the nominal stream by 62.5 microseconds at 16 kHz.
// Call Finish at normal end-of-stream, not after a temporary empty TCP read.
func (r *RateConverter) Finish() (AudioFrame, bool, error) {
	if r == nil {
		return AudioFrame{}, false, errors.New("rate converter is nil")
	}
	if r.finished {
		return AudioFrame{}, false, nil
	}
	r.finished = true
	if r.previous == nil {
		return AudioFrame{}, false, nil
	}

	var samples []int16
	if r.sourceRate == 8000 && r.targetRate == 16000 {
		samples = append(make([]int16, 0, 2*r.channels), r.previous...)
		samples = append(samples, r.previous...)
	} else {
		samples = append([]int16(nil), r.previous...)
	}
	r.previous = nil
	return r.emit(samples), true, nil
}

// emit constructs an independently owned output frame and advances its clock.
func (r *RateConverter) emit(samples []int16) AudioFrame {
	f := AudioFrame{
		Sequence:   r.nextOutSeq,
		Timestamp:  streamTimestamp(r.outputOffset, r.targetRate),
		SampleRate: r.targetRate,
		Channels:   r.channels,
		Samples:    samples,
	}
	r.nextOutSeq++
	r.outputOffset += uint64(len(samples) / r.channels)
	return f
}

// averagePCM16 widens before addition so -32768 + -32768 cannot overflow.
func averagePCM16(a, b int16) int16 {
	return int16((int32(a) + int32(b)) / 2)
}

// PCMChunker aggregates or splits a continuous stream into fixed-size
// AudioFrames (chunkSamples PER CHANNEL). A source frame need not align with a
// chunk boundary. Only an incomplete tail is buffered; complete chunks are
// returned immediately, not retained in a growing queue.
//
// Each chunker belongs to one call and one goroutine. Output frame Samples
// have independent backing arrays, even if inputs are overwritten/reused.
type PCMChunker struct {
	sampleRate   int
	channels     int
	chunkSamples int

	nextInputSeq uint64
	inputOffset  uint64
	nextOutSeq   uint64
	outputOffset uint64
	pending      []int16
	finished     bool
}

// NewPCMChunker configures a fixed chunk size of up to one second of audio.
// The bound prevents an accidentally huge incomplete-tail allocation.
func NewPCMChunker(sampleRate, channels, chunkSamples int) (*PCMChunker, error) {
	if sampleRate < 1 || sampleRate > 192000 || channels < 1 || channels > 2 {
		return nil, errors.New("invalid PCM chunker sample rate or channel count")
	}
	if chunkSamples < 1 || chunkSamples > sampleRate {
		return nil, fmt.Errorf("chunk size must be 1..%d samples per channel", sampleRate)
	}
	return &PCMChunker{sampleRate: sampleRate, channels: channels, chunkSamples: chunkSamples}, nil
}

// Push consumes a contiguous input frame, emitting every complete chunk.
// Invalid input never changes the chunker's state. The returned frame slices
// own their samples; consumers may retain them after subsequent Push calls.
func (c *PCMChunker) Push(f AudioFrame) ([]AudioFrame, error) {
	if c == nil {
		return nil, errors.New("PCM chunker is nil")
	}
	if c.finished {
		return nil, errors.New("PCM chunker already finished")
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	if f.SampleRate != c.sampleRate || f.Channels != c.channels {
		return nil, fmt.Errorf("chunker format mismatch: %d Hz/%d channels", f.SampleRate, f.Channels)
	}
	if f.Sequence != c.nextInputSeq {
		return nil, fmt.Errorf("chunker sequence discontinuity: got %d, want %d", f.Sequence, c.nextInputSeq)
	}
	if expected := streamTimestamp(c.inputOffset, c.sampleRate); f.Timestamp != expected {
		return nil, fmt.Errorf("chunker timestamp discontinuity: got %v, want %v", f.Timestamp, expected)
	}

	// Only retain up to one partial chunk; emit full chunks eagerly.
	chunkLength := c.chunkSamples * c.channels
	var out []AudioFrame
	for offset := 0; offset < len(f.Samples); {
		needed := chunkLength - len(c.pending)
		available := len(f.Samples) - offset
		if needed > available {
			needed = available
		}
		c.pending = append(c.pending, f.Samples[offset:offset+needed]...)
		offset += needed

		if len(c.pending) == chunkLength {
			out = append(out, c.emit(c.pending))
			c.pending = nil // ownership transferred to output; NEVER reuse
		}
	}
	c.nextInputSeq++
	c.inputOffset += uint64(f.SamplesPerChannel())
	return out, nil
}

// Finish returns a smaller trailing chunk (if any) without injecting silence.
// The final chunk's Duration reflects its actual number of samples.
func (c *PCMChunker) Finish() (AudioFrame, bool, error) {
	if c == nil {
		return AudioFrame{}, false, errors.New("PCM chunker is nil")
	}
	if c.finished {
		return AudioFrame{}, false, nil
	}
	c.finished = true
	if len(c.pending) == 0 {
		return AudioFrame{}, false, nil
	}
	f := c.emit(c.pending)
	c.pending = nil
	return f, true, nil
}

func (c *PCMChunker) emit(samples []int16) AudioFrame {
	f := AudioFrame{
		Sequence:   c.nextOutSeq,
		Timestamp:  streamTimestamp(c.outputOffset, c.sampleRate),
		SampleRate: c.sampleRate,
		Channels:   c.channels,
		Samples:    samples,
	}
	c.nextOutSeq++
	c.outputOffset += uint64(len(samples) / c.channels)
	return f
}
