// Package stt defines the provider-neutral speech-to-text contract.
// It does not depend on AudioSocket, Asterisk, or a transcription provider.
package stt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audio"
)

// Encoding identifies the sample encoding of the incoming AudioFrame.
// AudioFrame.Samples currently represents signed 16-bit PCM samples.
type Encoding string

const EncodingPCM16 Encoding = "pcm_s16le"

// SpeechConfig is validated before opening a transcription session.
// A provider adapter may enforce further restrictions of its own.
type SpeechConfig struct {
	CallID     string // correlation ID; do not put credentials or PII here
	Language   string // e.g. "en", "en-IN"; provider support varies
	SampleRate int    // samples per second, per channel
	Channels   int
	Encoding   Encoding
}

func (c SpeechConfig) Validate() error {
	if strings.TrimSpace(c.CallID) == "" {
		return errors.New("call ID is required")
	}
	if strings.TrimSpace(c.Language) == "" {
		return errors.New("language is required")
	}
	if c.Encoding != EncodingPCM16 {
		return fmt.Errorf("unsupported encoding %q; want %q", c.Encoding, EncodingPCM16)
	}
	// The initial Anzu streaming route is mono. Concrete providers may further
	// restrict sample rates; CP4.2 will map these to vendor-specific formats.
	if c.Channels != 1 {
		return fmt.Errorf("STT currently expects mono audio, got %d channels", c.Channels)
	}
	switch c.SampleRate {
	case 8000, 16000, 24000, 48000:
		return nil
	default:
		return fmt.Errorf("unsupported sample rate %d", c.SampleRate)
	}
}

// EventKind describes the lifecycle of ONE provider transcript segment.
// Final means the provider finalized a segment; it does NOT certify that the
// speaker has finished their conversational turn (that belongs to CP5).
type EventKind uint8

const (
	EventPartial EventKind = iota + 1
	EventFinal
	EventError
)

// TranscriptEvent represents a provider hypothesis or terminal failure.
// Sequence is monotonic per session and independent of audio frame Sequence.
// Segments may emit several partial hypotheses before a final hypothesis.
// Start/End are offsets within the caller's audio stream, not wall-clock time.
type TranscriptEvent struct {
	Kind      EventKind
	CallID    string
	Sequence  uint64
	SegmentID string
	Text      string
	Start     time.Duration
	End       time.Duration
	Err       error // only populated for EventError
}

func (e TranscriptEvent) Validate() error {
	if strings.TrimSpace(e.CallID) == "" {
		return errors.New("transcript event call ID is required")
	}
	switch e.Kind {
	case EventPartial, EventFinal:
		if strings.TrimSpace(e.SegmentID) == "" || strings.TrimSpace(e.Text) == "" {
			return errors.New("transcript segment ID and text are required")
		}
		if e.Start < 0 || e.End < e.Start {
			return errors.New("invalid transcript audio interval")
		}
		if e.Err != nil {
			return errors.New("text event must not carry an error")
		}
	case EventError:
		if e.Err == nil {
			return errors.New("error event must carry an error")
		}
	default:
		return fmt.Errorf("unknown transcript event kind: %d", e.Kind)
	}
	return nil
}

var (
	ErrSessionClosed = errors.New("STT session is closed")
	ErrSessionFailed = errors.New("STT session failed")
	ErrDiscontinuity = errors.New("STT audio frame discontinuity")
)

// Transcriber is a factory. One Start call creates ONE isolated call session.
type Transcriber interface {
	Start(ctx context.Context, cfg SpeechConfig) (TranscriptionSession, error)
}

// TranscriptionSession is deliberately small and provider-independent.
//
// WriteAudio must be called by one ordered producer. Errors are explicit:
// callers must not silently treat a failed transcription as complete.
// Events can be read concurrently while audio is written; every emitted event
// is delivered in channel order. Consumers MUST read Events in real providers
// to avoid eventual bounded-buffer backpressure.
// Finish signals NORMAL end-of-input. It may cause final transcript segments;
// callers still need to drain Events until the channel closes.
// Abort stops immediately without requesting finalization; it is idempotent.
// Every session must eventually be Finished or Aborted, even on failures.
type TranscriptionSession interface {
	WriteAudio(ctx context.Context, frame audio.AudioFrame) error
	Events() <-chan TranscriptEvent
	Finish(ctx context.Context) error
	Abort()
}
