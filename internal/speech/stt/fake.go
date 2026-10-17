package stt

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audio"
)

// FakeEmission is one deterministic provider event for testing.
// With OnFinish=false, emit after exactly AfterFrames successfully accepted
// audio frames (>=1). With OnFinish=true, emit during Finish in script order.
// Set Event.Kind, SegmentID/Text/Start/End or Err. The fake assigns CallID
// and a monotonic transcript event Sequence for each new session.
type FakeEmission struct {
	AfterFrames uint64
	OnFinish    bool
	Event       TranscriptEvent
}

// FakeTranscriber is a deterministic test double, NOT speech recognition.
// It never derives text from audio samples or talks to a network provider.
// The script is immutable; each Start gets separate counters and event buffers.
type FakeTranscriber struct {
	script []FakeEmission
}

var _ Transcriber = (*FakeTranscriber)(nil)
var _ TranscriptionSession = (*FakeSession)(nil)

// NewFakeTranscriber validates a script. Frame-triggered emissions must be
// ordered by threshold, and Finish emissions must follow them. All events
// fit in a bounded channel sized exactly to the script, so tests do not need
// a reader to avoid blocking fake WriteAudio while a mutex is held.
func NewFakeTranscriber(script []FakeEmission) (*FakeTranscriber, error) {
	if len(script) > 256 {
		return nil, errors.New("fake STT script is limited to 256 events")
	}
	copyScript := append([]FakeEmission(nil), script...)
	var last uint64
	finishing := false
	for i, e := range copyScript {
		if err := validateEventPayload(e.Event); err != nil {
			return nil, fmt.Errorf("script event %d: %w", i, err)
		}
		if e.OnFinish {
			finishing = true
			if e.AfterFrames != 0 {
				return nil, fmt.Errorf("script event %d: Finish event must use AfterFrames=0", i)
			}
		} else {
			if finishing || e.AfterFrames == 0 || e.AfterFrames < last {
				return nil, fmt.Errorf("script event %d: frame triggers must be ordered, positive, and before Finish events", i)
			}
			last = e.AfterFrames
		}
	}
	return &FakeTranscriber{script: copyScript}, nil
}

func validateEventPayload(e TranscriptEvent) error {
	e.CallID = "validated-on-session-start"
	return e.Validate()
}

func (p *FakeTranscriber) Start(ctx context.Context, cfg SpeechConfig) (TranscriptionSession, error) {
	if p == nil {
		return nil, errors.New("fake transcriber is nil")
	}
	if ctx == nil {
		return nil, errors.New("session context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	capacity := len(p.script)
	if capacity == 0 {
		capacity = 1
	}
	s := &FakeSession{cfg: cfg, parent: ctx, script: p.script, events: make(chan TranscriptEvent, capacity)}
	// No goroutine is retained for lifecycle monitoring. Go arranges for Abort
	// to run only if the parent context becomes canceled.
	s.mu.Lock()
	s.stopWatch = context.AfterFunc(ctx, s.Abort)
	s.mu.Unlock()
	// Cancellation can race the Start/AfterFunc registration. Abort guarantees
	// that a canceled Start does not hand out a usable session.
	if err := ctx.Err(); err != nil {
		s.Abort()
		return nil, err
	}
	return s, nil
}

type fakeState uint8

const (
	fakeOpen fakeState = iota
	fakeFinished
	fakeAborted
	fakeFailed
)

// FakeSession serializes its mutable state, including counters and event sends.
// It supports safe concurrent inspection/Abort, but upstream media should have
// a single ordered WriteAudio producer, as in Anzu's per-call pipeline.
type FakeSession struct {
	mu        sync.Mutex
	cfg       SpeechConfig
	parent    context.Context
	script    []FakeEmission
	events    chan TranscriptEvent
	stopWatch func() bool
	state     fakeState
	failure   error
	cursor    int
	nextEvent uint64
	frames    uint64
	samples   uint64 // per-channel samples; stream-relative audio clock
}

func (s *FakeSession) Events() <-chan TranscriptEvent {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *FakeSession) WriteAudio(ctx context.Context, f audio.AudioFrame) error {
	if s == nil {
		return errors.New("fake session is nil")
	}
	if ctx == nil {
		return errors.New("write context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.parent.Err(); err != nil {
		s.Abort()
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.stateErrorLocked(); err != nil {
		return err
	}
	if err := f.Validate(); err != nil {
		return fmt.Errorf("invalid STT audio: %w", err)
	}
	if f.SampleRate != s.cfg.SampleRate || f.Channels != s.cfg.Channels {
		return fmt.Errorf("STT frame format mismatch: got %dHz/%dch want %dHz/%dch", f.SampleRate, f.Channels, s.cfg.SampleRate, s.cfg.Channels)
	}
	if f.Sequence != s.frames || f.Timestamp != fakeSampleTime(s.samples, s.cfg.SampleRate) {
		return fmt.Errorf("%w: sequence=%d timestamp=%s (want seq=%d timestamp=%s)",
			ErrDiscontinuity, f.Sequence, f.Timestamp, s.frames, fakeSampleTime(s.samples, s.cfg.SampleRate))
	}
	// Commit counters only AFTER validation. The fake does not retain PCM.
	s.frames++
	s.samples += uint64(f.SamplesPerChannel())
	for s.cursor < len(s.script) && !s.script[s.cursor].OnFinish && s.script[s.cursor].AfterFrames == s.frames {
		if s.emitLocked(s.script[s.cursor].Event) {
			s.cursor++
			return nil
		}
		s.cursor++
	}
	return nil
}

// Finish simulates sending an end-of-audio marker to a real provider.
// It delivers any scripted finish events, closes Events and is idempotent.
// Drain Events after Finish to receive all buffered transcript results.
func (s *FakeSession) Finish(ctx context.Context) error {
	if s == nil {
		return errors.New("fake session is nil")
	}
	if ctx == nil {
		return errors.New("finish context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.parent.Err(); err != nil {
		s.Abort()
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == fakeFinished {
		return nil
	}
	if err := s.stateErrorLocked(); err != nil {
		return err
	}
	if s.cursor < len(s.script) && !s.script[s.cursor].OnFinish {
		return fmt.Errorf("cannot finish fake STT: script expects %d audio frames, received %d", s.script[s.cursor].AfterFrames, s.frames)
	}
	for s.cursor < len(s.script) {
		failure := s.emitLocked(s.script[s.cursor].Event)
		s.cursor++
		if failure {
			return s.stateErrorLocked()
		}
	}
	s.state = fakeFinished
	close(s.events)
	s.stopWatchLocked()
	return nil
}

// Abort cancels a session immediately. Already-buffered events remain readable,
// but must not be used as proof that an aborted transcript is complete.
func (s *FakeSession) Abort() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != fakeOpen {
		return
	}
	s.state = fakeAborted
	close(s.events)
	s.stopWatchLocked()
}

// FakeStats allows tests to assert that an invalid frame was not accepted.
type FakeStats struct {
	AcceptedFrames  uint64
	AcceptedSamples uint64
	Finished        bool
	Aborted         bool
	Failed          bool
}

func (s *FakeSession) Stats() FakeStats {
	if s == nil {
		return FakeStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return FakeStats{AcceptedFrames: s.frames, AcceptedSamples: s.samples,
		Finished: s.state == fakeFinished, Aborted: s.state == fakeAborted, Failed: s.state == fakeFailed}
}

// emitLocked returns true if an error event terminates the fake provider.
func (s *FakeSession) emitLocked(event TranscriptEvent) bool {
	event.CallID = s.cfg.CallID
	event.Sequence = s.nextEvent
	s.nextEvent++
	s.events <- event // guaranteed script-sized capacity; never blocks
	if event.Kind == EventError {
		s.failure = event.Err
		s.state = fakeFailed
		close(s.events)
		s.stopWatchLocked()
		return true
	}
	return false
}

func (s *FakeSession) stateErrorLocked() error {
	switch s.state {
	case fakeOpen:
		return nil
	case fakeFailed:
		return errors.Join(ErrSessionFailed, s.failure)
	default:
		return ErrSessionClosed
	}
}

func (s *FakeSession) stopWatchLocked() {
	if s.stopWatch != nil {
		s.stopWatch()
		s.stopWatch = nil
	}
}

// Avoid multiplying total samples by one billion before dividing: that would
// overflow after long calls. Calculate whole seconds and remainder separately.
func fakeSampleTime(samples uint64, sampleRate int) time.Duration {
	r := uint64(sampleRate)
	return time.Duration(samples/r)*time.Second + time.Duration(samples%r)*time.Second/time.Duration(r)
}
