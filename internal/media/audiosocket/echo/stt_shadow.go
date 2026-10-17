package echo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audio"
	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt"
)

// STTShadowConfig enables REAL speech recognition as a call-scoped observer.
// The existing AudioSocket echo remains the ONLY writer to the telephone.
// LogText is opt-in because transcribed speech is private user data.
type STTShadowConfig struct {
	Provider      stt.Transcriber
	Language      string
	QueueFrames   int
	QueueDuration time.Duration
	DrainTimeout  time.Duration
	FinishTimeout time.Duration
	LogText       bool
}

// NewSTTShadowHandler returns an opt-in Asterisk handler. It is independent
// of any particular STT provider and never writes converted audio to Asterisk.
func NewSTTShadowHandler(cfg STTShadowConfig) (func(net.Conn), error) {
	if cfg.Provider == nil {
		return nil, errors.New("STT provider is required")
	}
	if strings.TrimSpace(cfg.Language) == "" {
		return nil, errors.New("STT language is required")
	}
	if cfg.DrainTimeout <= 0 || cfg.FinishTimeout <= 0 || cfg.FinishTimeout > cfg.DrainTimeout {
		return nil, errors.New("STT finish and drain timeouts must be positive; finish must not exceed drain")
	}
	prototype, err := audio.NewInputPipeline(sttPipelineConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("STT input pipeline configuration: %w", err)
	}
	prototype.Abort()
	return func(conn net.Conn) {
		handleConnection(conn, func(id string) (audioObserver, error) { return newSTTShadowSession(id, cfg) })
	}, nil
}

func sttPipelineConfig(cfg STTShadowConfig) audio.InputPipelineConfig {
	return audio.InputPipelineConfig{
		SourceRate: 8000, TargetRate: 16000, Channels: 1, ChunkSamples: 320,
		Queue: audio.QueueConfig{
			SampleRate: 16000, Channels: 1, MaxFrames: cfg.QueueFrames,
			MaxBuffered: cfg.QueueDuration, Overflow: audio.OverflowReject,
		},
	}
}

type sttShadowSummary struct {
	complete    bool
	audioFrames uint64
	partials    uint64
	finals      uint64
	stats       audio.QueueStats
	err         error
}

// One producer: AudioSocket handler calls FeedAudio/End serially.
// The STT writer and event reader run independently; shared counters and
// failure state are mutex-protected. Never invoke InputPipeline methods
// from those background goroutines: the pipeline is single-producer-owned.
type sttShadowSession struct {
	id         string
	cfg        STTShadowConfig
	pipe       *audio.InputPipeline
	stt        stt.TranscriptionSession
	ctx        context.Context
	cancel     context.CancelFunc
	writerDone chan struct{}
	readerDone chan struct{}

	mu          sync.Mutex
	failure     error
	finishing   bool
	audioFrames uint64
	partials    uint64
	finals      uint64
	summary     sttShadowSummary
}

func newSTTShadowSession(id string, cfg STTShadowConfig) (*sttShadowSession, error) {
	pipe, err := audio.NewInputPipeline(sttPipelineConfig(cfg))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	session, err := cfg.Provider.Start(ctx, stt.SpeechConfig{
		CallID: id, Language: cfg.Language, SampleRate: 16000,
		Channels: 1, Encoding: stt.EncodingPCM16,
	})
	if err != nil {
		cancel()
		pipe.Abort()
		return nil, fmt.Errorf("STT provider start: %w", err)
	}
	if session == nil || session.Events() == nil {
		if session != nil {
			session.Abort()
		}
		cancel()
		pipe.Abort()
		return nil, errors.New("STT provider returned nil session or events channel")
	}
	s := &sttShadowSession{id: id, cfg: cfg, pipe: pipe, stt: session,
		ctx: ctx, cancel: cancel, writerDone: make(chan struct{}), readerDone: make(chan struct{})}
	go s.writeAudio()
	go s.readEvents()
	return s, nil
}

func (s *sttShadowSession) FeedAudio(payload []byte) error {
	if err := s.getFailure(); err != nil {
		return err
	}
	if err := s.pipe.FeedPCM16LE(payload); err != nil {
		s.fail(fmt.Errorf("input audio incomplete: %w", err))
		return err
	}
	return nil
}

func (s *sttShadowSession) getFailure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failure
}

// fail is safe from any goroutine; the first terminal error is retained.
// No raw PCM or transcript content is incorporated into our error text.
func (s *sttShadowSession) fail(err error) {
	s.mu.Lock()
	if s.failure == nil {
		s.failure = err
	}
	s.mu.Unlock()
	s.cancel()
	s.stt.Abort()
}

func (s *sttShadowSession) writeAudio() {
	defer close(s.writerDone)
	for {
		item, err := s.pipe.Queue().Pop(s.ctx)
		if err != nil {
			if errors.Is(err, audio.ErrQueueClosed) && s.ctx.Err() == nil && s.getFailure() == nil {
				// Normal end of the decoded audio stream: the provider must
				// finalize its pending recognition output before we return.
				s.mu.Lock()
				s.finishing = true
				s.mu.Unlock()
				finishCtx, cancel := context.WithTimeout(s.ctx, s.cfg.FinishTimeout)
				endErr := s.stt.Finish(finishCtx)
				cancel()
				if endErr != nil {
					s.fail(fmt.Errorf("STT finish: %w", endErr))
				}
			} else if !errors.Is(err, context.Canceled) && !errors.Is(err, audio.ErrQueueClosed) {
				s.fail(fmt.Errorf("STT queue read: %w", err))
			}
			return
		}
		if item.DroppedBefore != 0 {
			s.fail(fmt.Errorf("discontinuous STT audio: %d frames evicted", item.DroppedBefore))
			return
		}
		if err := s.stt.WriteAudio(s.ctx, item.Frame); err != nil {
			s.fail(fmt.Errorf("STT audio write: %w", err))
			return
		}
		s.mu.Lock()
		s.audioFrames++
		s.mu.Unlock()
	}
}

func (s *sttShadowSession) readEvents() {
	defer close(s.readerDone)
	for event := range s.stt.Events() {
		if event.CallID != s.id {
			s.fail(errors.New("STT event call ID mismatch"))
			return
		}
		switch event.Kind {
		case stt.EventPartial:
			s.mu.Lock()
			s.partials++
			s.mu.Unlock()
		case stt.EventFinal:
			s.mu.Lock()
			s.finals++
			s.mu.Unlock()
		case stt.EventError:
			s.fail(fmt.Errorf("STT provider error: %w", event.Err))
			return
		default:
			s.fail(errors.New("STT event kind invalid"))
			return
		}
		if s.cfg.LogText && (event.Kind == stt.EventPartial || event.Kind == stt.EventFinal) {
			// Explicit lab opt-in only. These contain real caller speech.
			log.Printf("stt transcript: uuid=%s segment=%s final=%t text=%q",
				s.id, event.SegmentID, event.Kind == stt.EventFinal, event.Text)
		}
	}
	s.mu.Lock()
	finishing := s.finishing
	failed := s.failure != nil
	s.mu.Unlock()
	if !finishing && !failed && s.ctx.Err() == nil {
		s.fail(errors.New("STT event stream ended before audio finished"))
	}
}

// End is always called by the AudioSocket handler exactly once. It has a
// strict total deadline. A broken or canceled STT connection must not hold a
// telephone handler indefinitely. The phone echo itself never depends on STT.
func (s *sttShadowSession) End(normal bool) {
	if normal && s.getFailure() == nil {
		if err := s.pipe.Finish(); err != nil {
			s.fail(fmt.Errorf("input pipeline finish: %w", err))
		}
	} else {
		s.pipe.Abort()
		s.cancel()
		s.stt.Abort()
	}

	// Use one shared deadline for writer+reader; they must both terminate.
	timer := time.NewTimer(s.cfg.DrainTimeout)
	defer timer.Stop()
	drained := true
	select {
	case <-s.writerDone:
	case <-timer.C:
		drained = false
	}
	if drained {
		select {
		case <-s.readerDone:
		case <-timer.C:
			drained = false
		}
	}
	if !drained {
		s.fail(errors.New("STT session drain deadline exceeded"))
	}
	// Guarantee cleanup for fast, cooperative sessions. A provider that ignores
	// cancellation is recorded as incomplete; never block on it indefinitely.
	s.cancel()
	s.stt.Abort()
	stats := s.pipe.Queue().Stats()
	s.mu.Lock()
	cause := s.failure
	frames, partials, finals := s.audioFrames, s.partials, s.finals
	s.mu.Unlock()
	complete := normal && drained && cause == nil && stats.Rejected == 0 && stats.Dropped == 0 && stats.Depth == 0 && stats.Accepted == stats.Delivered && frames == stats.Delivered
	s.summary = sttShadowSummary{complete: complete, audioFrames: frames, partials: partials, finals: finals, stats: stats, err: cause}
	log.Printf("stt call summary: uuid=%s complete=%t frames=%d partials=%d finals=%d accepted=%d delivered=%d rejected=%d dropped=%d queued=%d error=%v",
		s.id, complete, frames, partials, finals, stats.Accepted, stats.Delivered, stats.Rejected, stats.Dropped, stats.Depth, cause)
}
