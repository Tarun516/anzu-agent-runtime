package echo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audio"
)

// ShadowConfig controls the bounded, in-process audio pipeline used to verify
// that call audio can be copied and converted without affecting socket echo.
type ShadowConfig struct {
	MaxFrames    int
	MaxBuffered  time.Duration
	DrainTimeout time.Duration
}

// NewShadowHandler creates an echo handler with a discard-only 16 kHz shadow
// consumer. It is useful for exercising the media pipeline without sending
// caller audio to an external service.
func NewShadowHandler(cfg ShadowConfig) (func(net.Conn), error) {
	if cfg.DrainTimeout <= 0 {
		return nil, errors.New("shadow drain timeout must be positive")
	}
	prototype, err := newShadowPipeline(cfg)
	if err != nil {
		return nil, err
	}
	prototype.Abort()
	return func(conn net.Conn) {
		handleConnection(conn, func(string) (audioObserver, error) {
			return newShadowSession(cfg)
		})
	}, nil
}

func newShadowPipeline(cfg ShadowConfig) (*audio.InputPipeline, error) {
	pipe, err := audio.NewInputPipeline(audio.InputPipelineConfig{
		SourceRate: 8000, TargetRate: 16000, Channels: 1, ChunkSamples: 320,
		Queue: audio.QueueConfig{
			SampleRate: 16000, Channels: 1, MaxFrames: cfg.MaxFrames,
			MaxBuffered: cfg.MaxBuffered, Overflow: audio.OverflowReject,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("shadow input pipeline configuration: %w", err)
	}
	return pipe, nil
}

type shadowSession struct {
	pipe         *audio.InputPipeline
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	drainTimeout time.Duration

	mu      sync.Mutex
	failure error
}

func newShadowSession(cfg ShadowConfig) (*shadowSession, error) {
	pipe, err := newShadowPipeline(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &shadowSession{
		pipe: pipe, ctx: ctx, cancel: cancel, done: make(chan struct{}),
		drainTimeout: cfg.DrainTimeout,
	}
	go s.consume()
	return s, nil
}

func (s *shadowSession) FeedAudio(payload []byte) error {
	if err := s.pipe.FeedPCM16LE(payload); err != nil {
		s.recordFailure(err)
		return err
	}
	return nil
}

func (s *shadowSession) End(normal bool) {
	if normal {
		if err := s.pipe.Finish(); err != nil {
			s.recordFailure(err)
		}
	} else {
		s.pipe.Abort()
	}
	timer := time.NewTimer(s.drainTimeout)
	defer timer.Stop()
	select {
	case <-s.done:
	case <-timer.C:
		s.recordFailure(errors.New("shadow drain deadline exceeded"))
	}
	s.cancel()
}

func (s *shadowSession) consume() {
	defer close(s.done)
	for {
		_, err := s.pipe.Queue().Pop(s.ctx)
		if err == nil {
			continue
		}
		if !errors.Is(err, audio.ErrQueueClosed) && !errors.Is(err, context.Canceled) {
			s.recordFailure(err)
		}
		return
	}
}

func (s *shadowSession) recordFailure(err error) {
	s.mu.Lock()
	if s.failure == nil {
		s.failure = err
	}
	s.mu.Unlock()
}
