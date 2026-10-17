package echo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audio"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket"
	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt"
)

func shadowTestPCM() []byte {
	return make([]byte, 320) // 20 ms of 8 kHz mono PCM16
}

func shadowTestCall(t *testing.T, handler func(net.Conn), audioFrames int, terminal audiosocket.Frame) {
	t.Helper()
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler(server)
	}()
	t.Cleanup(func() {
		client.Close()
		<-done
	})

	uuid := audiosocket.Frame{Type: audiosocket.TypeUUID, Payload: make([]byte, 16)}
	if err := audiosocket.WriteFrame(client, uuid); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < audioFrames; i++ {
		frame := audiosocket.Frame{Type: audiosocket.TypeAudio, Payload: shadowTestPCM()}
		if err := audiosocket.WriteFrame(client, frame); err != nil {
			t.Fatal(err)
		}
		echoed, err := audiosocket.ReadFrame(client)
		if err != nil {
			t.Fatal(err)
		}
		if echoed.Type != frame.Type || len(echoed.Payload) != len(frame.Payload) {
			t.Fatalf("unexpected echo: %+v", echoed)
		}
	}
	if err := audiosocket.WriteFrame(client, terminal); err != nil {
		t.Fatal(err)
	}
	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not terminate")
	}
}

func sttTestConfig(t *testing.T, script []stt.FakeEmission) STTShadowConfig {
	t.Helper()
	provider, err := stt.NewFakeTranscriber(script)
	if err != nil {
		t.Fatal(err)
	}
	return STTShadowConfig{
		Provider: provider, Language: "en-IN", QueueFrames: 32,
		QueueDuration: time.Second, DrainTimeout: time.Second,
		FinishTimeout: 700 * time.Millisecond,
	}
}

func TestSTTShadowAudioSocketEchoAndFinalEvents(t *testing.T) {
	cfg := sttTestConfig(t, []stt.FakeEmission{
		{AfterFrames: 1, Event: stt.TranscriptEvent{
			Kind: stt.EventPartial, SegmentID: "segment1", Text: "hello", End: 20 * time.Millisecond,
		}},
		{OnFinish: true, Event: stt.TranscriptEvent{
			Kind: stt.EventFinal, SegmentID: "segment1", Text: "hello world", End: 40 * time.Millisecond,
		}},
	})
	var observed *sttShadowSession
	shadowTestCall(t, func(c net.Conn) {
		handleConnection(c, func(id string) (audioObserver, error) {
			var err error
			observed, err = newSTTShadowSession(id, cfg)
			return observed, err
		})
	}, 2, audiosocket.Frame{Type: audiosocket.TypeHangup})
	if observed == nil {
		t.Fatal("observer not constructed")
	}
	got := observed.summary
	if !got.complete || got.err != nil || got.audioFrames != 2 || got.partials != 1 || got.finals != 1 || got.stats.Accepted != 2 || got.stats.Delivered != 2 {
		t.Fatalf("unexpected transcription shadow summary: %+v", got)
	}
}

func TestSTTShadowTruncatedWireAborts(t *testing.T) {
	cfg := sttTestConfig(t, nil)
	var observed *sttShadowSession
	shadowTestCall(t, func(c net.Conn) {
		handleConnection(c, func(id string) (audioObserver, error) {
			var err error
			observed, err = newSTTShadowSession(id, cfg)
			return observed, err
		})
	}, 2, audiosocket.Frame{Type: audiosocket.TypeError})
	if observed == nil || observed.summary.complete {
		t.Fatalf("broken wire must abort: %+v", observed)
	}
}

func TestSTTShadowProviderErrorIsIncomplete(t *testing.T) {
	cfg := sttTestConfig(t, []stt.FakeEmission{{AfterFrames: 1, Event: stt.TranscriptEvent{Kind: stt.EventError, Err: errors.New("synthetic recognizer failure")}}})
	var observed *sttShadowSession
	shadowTestCall(t, func(c net.Conn) {
		handleConnection(c, func(id string) (audioObserver, error) {
			var err error
			observed, err = newSTTShadowSession(id, cfg)
			return observed, err
		})
	}, 3, audiosocket.Frame{Type: audiosocket.TypeHangup})
	if observed == nil || observed.summary.complete || observed.summary.err == nil {
		t.Fatalf("must report incomplete on provider error: %+v", observed)
	}
}

func TestSTTShadowRejectsInvalidConfig(t *testing.T) {
	cfg := sttTestConfig(t, nil)
	cfg.Provider = nil
	if _, err := NewSTTShadowHandler(cfg); err == nil {
		t.Fatal("nil transcriber accepted")
	}
	cfg = sttTestConfig(t, nil)
	cfg.Language = " "
	if _, err := NewSTTShadowHandler(cfg); err == nil {
		t.Fatal("blank language accepted")
	}
	cfg = sttTestConfig(t, nil)
	cfg.FinishTimeout = cfg.DrainTimeout + time.Second
	if _, err := NewSTTShadowHandler(cfg); err == nil {
		t.Fatal("bad finish deadline accepted")
	}
	cfg = sttTestConfig(t, nil)
	cfg.QueueFrames = 0
	if _, err := NewSTTShadowHandler(cfg); err == nil {
		t.Fatal("unbounded queue accepted")
	}
}

func TestSTTShadowConcurrentCallsAreIndependent(t *testing.T) {
	cfg := sttTestConfig(t, []stt.FakeEmission{{OnFinish: true, Event: stt.TranscriptEvent{Kind: stt.EventFinal, SegmentID: "s", Text: "hello", End: 40 * time.Millisecond}}})
	var wg sync.WaitGroup
	errCh := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := newSTTShadowSession(fmt.Sprintf("call-%d", i), cfg)
			if err != nil {
				errCh <- err
				return
			}
			for j := 0; j < 2; j++ {
				if err := s.FeedAudio(shadowTestPCM()); err != nil {
					errCh <- err
					s.End(false)
					return
				}
			}
			s.End(true)
			if !s.summary.complete || s.summary.audioFrames != 2 || s.summary.finals != 1 {
				errCh <- fmt.Errorf("call %d: %+v", i, s.summary)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// This provider blocks inside WriteAudio until cancellation, simulating an
// unreachable vendor writer. It allows a deterministic test of bounded audio
// queue overload and the ability to terminate the call-scoped observer.
type blockedSTTProvider struct{ started chan struct{} }

type blockedSTTSession struct {
	started     chan struct{}
	startedOnce sync.Once
	closeOnce   sync.Once
	events      chan stt.TranscriptEvent
}

func (p *blockedSTTProvider) Start(_ context.Context, _ stt.SpeechConfig) (stt.TranscriptionSession, error) {
	return &blockedSTTSession{started: p.started, events: make(chan stt.TranscriptEvent)}, nil
}
func (s *blockedSTTSession) WriteAudio(ctx context.Context, _ audio.AudioFrame) error {
	s.startedOnce.Do(func() { close(s.started) })
	<-ctx.Done()
	return ctx.Err()
}
func (s *blockedSTTSession) Events() <-chan stt.TranscriptEvent { return s.events }
func (s *blockedSTTSession) Finish(ctx context.Context) error   { return ctx.Err() }
func (s *blockedSTTSession) Abort()                             { s.closeOnce.Do(func() { close(s.events) }) }

func TestSTTShadowBlockedProviderFailsClosedOnOverflow(t *testing.T) {
	provider := &blockedSTTProvider{started: make(chan struct{})}
	cfg := sttTestConfig(t, nil)
	cfg.Provider = provider
	cfg.QueueFrames = 1
	cfg.QueueDuration = 20 * time.Millisecond
	s, err := newSTTShadowSession("overloaded-call", cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Converter emits a full 20ms frame after two 8k source frames.
	for i := 0; i < 2; i++ {
		if err := s.FeedAudio(shadowTestPCM()); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("STT writer did not block")
	}
	// The writer is stopped; exactly one queued frame can wait. Any excess
	// must explicitly return ErrQueueFull rather than letting RAM/lag grow.
	var full error
	for i := 0; i < 10; i++ {
		if full = s.FeedAudio(shadowTestPCM()); full != nil {
			break
		}
	}
	if !errors.Is(full, audio.ErrQueueFull) {
		t.Fatalf("expected bounded queue overflow; got %v", full)
	}
	s.End(true)
	if s.summary.complete || !errors.Is(s.summary.err, audio.ErrQueueFull) || s.summary.stats.Rejected == 0 {
		t.Fatalf("lost overload integrity signal: %+v", s.summary)
	}
}

func TestSTTShadowFailedSessionCreationPreservesEcho(t *testing.T) {
	shadowTestCall(t, func(c net.Conn) {
		handleConnection(c, func(string) (audioObserver, error) {
			return nil, errors.New("synthetic STT handshake failure")
		})
	}, 3, audiosocket.Frame{Type: audiosocket.TypeHangup})
}
