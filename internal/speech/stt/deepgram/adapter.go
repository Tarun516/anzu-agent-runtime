// Package deepgram implements the provider-neutral stt.Transcriber contract
// for Deepgram's Listen v1 streaming WebSocket protocol.
// It does not depend on a particular WebSocket library; Dialer is injected.
package deepgram

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audio"
	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt"
)

const (
	DefaultEndpoint = "wss://api.deepgram.com/v1/listen"
	TextMessage     = 1
	BinaryMessage   = 2
)

// Socket is deliberately tiny: only these methods are needed by the adapter.
// It has exactly one reader (readLoop) and serializes ALL writes (audio,
// keep-alives, CloseStream) with writeMu.
type Socket interface {
	Read(ctx context.Context) (messageType int, payload []byte, err error)
	Write(ctx context.Context, messageType int, payload []byte) error
	Close() error
}

type Dialer interface {
	Dial(ctx context.Context, endpoint string, headers http.Header) (Socket, error)
}

// Config contains provider-wide settings. Call/language/PCM format belong to
// stt.SpeechConfig and are supplied to Start separately.
type Config struct {
	APIKey            string
	Model             string
	Endpoint          string
	Dialer            Dialer
	EventBuffer       int           // default 32; fail closed if consumer is too slow
	KeepAliveInterval time.Duration // default 4s; send JSON KeepAlive on idle
}

type Provider struct{ cfg Config }

var _ stt.Transcriber = (*Provider)(nil)

func New(cfg Config) (*Provider, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, errors.New("Deepgram API key required")
	}
	if strings.ContainsAny(cfg.APIKey, "\r\n") {
		return nil, errors.New("invalid Deepgram API key")
	}
	if cfg.Dialer == nil {
		return nil, errors.New("Deepgram WebSocket dialer required")
	}
	if cfg.Model == "" {
		cfg.Model = "nova-3"
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = DefaultEndpoint
	}
	parsed, err := url.Parse(cfg.Endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "wss" && parsed.Scheme != "ws") {
		return nil, errors.New("invalid Deepgram WebSocket endpoint")
	}
	// API tokens must not travel unencrypted; only allow ws:// loopback for
	// deterministic local test servers. In all deployments use wss://.
	if parsed.Scheme == "ws" {
		host := parsed.Hostname()
		if net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			if host != "localhost" {
				return nil, errors.New("plain WebSocket allowed only on loopback")
			}
		}
	}
	if cfg.EventBuffer == 0 {
		cfg.EventBuffer = 32
	}
	if cfg.EventBuffer < 1 || cfg.EventBuffer > 256 {
		return nil, errors.New("event buffer must be 1..256")
	}
	if cfg.KeepAliveInterval == 0 {
		cfg.KeepAliveInterval = 4 * time.Second
	}
	if cfg.KeepAliveInterval < 10*time.Millisecond || cfg.KeepAliveInterval > time.Minute {
		return nil, errors.New("invalid KeepAlive interval")
	}
	return &Provider{cfg: cfg}, nil
}

func (p *Provider) Start(ctx context.Context, cfg stt.SpeechConfig) (stt.TranscriptionSession, error) {
	if ctx == nil {
		return nil, errors.New("STT context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	u, _ := url.Parse(p.cfg.Endpoint)
	params := u.Query()
	params.Set("model", p.cfg.Model)
	params.Set("language", cfg.Language)
	params.Set("encoding", "linear16")
	params.Set("sample_rate", strconv.Itoa(cfg.SampleRate))
	params.Set("channels", strconv.Itoa(cfg.Channels))
	params.Set("interim_results", "true")
	params.Set("punctuate", "true")
	params.Set("endpointing", "300") // provider segment handling != CP5 turn commitment
	u.RawQuery = params.Encode()
	h := make(http.Header)
	h.Set("Authorization", "Token "+p.cfg.APIKey)
	conn, err := p.cfg.Dialer.Dial(ctx, u.String(), h)
	if err != nil {
		return nil, fmt.Errorf("Deepgram connect: %w", err)
	} // Never log auth header.
	if conn == nil {
		return nil, errors.New("Deepgram dialer returned a nil socket")
	}
	s := &Session{cfg: cfg, socket: conn, events: make(chan stt.TranscriptEvent, p.cfg.EventBuffer), done: make(chan struct{}), alive: true, lastWrite: time.Now()}
	s.stopParent = context.AfterFunc(ctx, s.Abort)
	go s.readLoop()
	go s.keepAliveLoop(p.cfg.KeepAliveInterval)
	return s, nil
}

// Session state is protected by mu; only writeMu touches outgoing WebSocket
// messages. The readLoop is the sole producer and closer of events/done.
type Session struct {
	cfg        stt.SpeechConfig
	socket     Socket
	events     chan stt.TranscriptEvent
	done       chan struct{}
	stopParent func() bool

	writeMu      sync.Mutex
	mu           sync.Mutex
	alive        bool
	finishing    bool
	aborted      bool
	failure      error
	nextInputSeq uint64
	inputSamples uint64    // samples PER channel
	nextEventSeq uint64    // touched only by readLoop
	lastWrite    time.Time // touched only under writeMu
}

var _ stt.TranscriptionSession = (*Session)(nil)

func (s *Session) Events() <-chan stt.TranscriptEvent { return s.events }

func (s *Session) WriteAudio(ctx context.Context, f audio.AudioFrame) error {
	if ctx == nil {
		return errors.New("audio write context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.Validate(); err != nil {
		return fmt.Errorf("invalid audio: %w", err)
	}
	if f.SampleRate != s.cfg.SampleRate || f.Channels != s.cfg.Channels {
		return errors.New("STT audio format mismatch")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if err := s.stateErrorLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	expectedSeq := s.nextInputSeq
	expectedTime := sampleTime(s.inputSamples, s.cfg.SampleRate)
	s.mu.Unlock()
	if f.Sequence != expectedSeq || f.Timestamp != expectedTime {
		return fmt.Errorf("%w: seq=%d want=%d time=%s want=%s", stt.ErrDiscontinuity, f.Sequence, expectedSeq, f.Timestamp, expectedTime)
	}
	payload := make([]byte, len(f.Samples)*2)
	for i, v := range f.Samples {
		binary.LittleEndian.PutUint16(payload[2*i:], uint16(v))
	}
	if err := s.socket.Write(ctx, BinaryMessage, payload); err != nil {
		s.fail(fmt.Errorf("Deepgram audio write: %w", err))
		return fmt.Errorf("%w: audio write: %v", stt.ErrSessionFailed, err)
	}
	s.mu.Lock()
	s.nextInputSeq++
	s.inputSamples += uint64(f.SamplesPerChannel())
	s.mu.Unlock()
	s.lastWrite = time.Now()
	return nil
}

// Finish gracefully half-closes the Deepgram input by writing CloseStream and
// waiting for the read loop to see the server's normal close. Consumers should
// ALWAYS read Events concurrently, even while Finish is in progress.
func (s *Session) Finish(ctx context.Context) error {
	if ctx == nil {
		return errors.New("finish context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.writeMu.Lock()
	s.mu.Lock()
	stateErr := s.stateErrorLocked()
	if stateErr != nil && !s.finishing {
		s.mu.Unlock()
		s.writeMu.Unlock()
		return stateErr
	}
	first := !s.finishing
	if first {
		s.finishing = true
	}
	s.mu.Unlock()
	if first {
		if err := s.socket.Write(ctx, TextMessage, []byte(`{"type":"CloseStream"}`)); err != nil {
			s.writeMu.Unlock()
			s.fail(fmt.Errorf("Deepgram CloseStream: %w", err))
			return fmt.Errorf("%w: CloseStream: %v", stt.ErrSessionFailed, err)
		}
		s.lastWrite = time.Now()
	}
	s.writeMu.Unlock()
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.failure != nil {
			return fmt.Errorf("%w: %v", stt.ErrSessionFailed, s.failure)
		}
		if s.aborted {
			return stt.ErrSessionClosed
		}
		return nil
	case <-ctx.Done():
		s.Abort()
		return ctx.Err()
	}
}

func (s *Session) Abort() {
	s.mu.Lock()
	if s.aborted || (!s.alive && !s.finishing) {
		s.mu.Unlock()
		return
	}
	s.aborted = true
	s.alive = false
	s.mu.Unlock()
	_ = s.socket.Close() // interrupts reader, writer and keepalive; idempotent socket close expected
}

func (s *Session) stateErrorLocked() error {
	if s.failure != nil {
		return fmt.Errorf("%w: %v", stt.ErrSessionFailed, s.failure)
	}
	if !s.alive || s.aborted || s.finishing {
		return stt.ErrSessionClosed
	}
	return nil
}

func (s *Session) fail(err error) {
	s.mu.Lock()
	if s.failure == nil && !s.aborted {
		s.failure = err
	}
	s.alive = false
	s.mu.Unlock()
	_ = s.socket.Close()
}

func (s *Session) keepAliveLoop(interval time.Duration) {
	timer := time.NewTicker(interval)
	defer timer.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-timer.C:
			s.writeMu.Lock()
			s.mu.Lock()
			inactive := !s.alive || s.finishing
			s.mu.Unlock()
			if !inactive && time.Since(s.lastWrite) >= interval {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				err := s.socket.Write(ctx, TextMessage, []byte(`{"type":"KeepAlive"}`))
				cancel()
				if err != nil {
					s.writeMu.Unlock()
					s.fail(fmt.Errorf("Deepgram KeepAlive: %w", err))
					return
				}
				s.lastWrite = time.Now()
			}
			s.writeMu.Unlock()
		}
	}
}

type dgResult struct {
	Type     string  `json:"type"`
	Start    float64 `json:"start"`
	Duration float64 `json:"duration"`
	IsFinal  bool    `json:"is_final"`
	Channel  struct {
		Alternatives []struct {
			Transcript string `json:"transcript"`
		} `json:"alternatives"`
	} `json:"channel"`
	Description string `json:"description"`
	Code        string `json:"code"`
}

func (s *Session) readLoop() {
	defer close(s.events)
	defer close(s.done)
	defer s.stopParent()
	defer s.socket.Close()
	for {
		typ, data, err := s.socket.Read(context.Background())
		if err != nil {
			s.mu.Lock()
			expected := s.finishing && !s.aborted && s.failure == nil
			aborted := s.aborted
			s.alive = false
			s.mu.Unlock()
			if !aborted && !(expected && errors.Is(err, io.EOF)) {
				s.reportFailure(fmt.Errorf("Deepgram read/close: %w", err))
			}
			return
		}
		if typ != TextMessage {
			s.reportFailure(errors.New("unexpected binary STT server message"))
			return
		}
		var msg dgResult
		if err := json.Unmarshal(data, &msg); err != nil {
			s.reportFailure(fmt.Errorf("invalid Deepgram JSON: %w", err))
			return
		}
		switch msg.Type {
		case "Results":
			if len(msg.Channel.Alternatives) == 0 {
				continue
			}
			transcript := strings.TrimSpace(msg.Channel.Alternatives[0].Transcript)
			if transcript == "" {
				continue
			} // silence often produces empty Results
			start, valid1 := secondsDuration(msg.Start)
			length, valid2 := secondsDuration(msg.Duration)
			if !valid1 || !valid2 || int64(start) > math.MaxInt64-int64(length) {
				s.reportFailure(errors.New("invalid Deepgram time offsets"))
				return
			}
			end := start + length
			kind := stt.EventPartial
			if msg.IsFinal {
				kind = stt.EventFinal
			}
			// Deepgram revises results at the same start offset; end can grow
			// during partial hypotheses. ID must NOT depend on duration.
			evt := stt.TranscriptEvent{Kind: kind, CallID: s.cfg.CallID, Sequence: s.nextEventSeq,
				SegmentID: fmt.Sprintf("seg-%d", start.Microseconds()), Text: transcript, Start: start, End: end}
			if err := evt.Validate(); err != nil {
				s.reportFailure(err)
				return
			}
			if !s.publish(evt) {
				return
			}
			s.nextEventSeq++
		case "Error":
			detail := strings.TrimSpace(msg.Description)
			if detail == "" {
				detail = "provider error"
			}
			e := fmt.Errorf("Deepgram error %s: %s", msg.Code, detail)
			s.reportFailure(e)
			return
		case "Metadata", "SpeechStarted", "UtteranceEnd":
			// Future CP5 can extend the provider-neutral event schema for VAD.
		default:
			// Forward compatible: ignore non-transcript vendor messages.
		}
	}
}

func (s *Session) publish(evt stt.TranscriptEvent) bool {
	select {
	case s.events <- evt:
		return true
	default:
		s.reportFailure(errors.New("STT event buffer overflow: consumer is too slow"))
		return false
	}
}

func (s *Session) reportFailure(err error) {
	s.mu.Lock()
	already := s.failure != nil || s.aborted
	s.mu.Unlock()
	if already {
		return
	}
	evt := stt.TranscriptEvent{Kind: stt.EventError, CallID: s.cfg.CallID, Sequence: s.nextEventSeq, Err: err}
	// An already-full consumer buffer cannot accept an error event. The
	// terminal state is still preserved for the writer / Finish result.
	select {
	case s.events <- evt:
		s.nextEventSeq++
	default:
	}
	s.fail(err)
}

func sampleTime(offset uint64, rate int) time.Duration {
	whole := offset / uint64(rate)
	rem := offset % uint64(rate)
	return time.Duration(whole)*time.Second + time.Duration(rem)*time.Second/time.Duration(rate)
}
func secondsDuration(v float64) (time.Duration, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > float64(math.MaxInt64)/float64(time.Second) {
		return 0, false
	}
	return time.Duration(math.Round(v * float64(time.Second))), true
}
