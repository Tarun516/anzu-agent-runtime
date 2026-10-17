package deepgram

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audio"
	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt"
)

type mockMessage struct {
	typ  int
	data []byte
	err  error
}
type mockSocket struct {
	incoming chan mockMessage
	closed   chan struct{}
	once     sync.Once
	mu       sync.Mutex
	writes   []mockMessage
	onWrite  func(int, []byte)
}

func newMockSocket() *mockSocket {
	return &mockSocket{incoming: make(chan mockMessage, 100), closed: make(chan struct{})}
}
func (m *mockSocket) Read(ctx context.Context) (int, []byte, error) {
	select {
	case <-m.closed:
		return 0, nil, io.EOF
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case v := <-m.incoming:
		return v.typ, v.data, v.err
	}
}
func (m *mockSocket) Write(ctx context.Context, typ int, body []byte) error {
	select {
	case <-m.closed:
		return io.ErrClosedPipe
	default:
	}
	cp := append([]byte(nil), body...)
	m.mu.Lock()
	m.writes = append(m.writes, mockMessage{typ: typ, data: cp})
	fn := m.onWrite
	m.mu.Unlock()
	if fn != nil {
		fn(typ, cp)
	}
	return nil
}
func (m *mockSocket) Close() error { m.once.Do(func() { close(m.closed) }); return nil }
func (m *mockSocket) sendJSON(js string) {
	m.incoming <- mockMessage{typ: TextMessage, data: []byte(js)}
}
func (m *mockSocket) eof() { m.incoming <- mockMessage{err: io.EOF} }
func (m *mockSocket) snapshot() []mockMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]mockMessage(nil), m.writes...)
}

type mockDial struct {
	mu       sync.Mutex
	conn     *mockSocket
	endpoint string
	auth     string
}

func (d *mockDial) Dial(ctx context.Context, u string, h http.Header) (Socket, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.endpoint = u
	d.auth = h.Get("Authorization")
	return d.conn, nil
}
func testProvider(t *testing.T, conn *mockSocket, buf int, interval time.Duration) (*Provider, *mockDial) {
	t.Helper()
	d := &mockDial{conn: conn}
	p, err := New(Config{APIKey: "test-not-real", Model: "nova-3", Dialer: d, EventBuffer: buf, KeepAliveInterval: interval})
	if err != nil {
		t.Fatal(err)
	}
	return p, d
}
func testCfg(callID string) stt.SpeechConfig {
	return stt.SpeechConfig{CallID: callID, Language: "en-IN", SampleRate: 16000, Channels: 1, Encoding: stt.EncodingPCM16}
}
func frame(seq uint64, ms int64) audio.AudioFrame {
	return audio.AudioFrame{Sequence: seq, Timestamp: time.Duration(ms) * time.Millisecond, SampleRate: 16000, Channels: 1, Samples: make([]int16, 320)}
}
func waitFor(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDeepgramHandshakeAndPartialFinalEvents(t *testing.T) {
	ws := newMockSocket()
	p, d := testProvider(t, ws, 10, time.Second)
	ws.onWrite = func(typ int, payload []byte) {
		if typ == TextMessage && string(payload) == `{"type":"CloseStream"}` {
			ws.sendJSON(`{"type":"Results","start":0,"duration":0.04,"is_final":true,"channel":{"alternatives":[{"transcript":"book a table"}]}}`)
			ws.eof()
		}
	}
	s, err := p.Start(context.Background(), testCfg("call-1"))
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(d.endpoint)
	q := u.Query()
	if q.Get("model") != "nova-3" || q.Get("encoding") != "linear16" || q.Get("sample_rate") != "16000" || q.Get("language") != "en-IN" || q.Get("interim_results") != "true" || d.auth != "Token test-not-real" {
		t.Fatalf("bad request query=%v auth=%v", q, d.auth)
	}
	ws.sendJSON(`{"type":"Metadata","request_id":"xyz"}`)
	ws.sendJSON(`{"type":"Results","start":0,"duration":0.02,"is_final":false,"channel":{"alternatives":[{"transcript":"book"}]}}`)
	ws.sendJSON(`{"type":"Results","start":0,"duration":0.04,"is_final":false,"channel":{"alternatives":[{"transcript":"book a"}]}}`)
	ws.sendJSON(`{"type":"Results","start":0,"duration":0.04,"is_final":false,"channel":{"alternatives":[{"transcript":""}]}}`) // ignored
	a := frame(0, 0)
	a.Samples[0] = -32768
	a.Samples[1] = 32767
	if err := s.WriteAudio(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteAudio(context.Background(), frame(1, 20)); err != nil {
		t.Fatal(err)
	}
	writes := ws.snapshot()
	if len(writes) != 2 || writes[0].typ != BinaryMessage || len(writes[0].data) != 640 {
		t.Fatalf("wrong binary writes: %+v", writes)
	}
	if binary.LittleEndian.Uint16(writes[0].data[:2]) != 0x8000 || binary.LittleEndian.Uint16(writes[0].data[2:4]) != 0x7fff {
		t.Fatalf("wrong PCM16 byte order: %x", writes[0].data[:4])
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Finish(ctx); err != nil {
		t.Fatal(err)
	}
	var all []stt.TranscriptEvent
	for e := range s.Events() {
		all = append(all, e)
	}
	if len(all) != 3 {
		t.Fatalf("got %d transcript events: %+v", len(all), all)
	}
	for i, e := range all {
		if e.Sequence != uint64(i) || e.CallID != "call-1" || e.SegmentID != "seg-0" {
			t.Fatalf("wrong event %+v", e)
		}
	}
	if all[0].Kind != stt.EventPartial || all[1].Kind != stt.EventPartial || all[2].Kind != stt.EventFinal || all[2].Text != "book a table" {
		t.Fatalf("bad partial/final semantics: %+v", all)
	}
	if all[2].End != 40*time.Millisecond {
		t.Fatalf("wrong timing: %+v", all[2])
	}
	if err := s.WriteAudio(context.Background(), frame(2, 40)); !errors.Is(err, stt.ErrSessionClosed) {
		t.Fatalf("write after finish: %v", err)
	}
	s.Abort()
}

func TestDeepgramDiscontinuityAndFormatValidation(t *testing.T) {
	ws := newMockSocket()
	p, _ := testProvider(t, ws, 4, time.Second)
	s, err := p.Start(context.Background(), testCfg("call-x"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	if err := s.WriteAudio(context.Background(), frame(1, 20)); !errors.Is(err, stt.ErrDiscontinuity) {
		t.Fatalf("expect discontinuity, got %v", err)
	}
	if err := s.WriteAudio(context.Background(), frame(0, 20)); !errors.Is(err, stt.ErrDiscontinuity) {
		t.Fatalf("expect timestamp discontinuity, got %v", err)
	}
	bad := frame(0, 0)
	bad.SampleRate = 8000
	if err := s.WriteAudio(context.Background(), bad); err == nil {
		t.Fatal("expected audio format error")
	}
	if len(ws.snapshot()) != 0 {
		t.Fatal("invalid audio reached socket")
	}
	if err := s.WriteAudio(context.Background(), frame(0, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteAudio(context.Background(), frame(1, 20)); err != nil {
		t.Fatal(err)
	}
	if len(ws.snapshot()) != 2 {
		t.Fatal("valid retry failed")
	}
}

func TestDeepgramErrorsAndUnexpectedEOF(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		eof       bool
	}{
		{name: "provider error", raw: `{"type":"Error","code":"Invalid","description":"bad format"}`},
		{name: "malformed JSON", raw: `{oops`},
		{name: "unexpected EOF", eof: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws := newMockSocket()
			p, _ := testProvider(t, ws, 4, time.Second)
			s, err := p.Start(context.Background(), testCfg("c"))
			if err != nil {
				t.Fatal(err)
			}
			if tc.eof {
				ws.eof()
			} else {
				ws.sendJSON(tc.raw)
			}
			var evs []stt.TranscriptEvent
			for e := range s.Events() {
				evs = append(evs, e)
			}
			if len(evs) != 1 || evs[0].Kind != stt.EventError || evs[0].Err == nil {
				t.Fatalf("missing terminal EventError: %+v", evs)
			}
			if err := s.WriteAudio(context.Background(), frame(0, 0)); !errors.Is(err, stt.ErrSessionFailed) {
				t.Fatalf("missing failed state: %v", err)
			}
			if err := s.Finish(context.Background()); !errors.Is(err, stt.ErrSessionFailed) {
				t.Fatalf("missing failed finish: %v", err)
			}
		})
	}
}

func TestDeepgramSlowEventConsumerFailsClosed(t *testing.T) {
	ws := newMockSocket()
	p, _ := testProvider(t, ws, 1, time.Second)
	s, err := p.Start(context.Background(), testCfg("c"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		ws.sendJSON(`{"type":"Results","start":0,"duration":0.02,"is_final":false,"channel":{"alternatives":[{"transcript":"hello"}]}}`)
	}
	<-s.(*Session).done
	// A full event buffer may prevent even EventError from being delivered.
	if err := s.Finish(context.Background()); !errors.Is(err, stt.ErrSessionFailed) {
		t.Fatalf("expected overflow failure, got %v", err)
	}
	count := 0
	for range s.Events() {
		count++
	}
	if count != 1 {
		t.Fatalf("buffer should be bounded; got %d", count)
	}
}

func TestDeepgramAbortAndParentCancellation(t *testing.T) {
	ws := newMockSocket()
	p, _ := testProvider(t, ws, 4, time.Second)
	parent, cancel := context.WithCancel(context.Background())
	s, err := p.Start(parent, testCfg("c"))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitFor(t, func() bool {
		select {
		case <-s.(*Session).done:
			return true
		default:
			return false
		}
	})
	s.Abort()
	s.Abort()
	if err := s.WriteAudio(context.Background(), frame(0, 0)); !errors.Is(err, stt.ErrSessionClosed) {
		t.Fatalf("expected closed: %v", err)
	}
	for range s.Events() {
	} // safely closed once
}

func TestDeepgramKeepAlive(t *testing.T) {
	ws := newMockSocket()
	p, _ := testProvider(t, ws, 5, 15*time.Millisecond)
	s, err := p.Start(context.Background(), testCfg("c"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, x := range ws.snapshot() {
			if x.typ == TextMessage && strings.Contains(string(x.data), "KeepAlive") {
				return true
			}
		}
		return false
	})
	s.Abort()
	<-s.(*Session).done
}

func TestDeepgramValidation(t *testing.T) {
	d := &mockDial{conn: newMockSocket()}
	for _, cfg := range []Config{{Model: "nova-3", Dialer: d}, {APIKey: "k"}, {APIKey: "k", Dialer: d, Endpoint: "ws://example.org/"}, {APIKey: "k", Dialer: d, EventBuffer: 257}, {APIKey: "k", Dialer: d, Endpoint: "https://api.deepgram.com/v1/listen"}} {
		if _, err := New(cfg); err == nil {
			t.Fatalf("expected invalid config %+v", cfg)
		}
	}
	p, err := New(Config{APIKey: "k", Dialer: d, Endpoint: "ws://127.0.0.1:12345/v1/listen"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Start(context.Background(), stt.SpeechConfig{}); err == nil {
		t.Fatal("invalid speech cfg accepted")
	}
}
