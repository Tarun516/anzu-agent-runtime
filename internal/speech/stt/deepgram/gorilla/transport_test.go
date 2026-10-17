package gorilla

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audio"
	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt"
	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt/deepgram"
	"github.com/gorilla/websocket"
)

// Tests real local RFC6455 handshake, authorization header, binary PCM upload,
// vendor Results JSON, CloseStream, normal close and a final STT event WITHOUT
// contacting Deepgram or consuming any credit.
func TestLocalWebSocketRoundTrip(t *testing.T) {
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("encoding") != "linear16" || r.URL.Query().Get("sample_rate") != "16000" || r.URL.Query().Get("language") != "en-IN" || r.Header.Get("Authorization") != "Token test-local" {
			result <- fmt.Errorf("unexpected Deepgram handshake parameters")
			http.Error(w, "bad config", http.StatusBadRequest)
			return
		}
		c, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			result <- err
			return
		}
		defer c.Close()
		typ, buf, err := c.ReadMessage()
		if err != nil {
			result <- err
			return
		}
		if typ != websocket.BinaryMessage || len(buf) != 640 {
			result <- fmt.Errorf("unexpected audio payload type=%d bytes=%d", typ, len(buf))
			return
		}
		if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"Results","start":0,"duration":0.02,"is_final":false,"channel":{"alternatives":[{"transcript":"hello"}]}}`)); err != nil {
			result <- err
			return
		}
		typ, buf, err = c.ReadMessage()
		if err != nil {
			result <- err
			return
		}
		if typ != websocket.TextMessage || string(buf) != `{"type":"CloseStream"}` {
			result <- fmt.Errorf("unexpected finish message type=%d: %s", typ, buf)
			return
		}
		if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"Results","start":0,"duration":0.02,"is_final":true,"channel":{"alternatives":[{"transcript":"hello world"}]}}`)); err != nil {
			result <- err
			return
		}
		if err := c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second)); err != nil {
			result <- err
			return
		}
		result <- nil
	}))
	defer server.Close()

	p, err := deepgram.New(deepgram.Config{APIKey: "test-local", Endpoint: "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/listen", Dialer: Dialer{}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := p.Start(ctx, stt.SpeechConfig{CallID: "local-call", Language: "en-IN", SampleRate: 16000, Channels: 1, Encoding: stt.EncodingPCM16})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	if err := s.WriteAudio(ctx, audio.AudioFrame{Sequence: 0, Timestamp: 0, SampleRate: 16000, Channels: 1, Samples: make([]int16, 320)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx); err != nil {
		t.Fatal(err)
	}
	var events []stt.TranscriptEvent
	for e := range s.Events() {
		events = append(events, e)
	}
	if len(events) != 2 || events[0].Kind != stt.EventPartial || events[1].Kind != stt.EventFinal || events[1].Text != "hello world" {
		t.Fatalf("unexpected events %+v", events)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("server handler failed to exit")
	}
}
