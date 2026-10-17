// Package gorilla bridges the small Deepgram Socket interface to the
// maintained github.com/gorilla/websocket implementation. Keep all RFC6455
// framing, masking, TLS and handshake details inside this dependency.
package gorilla

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt/deepgram"
	"github.com/gorilla/websocket"
)

type Dialer struct {
	HandshakeTimeout time.Duration
	WriteTimeout     time.Duration
}

var _ deepgram.Dialer = Dialer{}

func (d Dialer) Dial(ctx context.Context, endpoint string, headers http.Header) (deepgram.Socket, error) {
	if d.HandshakeTimeout <= 0 {
		d.HandshakeTimeout = 10 * time.Second
	}
	if d.WriteTimeout <= 0 {
		d.WriteTimeout = 5 * time.Second
	}
	wd := websocket.Dialer{Proxy: http.ProxyFromEnvironment, HandshakeTimeout: d.HandshakeTimeout, EnableCompression: false}
	c, resp, err := wd.DialContext(ctx, endpoint, headers)
	if err != nil {
		// Never include response body, Authorization header or API key.
		if resp != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	c.SetReadLimit(1 << 20) // Do not let a malicious endpoint send unbounded JSON.
	return &socket{conn: c, writeTimeout: d.WriteTimeout}, nil
}

type socket struct {
	conn         *websocket.Conn
	writeTimeout time.Duration
}

var _ deepgram.Socket = (*socket)(nil)

func (s *socket) Read(ctx context.Context) (int, []byte, error) {
	// One reader at a time; cancellation force-closes the whole connection.
	stop := context.AfterFunc(ctx, func() { _ = s.conn.Close() })
	defer stop()
	typ, payload, err := s.conn.ReadMessage()
	if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		return typ, payload, io.EOF
	}
	return typ, payload, err
}

func (s *socket) Write(ctx context.Context, typ int, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = s.conn.Close() })
	defer stop()
	deadline := time.Now().Add(s.writeTimeout)
	if t, ok := ctx.Deadline(); ok && t.Before(deadline) {
		deadline = t
	}
	if err := s.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return s.conn.WriteMessage(typ, payload)
}
func (s *socket) Close() error { return s.conn.Close() }
