package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Server exposes health and readiness endpoints
// and owns the lifecycle of the health HTTP server.
type Server struct {
	name string

	server *http.Server
	logger *slog.Logger

	ready atomic.Bool

	shutdownTimeout time.Duration
}

// NewServer creates a configured health server.
//
// It prepares the HTTP routes and server configuration,
// but does not start listening yet.
func NewServer(
	name string,
	address string,
	shutdownTimeout time.Duration,
	logger *slog.Logger,
) *Server {
	// 1. Create the Anzu health server object.
	s := &Server{
		name:            name,
		logger:          logger,
		shutdownTimeout: shutdownTimeout,
	}

	// 2. Create the HTTP request router.
	mux := http.NewServeMux()

	// 3. Register the liveness endpoint.
	mux.HandleFunc(
		"GET /healthz",
		s.health,
	)

	// 4. Register the readiness endpoint.
	mux.HandleFunc(
		"GET /readyz",
		s.readiness,
	)

	// 5. Create the underlying Go HTTP server.
	s.server = &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

// SetReady changes whether this process should
// currently receive new work.
func (s *Server) SetReady(ready bool) {
	// 1. Publish the new readiness value atomically.
	s.ready.Store(ready)
}

// Run starts the HTTP server and waits until either
// the server fails or the application context is cancelled.
//
// When cancellation occurs, the server performs
// a bounded graceful shutdown.
func (s *Server) Run(ctx context.Context) error {
	// 1. Create a one-result channel for the HTTP server's
	// final result.
	errCh := make(chan error, 1)

	// 2. Start the blocking HTTP server concurrently.
	go func() {
		errCh <- s.server.ListenAndServe()
	}()

	// 3. Wait for either server termination
	// or application cancellation.
	select {
	case err := <-errCh:
		// 4. ErrServerClosed is expected when Shutdown()
		// intentionally closes the server.
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return fmt.Errorf(
			"health HTTP server stopped: %w",
			err,
		)

	case <-ctx.Done():
		// 5. Stop advertising readiness immediately.
		s.SetReady(false)

		// 6. Create an independent timeout for cleanup.
		//
		// We use context.Background() because the root
		// application context is already cancelled.
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			s.shutdownTimeout,
		)
		defer cancel()

		// 7. Record that graceful shutdown has started.
		s.logger.Info(
			"shutting down health server",
			"service",
			s.name,
		)

		// 8. Stop accepting new requests and allow active
		// requests a bounded amount of time to finish.
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf(
				"shutdown health HTTP server: %w",
				err,
			)
		}

		return nil
	}
}

// health handles the liveness endpoint.
//
// It answers:
//
//	"Is this process alive?"
func (s *Server) health(
	w http.ResponseWriter,
	_ *http.Request,
) {
	// 1. If this handler is executing, the process
	// can respond to HTTP requests.
	writeJSON(
		w,
		http.StatusOK,
		map[string]string{
			"status":  "ok",
			"service": s.name,
		},
	)
}

// readiness handles the readiness endpoint.
//
// It answers:
//
//	"Should this process receive new work?"
func (s *Server) readiness(
	w http.ResponseWriter,
	_ *http.Request,
) {
	// 1. Read the current readiness state safely.
	if !s.ready.Load() {
		// 2. The process is alive but is not ready
		// to receive new work.
		writeJSON(
			w,
			http.StatusServiceUnavailable,
			map[string]string{
				"status":  "not_ready",
				"service": s.name,
			},
		)

		return
	}

	// 3. The process is ready to receive work.
	writeJSON(
		w,
		http.StatusOK,
		map[string]string{
			"status":  "ready",
			"service": s.name,
		},
	)
}

// writeJSON writes an HTTP status code and encodes
// the supplied value as a JSON response.
func writeJSON(
	w http.ResponseWriter,
	status int,
	body any,
) {
	// 1. Tell the client that the response body contains JSON.
	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	// 2. Write the HTTP status code.
	w.WriteHeader(status)

	// 3. Encode the Go value directly into the response body.
	_ = json.NewEncoder(w).Encode(body)
}
