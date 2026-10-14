package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

const defaultReadinessTimeout = 1 * time.Second

// ReadinessCheck represents one dependency that must be available
// before this process should receive new work.
type ReadinessCheck struct {
	Name  string
	Check func(context.Context) error
}

// Server exposes liveness and readiness endpoints
// and owns the lifecycle of the health HTTP server.
type Server struct {
	name string

	server *http.Server
	logger *slog.Logger

	ready atomic.Bool

	readinessChecks  []ReadinessCheck
	readinessTimeout time.Duration

	shutdownTimeout time.Duration
}

// NewServer creates a configured health server.
//
// It prepares the HTTP routes and dependency checks,
// but does not start listening yet.
func NewServer(
	name string,
	address string,
	shutdownTimeout time.Duration,
	logger *slog.Logger,
	readinessChecks []ReadinessCheck,
) *Server {
	// 1. Create the application-level health server.
	s := &Server{
		name:             name,
		logger:           logger,
		shutdownTimeout:  shutdownTimeout,
		readinessTimeout: defaultReadinessTimeout,
		readinessChecks:  readinessChecks,
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

	// 5. Create the underlying HTTP server.
	s.server = &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

// SetReady changes whether the application lifecycle
// currently allows this process to receive new work.
func (s *Server) SetReady(ready bool) {
	// 1. Publish the lifecycle-readiness state atomically.
	s.ready.Store(ready)
}

// Run preserves the existing standalone HTTP behavior.
// Binding now happens synchronously, so port errors are
// returned before the HTTP serving goroutine starts.
func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		s.SetReady(false)
		return fmt.Errorf(
			"bind health HTTP listener %q: %w",
			s.server.Addr,
			err,
		)
	}

	return s.RunOnListener(ctx, listener)
}

// RunOnListener serves HTTP on a listener already bound
// by the caller.
//
// Ownership of the listener transfers to this method.
// The caller must not independently close it while serving,
// except as part of an explicitly coordinated shutdown.
func (s *Server) RunOnListener(
	ctx context.Context,
	listener net.Listener,
) error {
	if listener == nil {
		s.SetReady(false)
		return errors.New("health HTTP listener is nil")
	}

	defer listener.Close()

	errCh := make(chan error, 1)

	// Serve accepts HTTP connections using the socket that
	// was already bound by the parent.
	go func() {
		errCh <- s.server.Serve(listener)
	}()

	select {
	case err := <-errCh:
		s.SetReady(false)

		// ErrServerClosed is expected only when the parent
		// has requested shutdown.
		if errors.Is(err, http.ErrServerClosed) &&
			ctx.Err() != nil {
			return nil
		}

		return fmt.Errorf(
			"health HTTP server stopped: %w",
			err,
		)

	case <-ctx.Done():
		// Stop advertising readiness before HTTP shutdown.
		s.SetReady(false)

		// The root context is already cancelled, so HTTP
		// cleanup needs a fresh, bounded context.
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			s.shutdownTimeout,
		)
		defer cancel()

		s.logger.Info(
			"shutting down health server",
			"service",
			s.name,
		)

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
// Liveness answers:
//
//	"Is the Anzu process itself alive?"
func (s *Server) health(
	w http.ResponseWriter,
	_ *http.Request,
) {
	// 1. If this handler can execute, the process is alive.
	writeJSON(
		w,
		http.StatusOK,
		map[string]string{
			"status":  "ok",
			"service": s.name,
		},
	)
}

// readiness determines whether this process should
// currently receive new work.
//
// Readiness requires both:
//   - the application lifecycle to allow new work;
//   - all required dependencies to pass their checks.
func (s *Server) readiness(
	w http.ResponseWriter,
	r *http.Request,
) {
	// 1. Check the application lifecycle gate first.
	//
	// During startup or graceful shutdown, dependency health
	// does not matter because this process should not accept work.
	if !s.ready.Load() {
		writeNotReady(
			w,
			s.name,
		)

		return
	}

	// 2. Verify every required dependency.
	for _, check := range s.readinessChecks {
		// 3. Give this dependency check a bounded timeout.
		//
		// The request context is used as the parent so work
		// also stops if the HTTP request itself is cancelled.
		checkCtx, cancel := context.WithTimeout(
			r.Context(),
			s.readinessTimeout,
		)

		// 4. Run the injected dependency check.
		err := check.Check(checkCtx)

		// 5. Release this check's context/timer resources
		// immediately instead of deferring until handler return.
		cancel()

		// 6. One failed required dependency makes
		// the entire process unready.
		if err != nil {
			s.logger.Debug(
				"readiness check failed",
				"service",
				s.name,
				"dependency",
				check.Name,
				"error",
				err,
			)

			writeNotReady(
				w,
				s.name,
			)

			return
		}
	}

	// 7. The lifecycle gate is open and all
	// required dependencies are healthy.
	writeJSON(
		w,
		http.StatusOK,
		map[string]string{
			"status":  "ready",
			"service": s.name,
		},
	)
}

// writeNotReady writes the standard response used whenever
// this process should not receive new work.
func writeNotReady(
	w http.ResponseWriter,
	service string,
) {
	// 1. Return a generic response without exposing
	// dependency/network internals to callers.
	writeJSON(
		w,
		http.StatusServiceUnavailable,
		map[string]string{
			"status":  "not_ready",
			"service": service,
		},
	)
}

// writeJSON writes an HTTP status code and serializes
// the supplied value as JSON.
func writeJSON(
	w http.ResponseWriter,
	status int,
	body any,
) {
	// 1. Tell the client that the response contains JSON.
	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	// 2. Write the HTTP status code.
	w.WriteHeader(status)

	// 3. Encode the supplied Go value into the response body.
	_ = json.NewEncoder(w).Encode(body)
}
