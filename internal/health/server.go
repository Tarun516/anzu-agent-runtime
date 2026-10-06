// This file is responsible for creating an http server with health & ready method/ready check which tells us if the process is alive and is ready to recieve the calls or not
// It shuts down gracefully when the application is being stopped

package health

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

type Server struct {
	name            string
	server          *http.Server
	logger          *slog.Logger
	ready           atomic.Bool
	shutdownTimeout time.Duration
}

func NewServer(
	name string,
	address string,
	shutdownTimeout time.Duration,
	logger *slog.Logger,
) *Server {
	s := &Server{
		name:            name,
		logger:          logger,
		shutdownTimeout: shutdownTimeout,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.readiness)

	s.server = &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// Run function of the server
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	s.SetReady(true)

	go func() {
		errCh <- s.server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		s.SetReady(false)

		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err

	case <-ctx.Done():
		s.SetReady(false)

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			s.shutdownTimeout,
		)

		defer cancel()

		s.logger.Info("shutting down health server")

		return s.server.Shutdown(shutdownCtx)
	}
}

// liveness check health endpoint -> if the process is alive or not
func (s *Server) health(
	w http.ResponseWriter,
	_ *http.Request,
) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": s.name,
	})
}

// readiness check -> if he process is ready to accept the traffic or not
func (s *Server) readiness(
	w http.ResponseWriter,
	_ *http.Request,
) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":  "not_ready",
			"service": s.name,
		})

		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ready",
		"service": s.name,
	})
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	body any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(body)
}
