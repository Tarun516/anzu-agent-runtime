package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/Tarun516/anzu-agent-runtime/internal/config"
	"github.com/Tarun516/anzu-agent-runtime/internal/health"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/session"
)

type serverResult struct {
	name string
	err  error
}

func superviseListeners(
	ctx context.Context,
	httpServer *health.Server,
	listeners *boundListeners,
	manager *session.Manager,
	audioCfg config.AudioSocket,
) (resultErr error) {
	if ctx == nil || httpServer == nil ||
		listeners == nil || listeners.http == nil ||
		manager == nil {
		return errors.New("missing realtime dependency")
	}

	if audioCfg.Enabled {
		if listeners.audio == nil {
			return errors.New("AudioSocket listener missing")
		}
		if audioCfg.GracePeriod <= 0 ||
			audioCfg.CleanupTimeout <= 0 {
			return errors.New("invalid AudioSocket shutdown durations")
		}
	}

	// Release bound sockets after both serving routines exit.
	defer func() {
		resultErr = errors.Join(resultErr, listeners.Close())
	}()

	// Cancellation is shared between both servers.
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Buffered so either server can report completion
	// without waiting for the supervisor to receive.
	results := make(chan serverResult, 2)
	running := 1

	go func() {
		results <- serverResult{
			name: "http",
			err:  httpServer.RunOnListener(serveCtx, listeners.http),
		}
	}()

	if audioCfg.Enabled {
		running++

		go func() {
			results <- serverResult{
				name: "audiosocket",
				err: listeners.audio.Serve(
					serveCtx,
					manager.Start,
				),
			}
		}()
	}

	// Both required ports were already bound successfully.
	httpServer.SetReady(true)

	// Wait for shutdown or unexpected server termination.
	select {
	case <-ctx.Done():
		// Normal application shutdown.

	case stopped := <-results:
		running--

		if stopped.err != nil {
			resultErr = fmt.Errorf(
				"%s server failed: %w",
				stopped.name,
				stopped.err,
			)
		} else if ctx.Err() == nil {
			resultErr = fmt.Errorf(
				"%s server stopped unexpectedly",
				stopped.name,
			)
		}
	}

	// Stop advertising readiness before draining.
	httpServer.SetReady(false)

	// Reject new sessions before cancelling listeners.
	manager.BeginDrain()
	cancel()

	// Existing calls may finish naturally, then are
	// forcibly closed if their grace period expires.
	if audioCfg.Enabled {
		if err := session.Drain(
			manager,
			audioCfg.GracePeriod,
			audioCfg.CleanupTimeout,
		); err != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("drain AudioSocket sessions: %w", err),
			)
		}
	}

	// Both servers must finish their shutdown paths.
	for range running {
		stopped := <-results

		if stopped.err != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf(
					"%s shutdown: %w",
					stopped.name,
					stopped.err,
				),
			)
		}
	}

	return resultErr
}
