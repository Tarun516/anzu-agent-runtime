package session

import (
	"errors"
	"fmt"
	"time"
)

// Drain stops admission and waits for existing sessions.
// After the grace period, remaining connections are closed.
// The cleanup timeout bounds the final wait for handlers.
func Drain(
	manager *Manager,
	gracePeriod time.Duration,
	cleanupTimeout time.Duration,
) error {
	if manager == nil {
		return errors.New("session manager is nil")
	}

	if gracePeriod <= 0 || cleanupTimeout <= 0 {
		return errors.New("shutdown durations must be positive")
	}

	// No session may be admitted after draining begins.
	manager.BeginDrain()

	// Observe completion without blocking the coordinator.
	done := make(chan struct{})
	go func() {
		manager.Wait()
		close(done)
	}()

	graceTimer := time.NewTimer(gracePeriod)
	defer graceTimer.Stop()

	select {
	case <-done:
		return nil
	case <-graceTimer.C:
		// Existing calls exceeded the grace period.
	}

	// Interrupt outstanding reads and writes.
	manager.CloseActive()

	cleanupTimer := time.NewTimer(cleanupTimeout)
	defer cleanupTimer.Stop()

	select {
	case <-done:
		return nil
	case <-cleanupTimer.C:
		return fmt.Errorf(
			"AudioSocket cleanup timed out: %d active sessions",
			manager.ActiveCount(),
		)
	}
}
