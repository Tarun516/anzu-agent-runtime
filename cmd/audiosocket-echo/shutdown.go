package main

import (
	"fmt"
	"log"
	"time"
)

// drainSessions waits for admitted sessions to finish naturally.
//
// Admission is sealed before waiting. If the grace period expires,
// remaining connections are closed to unblock their handlers.
//
// If handlers still fail to exit within cleanupTimeout, the function
// returns an error rather than waiting indefinitely.
func drainSessions(
	manager *sessionManager,
	gracePeriod time.Duration,
	cleanupTimeout time.Duration,
) error {
	// 1. Prevent new handler registrations.
	//
	// The manager synchronizes BeginDrain with Start, so no new
	// WaitGroup.Add calls can race with the following Wait.
	manager.BeginDrain()

	// 2. Observe handler completion without blocking this goroutine.
	done := make(chan struct{})

	go func() {
		manager.Wait()
		close(done)
	}()

	// 3. Allow currently active calls to finish naturally.
	graceTimer := time.NewTimer(gracePeriod)
	defer graceTimer.Stop()

	select {
	case <-done:
		log.Println("all AudioSocket sessions finished gracefully")
		return nil

	case <-graceTimer.C:
		log.Println("shutdown grace period expired")
	}

	// 4. Force-close any connections still registered.
	//
	// Connection closure should unblock pending socket I/O.
	closed := manager.CloseActive()

	log.Printf(
		"forced shutdown: closed %d active connections",
		closed,
	)

	// 5. Give the handlers a separate, bounded cleanup window.
	cleanupTimer := time.NewTimer(cleanupTimeout)
	defer cleanupTimer.Stop()

	select {
	case <-done:
		log.Println("all AudioSocket sessions stopped")
		return nil

	case <-cleanupTimer.C:
		return fmt.Errorf(
			"shutdown cleanup timed out: %d sessions remain",
			manager.ActiveCount(),
		)
	}
}
