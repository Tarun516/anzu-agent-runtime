package main

import (
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/session"
)

// Preserve the existing lab API and its tests.
// Share the shutdown implementation with cmd/realtime.
func drainSessions(
	manager *sessionManager,
	gracePeriod time.Duration,
	cleanupTimeout time.Duration,
) error {
	return session.Drain(manager, gracePeriod, cleanupTimeout)
}
