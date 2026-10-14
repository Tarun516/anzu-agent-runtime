package main

import (
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/session"
)

// Keep the old lab type name while using the reusable manager.
// This is a type alias, not a second manager implementation.
type sessionManager = session.Manager

// newSessionManager preserves the existing lab constructor.
//
// The actual connection lifecycle is delegated to session.Manager.
// We inject the original, already-tested AudioSocket handler.
func newSessionManager() *sessionManager {
	manager, err := session.NewManager(handleConnection)
	if err != nil {
		// handleConnection is a known non-nil function.
		// An error here violates our construction invariant.
		panic(err)
	}

	return manager
}
