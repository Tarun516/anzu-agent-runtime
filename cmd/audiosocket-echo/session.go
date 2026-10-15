package main

import (
	"net"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/echo"
)

// Preserve the names used by the existing lab tests.
// The actual timeout policy now lives in the echo package.
const (
	initialFrameTimeout = echo.InitialFrameTimeout
	idleReadTimeout     = echo.IdleReadTimeout
	frameWriteTimeout   = echo.FrameWriteTimeout
)

// Preserve the lab's existing handler interface.
// Delegate actual AudioSocket processing to the reusable package.
func handleConnection(conn net.Conn) {
	echo.HandleConnection(conn)
}

// Preserve the UUID formatting helper used by existing tests.
func formatCallUUID(id []byte) string {
	return echo.FormatCallUUID(id)
}
