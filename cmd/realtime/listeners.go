package main

import (
	"errors"
	"fmt"
	"net"

	"github.com/Tarun516/anzu-agent-runtime/internal/config"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/transport"
)

// boundListeners owns the listening sockets acquired during startup.
// These are listening sockets, not active call connections.
type boundListeners struct {
	http  net.Listener
	audio *transport.Listener
}

// bindListeners acquires the HTTP port first, then the
// optional AudioSocket port.
//
// If the second bind fails, the first listener is closed.
func bindListeners(
	httpAddress string,
	audioCfg config.AudioSocket,
) (*boundListeners, error) {
	httpListener, err := net.Listen("tcp", httpAddress)
	if err != nil {
		return nil, fmt.Errorf(
			"bind HTTP listener %q: %w",
			httpAddress,
			err,
		)
	}

	listeners := &boundListeners{
		http: httpListener,
	}

	// Preserve HTTP-only mode while media is optional.
	if !audioCfg.Enabled {
		return listeners, nil
	}

	if audioCfg.ListenAddress == "" {
		_ = listeners.Close()
		return nil, errors.New(
			"AudioSocket enabled without listen address",
		)
	}

	audioListener, err := transport.Listen(
		audioCfg.ListenAddress,
	)
	if err != nil {
		_ = listeners.Close()

		return nil, fmt.Errorf(
			"bind AudioSocket listener %q: %w",
			audioCfg.ListenAddress,
			err,
		)
	}

	listeners.audio = audioListener
	return listeners, nil
}

// Close releases the listening sockets.
//
// Active call connections are owned separately by
// session.Manager, not by this structure.
func (l *boundListeners) Close() error {
	var httpErr, audioErr error

	if l.audio != nil {
		audioErr = l.audio.Close()
	}

	if l.http != nil {
		httpErr = l.http.Close()

		if errors.Is(httpErr, net.ErrClosed) {
			httpErr = nil
		}
	}

	return errors.Join(httpErr, audioErr)
}
