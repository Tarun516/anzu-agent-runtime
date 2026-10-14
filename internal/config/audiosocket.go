package config

import (
	"fmt"
	"net"
	"os"
	"time"
)

// AudioSocket contains the configuration for Anzu's TCP media listener.
//
// This is separate from Service because HTTP and telephone audio
// have different listen addresses and shutdown requirements.
//
// Enabled allows the existing HTTP-only realtime process to remain
// usable while media integration is introduced incrementally.
type AudioSocket struct {
	Enabled        bool
	ListenAddress  string
	GracePeriod    time.Duration
	CleanupTimeout time.Duration
}

// LoadAudioSocket reads and validates the media listener configuration.
//
// An unset listen address disables media for the current development
// deployment. An explicitly configured listener must use a specific
// network interface rather than an unrestricted wildcard address.
func LoadAudioSocket() (AudioSocket, error) {
	// 1. Read the media listener address.
	address := os.Getenv("ANZU_AUDIO_LISTEN_ADDR")

	// 2. Preserve the existing HTTP-only runtime when media
	// has not been configured yet.
	if address == "" {
		return AudioSocket{Enabled: false}, nil
	}

	// 3. Reject malformed network addresses before startup.
	if err := validateAddress(address); err != nil {
		return AudioSocket{}, fmt.Errorf(
			"ANZU_AUDIO_LISTEN_ADDR: %w", err,
		)
	}

	// 4. Require an explicit bind interface for this stage.
	// AudioSocket does not yet authenticate the connecting TCP peer,
	// so accidentally listening on all interfaces is unsafe.
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return AudioSocket{}, fmt.Errorf(
			"parse AudioSocket address: %w", err,
		)
	}

	if host == "" || host == "0.0.0.0" || host == "::" {
		return AudioSocket{}, fmt.Errorf(
			"ANZU_AUDIO_LISTEN_ADDR must specify a non-wildcard host",
		)
	}

	// 5. Load the time allowed for existing calls to end naturally.
	grace, err := durationFromEnv(
		"ANZU_AUDIO_GRACE_SECONDS",
		30*time.Second,
	)
	if err != nil {
		return AudioSocket{}, err
	}

	// 6. Load the additional time allowed for handler cleanup
	// after remaining connections are forcibly closed.
	cleanup, err := durationFromEnv(
		"ANZU_AUDIO_CLEANUP_SECONDS",
		5*time.Second,
	)
	if err != nil {
		return AudioSocket{}, err
	}

	// 7. Return the validated media-specific configuration.
	return AudioSocket{
		Enabled:        true,
		ListenAddress:  address,
		GracePeriod:    grace,
		CleanupTimeout: cleanup,
	}, nil
}
