package config

import (
	"strings"
	"testing"
	"time"
)

// TestLoadAudioSocketDisabledByDefault ensures that introducing
// media configuration does not unexpectedly start a listener.
func TestLoadAudioSocketDisabledByDefault(t *testing.T) {
	// 1. Simulate a deployment without media configuration.
	t.Setenv("ANZU_AUDIO_LISTEN_ADDR", "")

	// 2. Load the configuration.
	cfg, err := LoadAudioSocket()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	// 3. The existing HTTP-only process must remain supported.
	if cfg.Enabled {
		t.Fatal("expected AudioSocket to be disabled")
	}
}

// TestLoadAudioSocketUsesConfiguredValues verifies that audio
// has its own address and shutdown budgets.
func TestLoadAudioSocketUsesConfiguredValues(t *testing.T) {
	// 1. Configure a private local development listener.
	t.Setenv("ANZU_AUDIO_LISTEN_ADDR", "127.0.0.1:9019")
	t.Setenv("ANZU_AUDIO_GRACE_SECONDS", "45")
	t.Setenv("ANZU_AUDIO_CLEANUP_SECONDS", "7")

	// 2. Load the configuration.
	cfg, err := LoadAudioSocket()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	// 3. Verify media was enabled with the requested settings.
	if !cfg.Enabled {
		t.Fatal("expected AudioSocket to be enabled")
	}

	if cfg.ListenAddress != "127.0.0.1:9019" {
		t.Fatalf("unexpected listen address: %q", cfg.ListenAddress)
	}

	if cfg.GracePeriod != 45*time.Second {
		t.Fatalf("unexpected grace period: %v", cfg.GracePeriod)
	}

	if cfg.CleanupTimeout != 7*time.Second {
		t.Fatalf("unexpected cleanup timeout: %v", cfg.CleanupTimeout)
	}
}

// TestLoadAudioSocketRejectsUnsafeAddress ensures the media
// listener cannot accidentally bind to every interface.
func TestLoadAudioSocketRejectsUnsafeAddress(t *testing.T) {
	// 1. Configure an unrestricted listener.
	t.Setenv("ANZU_AUDIO_LISTEN_ADDR", "0.0.0.0:9019")

	// 2. Startup configuration must reject it.
	_, err := LoadAudioSocket()
	if err == nil || !strings.Contains(err.Error(), "non-wildcard") {
		t.Fatalf("expected unsafe address rejection, got %v", err)
	}
}

// TestLoadAudioSocketRejectsInvalidGrace ensures shutdown
// configuration cannot silently accept an invalid duration.
func TestLoadAudioSocketRejectsInvalidGrace(t *testing.T) {
	// 1. Enable media but supply an invalid grace period.
	t.Setenv("ANZU_AUDIO_LISTEN_ADDR", "127.0.0.1:9019")
	t.Setenv("ANZU_AUDIO_GRACE_SECONDS", "0")

	// 2. Verify that configuration loading fails.
	_, err := LoadAudioSocket()
	if err == nil {
		t.Fatal("expected invalid grace period error")
	}
}
