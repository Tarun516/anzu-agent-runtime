package main

import (
	"net"
	"testing"

	"github.com/Tarun516/anzu-agent-runtime/internal/config"
)

func TestBindListenersBindsBothPorts(t *testing.T) {
	listeners, err := bindListeners(
		"127.0.0.1:0",
		config.AudioSocket{
			Enabled:       true,
			ListenAddress: "127.0.0.1:0",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer listeners.Close()

	if listeners.http == nil {
		t.Fatal("HTTP listener missing")
	}

	if listeners.audio == nil {
		t.Fatal("AudioSocket listener missing")
	}

	if listeners.http.Addr().String() ==
		listeners.audio.Addr().String() {
		t.Fatal("HTTP and AudioSocket bound same address")
	}
}

func TestBindListenersRollsBackOnAudioFailure(t *testing.T) {
	// Occupy a port to force AudioSocket binding to fail.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	// Reserve an available HTTP port, then release it.
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	httpAddress := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}

	// HTTP bind should succeed, AudioSocket bind should fail.
	listeners, err := bindListeners(
		httpAddress,
		config.AudioSocket{
			Enabled:       true,
			ListenAddress: occupied.Addr().String(),
		},
	)

	if err == nil {
		if listeners != nil {
			_ = listeners.Close()
		}
		t.Fatal("expected AudioSocket bind failure")
	}

	if listeners != nil {
		_ = listeners.Close()
		t.Fatal("expected no listeners on startup failure")
	}

	// If rollback worked, we can bind the HTTP port again.
	probe, err := net.Listen("tcp", httpAddress)
	if err != nil {
		t.Fatalf("HTTP listener leaked after failure: %v", err)
	}
	defer probe.Close()
}
