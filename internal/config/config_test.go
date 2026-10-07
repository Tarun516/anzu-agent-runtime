package config

import (
	"strings"
	"testing"
	"time"
)

// TestLoadServiceUsesDefaults verifies that configuration falls back
// to the provided defaults when environment variables are not configured.
func TestLoadServiceUsesDefaults(t *testing.T) {
	// 1. Ensure these environment variables behave as unset
	// for the duration of this test.
	t.Setenv("ANZU_REALTIME_ADDR", "")
	t.Setenv("ANZU_SHUTDOWN_TIMEOUT_SECONDS", "")

	// 2. Load configuration for the realtime service.
	cfg, err := LoadService(
		"realtime",
		":8081",
	)
	if err != nil {
		t.Fatalf(
			"expected configuration to load successfully, got error: %v",
			err,
		)
	}

	// 3. Verify the service name.
	if cfg.Name != "realtime" {
		t.Errorf(
			"expected service name %q, got %q",
			"realtime",
			cfg.Name,
		)
	}

	// 4. Verify the default address was used.
	if cfg.Address != ":8081" {
		t.Errorf(
			"expected address %q, got %q",
			":8081",
			cfg.Address,
		)
	}

	// 5. Verify the default shutdown timeout was used.
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf(
			"expected shutdown timeout %v, got %v",
			10*time.Second,
			cfg.ShutdownTimeout,
		)
	}
}

// TestLoadServiceUsesEnvironmentValues verifies that environment
// variables override the supplied defaults.
func TestLoadServiceUsesEnvironmentValues(t *testing.T) {
	// 1. Configure values specifically for this test.
	t.Setenv(
		"ANZU_REALTIME_ADDR",
		"127.0.0.1:9000",
	)

	t.Setenv(
		"ANZU_SHUTDOWN_TIMEOUT_SECONDS",
		"20",
	)

	// 2. Load the service configuration.
	cfg, err := LoadService(
		"realtime",
		":8081",
	)
	if err != nil {
		t.Fatalf(
			"expected configuration to load successfully, got error: %v",
			err,
		)
	}

	// 3. Verify the environment address replaced the default.
	if cfg.Address != "127.0.0.1:9000" {
		t.Errorf(
			"expected address %q, got %q",
			"127.0.0.1:9000",
			cfg.Address,
		)
	}

	// 4. Verify the configured shutdown timeout.
	if cfg.ShutdownTimeout != 20*time.Second {
		t.Errorf(
			"expected shutdown timeout %v, got %v",
			20*time.Second,
			cfg.ShutdownTimeout,
		)
	}
}

// TestLoadServiceRejectsInvalidConfiguration verifies that invalid
// configuration prevents the service from starting.
func TestLoadServiceRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name          string
		address       string
		timeout       string
		expectedError string
	}{
		{
			name:          "invalid listen address",
			address:       "banana",
			timeout:       "",
			expectedError: "invalid listen address",
		},
		{
			name:          "timeout is not an integer",
			address:       ":8081",
			timeout:       "hello",
			expectedError: "must be an integer",
		},
		{
			name:          "timeout is zero",
			address:       ":8081",
			timeout:       "0",
			expectedError: "must be greater than zero",
		},
		{
			name:          "timeout is negative",
			address:       ":8081",
			timeout:       "-5",
			expectedError: "must be greater than zero",
		},
	}

	// 1. Run the same test behavior against several invalid inputs.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 2. Configure this individual test case.
			t.Setenv(
				"ANZU_REALTIME_ADDR",
				tt.address,
			)

			t.Setenv(
				"ANZU_SHUTDOWN_TIMEOUT_SECONDS",
				tt.timeout,
			)

			// 3. Attempt to load invalid configuration.
			_, err := LoadService(
				"realtime",
				":8081",
			)

			// 4. An error must be returned.
			if err == nil {
				t.Fatal(
					"expected configuration error, got nil",
				)
			}

			// 5. Verify the error explains the expected problem.
			if !strings.Contains(
				err.Error(),
				tt.expectedError,
			) {
				t.Errorf(
					"expected error containing %q, got %q",
					tt.expectedError,
					err.Error(),
				)
			}
		})
	}
}

// TestLoadServiceRejectsEmptyName verifies that a service
// cannot be created without an identity.
func TestLoadServiceRejectsEmptyName(t *testing.T) {
	// 1. Attempt to load a service without a name.
	_, err := LoadService(
		"",
		":8081",
	)

	// 2. The configuration should be rejected.
	if err == nil {
		t.Fatal(
			"expected error for empty service name, got nil",
		)
	}
}
