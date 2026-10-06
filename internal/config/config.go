package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultShutdownTimeout = 10 * time.Second

// Service contains the runtime configuration required
// to start one Anzu process.
type Service struct {
	Name            string
	Address         string
	ShutdownTimeout time.Duration
}

// LoadService loads and validates configuration for a service.
//
// The service name is used to construct its environment-variable key.
//
// Example:
//
//	name = "realtime"
//
// produces:
//
//	ANZU_REALTIME_ADDR
func LoadService(
	name string,
	defaultAddress string,
) (Service, error) {
	// 1. Ensure the caller provided a valid service name.
	if name == "" {
		return Service{}, fmt.Errorf("service name cannot be empty")
	}

	// 2. Build the environment-variable key for this service.
	addressKey := "ANZU_" + envName(name) + "_ADDR"

	// 3. Read the configured address or use the supplied default.
	address := getEnv(
		addressKey,
		defaultAddress,
	)

	// 4. Validate the address before allowing the process to start.
	if err := validateAddress(address); err != nil {
		return Service{}, fmt.Errorf(
			"%s: %w",
			addressKey,
			err,
		)
	}

	// 5. Load the maximum amount of time allowed
	// for graceful shutdown.
	shutdownTimeout, err := durationFromEnv(
		"ANZU_SHUTDOWN_TIMEOUT_SECONDS",
		defaultShutdownTimeout,
	)
	if err != nil {
		return Service{}, err
	}

	// 6. Return fully validated configuration.
	return Service{
		Name:            name,
		Address:         address,
		ShutdownTimeout: shutdownTimeout,
	}, nil
}

// envName converts a service name into the format
// used inside environment-variable names.
//
// Examples:
//
//	realtime     -> REALTIME
//	voice-worker -> VOICE_WORKER
func envName(name string) string {
	// 1. Replace hyphens with underscores.
	name = strings.ReplaceAll(
		name,
		"-",
		"_",
	)

	// 2. Normalize the result to uppercase.
	return strings.ToUpper(name)
}

// getEnv returns an environment-variable value when present.
// If it is empty, the provided fallback value is returned.
func getEnv(
	key string,
	fallback string,
) string {
	// 1. Read the value from this process's environment.
	value := os.Getenv(key)

	// 2. Use the default when nothing has been configured.
	if value == "" {
		return fallback
	}

	// 3. Use the configured value.
	return value
}

// validateAddress verifies that an address follows host:port syntax.
//
// Valid examples:
//
//	:8081
//	127.0.0.1:8081
//	0.0.0.0:8081
//
// Invalid example:
//
//	banana
func validateAddress(address string) error {
	// 1. Parse the address into host and port components.
	_, _, err := net.SplitHostPort(address)

	// 2. Return a descriptive error if parsing fails.
	if err != nil {
		return fmt.Errorf(
			"invalid listen address %q: %w",
			address,
			err,
		)
	}

	return nil
}

// durationFromEnv reads a duration expressed as integer seconds
// from an environment variable.
//
// Example:
//
//	ANZU_SHUTDOWN_TIMEOUT_SECONDS=15
//
// becomes:
//
//	15 * time.Second
func durationFromEnv(
	key string,
	fallback time.Duration,
) (time.Duration, error) {
	// 1. Read the raw environment-variable value.
	value := os.Getenv(key)

	// 2. Use the fallback when no value has been configured.
	if value == "" {
		return fallback, nil
	}

	// 3. Environment variables are strings,
	// so convert the value into an integer.
	seconds, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf(
			"%s must be an integer: %w",
			key,
			err,
		)
	}

	// 4. Reject values that cannot represent
	// a meaningful graceful-shutdown period.
	if seconds <= 0 {
		return 0, fmt.Errorf(
			"%s must be greater than zero",
			key,
		)
	}

	// 5. Convert the integer number of seconds
	// into Go's time.Duration type.
	return time.Duration(seconds) * time.Second, nil
}
