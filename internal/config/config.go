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

type Service struct {
	Name            string
	Address         string
	ShutdownTimeout time.Duration
}

func LoadService(name, defaultAddress string) (Service, error) {
	if name == "" {
		return Service{}, fmt.Errorf("service name cannot be empty")
	}

	addressKey := "ANZU_" + envName(name) + "_ADDR"

	address := getEnv(addressKey, defaultAddress)

	if err := validateAddress(address); err != nil {
		return Service{}, fmt.Errorf("%s: %w", addressKey, err)
	}

	shutdownTimeout, err := durationFromEnv(
		"ANZU_SHUTDOWN_TIMEOUT_SECONDS",
		defaultShutdownTimeout,
	)
	if err != nil {
		return Service{}, err
	}

	return Service{
		Name:            name,
		Address:         address,
		ShutdownTimeout: shutdownTimeout,
	}, nil
}

func envName(name string) string {
	name = strings.ReplaceAll(name, "-", "_")
	return strings.ToUpper(name)
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}

func validateAddress(address string) error {
	if _, _, err := net.SplitHostPort(address); err != nil {
		return fmt.Errorf("invalid listen address %q: %w", address, err)
	}

	return nil
}

func durationFromEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)

	if value == "" {
		return fallback, nil
	}

	seconds, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}

	if seconds <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", key)
	}

	return time.Duration(seconds) * time.Second, nil
}
