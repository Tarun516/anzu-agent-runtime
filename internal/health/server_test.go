package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newTestLogger creates a logger that discards output.
//
// Tests should generally stay quiet unless logging behavior
// itself is what the test is verifying.
func newTestLogger() *slog.Logger {
	// 1. Create a log handler that discards output.
	handler := slog.NewTextHandler(
		io.Discard,
		nil,
	)

	// 2. Return a logger backed by that handler.
	return slog.New(handler)
}

// TestHealthEndpoint verifies that the liveness endpoint
// reports an alive process using HTTP 200 and JSON.
func TestHealthEndpoint(t *testing.T) {
	// 1. Create the health server under test.
	server := NewServer(
		"realtime",
		":8081",
		10*time.Second,
		newTestLogger(),
		nil,
	)

	// 2. Create an in-memory HTTP request.
	request := httptest.NewRequest(
		http.MethodGet,
		"/healthz",
		nil,
	)

	// 3. Create an in-memory response recorder.
	response := httptest.NewRecorder()

	// 4. Execute the liveness handler.
	server.health(
		response,
		request,
	)

	// 5. Liveness should return HTTP 200.
	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusOK,
			response.Code,
		)
	}

	// 6. Decode the JSON response.
	var body map[string]string

	if err := json.NewDecoder(
		response.Body,
	).Decode(&body); err != nil {
		t.Fatalf(
			"decode response body: %v",
			err,
		)
	}

	// 7. Verify the reported liveness state.
	if body["status"] != "ok" {
		t.Errorf(
			"expected status %q, got %q",
			"ok",
			body["status"],
		)
	}

	if body["service"] != "realtime" {
		t.Errorf(
			"expected service %q, got %q",
			"realtime",
			body["service"],
		)
	}
}

// TestReadinessEndpointWhenNotReady verifies that
// a closed lifecycle gate returns HTTP 503.
func TestReadinessEndpointWhenNotReady(t *testing.T) {
	// 1. Construct a new server.
	//
	// atomic.Bool begins with the zero value false,
	// so lifecycle readiness starts closed.
	server := NewServer(
		"realtime",
		":8081",
		10*time.Second,
		newTestLogger(),
		nil,
	)

	// 2. Create an in-memory readiness request.
	request := httptest.NewRequest(
		http.MethodGet,
		"/readyz",
		nil,
	)

	// 3. Create the response recorder.
	response := httptest.NewRecorder()

	// 4. Execute the readiness handler.
	server.readiness(
		response,
		request,
	)

	// 5. The process is alive but should not
	// currently receive new work.
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusServiceUnavailable,
			response.Code,
		)
	}

	// 6. Decode the response body.
	var body map[string]string

	if err := json.NewDecoder(
		response.Body,
	).Decode(&body); err != nil {
		t.Fatalf(
			"decode response body: %v",
			err,
		)
	}

	// 7. Verify the reported state.
	if body["status"] != "not_ready" {
		t.Errorf(
			"expected status %q, got %q",
			"not_ready",
			body["status"],
		)
	}
}

// TestReadinessEndpointWhenReady verifies that a service
// with an open lifecycle gate and no failing dependencies
// returns HTTP 200.
func TestReadinessEndpointWhenReady(t *testing.T) {
	// 1. Construct the server with no required dependency checks.
	server := NewServer(
		"realtime",
		":8081",
		10*time.Second,
		newTestLogger(),
		nil,
	)

	// 2. Open the lifecycle-readiness gate.
	server.SetReady(true)

	// 3. Create an in-memory readiness request.
	request := httptest.NewRequest(
		http.MethodGet,
		"/readyz",
		nil,
	)

	response := httptest.NewRecorder()

	// 4. Execute the readiness handler.
	server.readiness(
		response,
		request,
	)

	// 5. Readiness should succeed.
	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusOK,
			response.Code,
		)
	}

	// 6. Decode the response.
	var body map[string]string

	if err := json.NewDecoder(
		response.Body,
	).Decode(&body); err != nil {
		t.Fatalf(
			"decode response body: %v",
			err,
		)
	}

	// 7. Verify the readiness state.
	if body["status"] != "ready" {
		t.Errorf(
			"expected status %q, got %q",
			"ready",
			body["status"],
		)
	}
}

// TestReadinessEndpointWhenDependencyIsHealthy verifies that
// readiness succeeds when the lifecycle gate is open and
// every required dependency is available.
func TestReadinessEndpointWhenDependencyIsHealthy(
	t *testing.T,
) {
	// 1. Create a dependency check that always succeeds.
	checks := []ReadinessCheck{
		{
			Name: "postgres",
			Check: func(
				ctx context.Context,
			) error {
				return nil
			},
		},
	}

	// 2. Construct the health server.
	server := NewServer(
		"realtime",
		":8081",
		10*time.Second,
		newTestLogger(),
		checks,
	)

	// 3. Open the lifecycle-readiness gate.
	server.SetReady(true)

	// 4. Create an in-memory readiness request.
	request := httptest.NewRequest(
		http.MethodGet,
		"/readyz",
		nil,
	)

	response := httptest.NewRecorder()

	// 5. Execute the readiness handler.
	server.readiness(
		response,
		request,
	)

	// 6. Healthy dependencies should result in HTTP 200.
	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusOK,
			response.Code,
		)
	}
}

// TestReadinessEndpointWhenDependencyFails verifies that
// a required dependency failure makes the process unready.
func TestReadinessEndpointWhenDependencyFails(
	t *testing.T,
) {
	// 1. Create a dependency check that simulates
	// an unavailable PostgreSQL instance.
	checks := []ReadinessCheck{
		{
			Name: "postgres",
			Check: func(
				ctx context.Context,
			) error {
				return errors.New(
					"database unavailable",
				)
			},
		},
	}

	// 2. Construct the health server.
	server := NewServer(
		"realtime",
		":8081",
		10*time.Second,
		newTestLogger(),
		checks,
	)

	// 3. The application itself is willing to accept work.
	server.SetReady(true)

	// 4. Create an in-memory readiness request.
	request := httptest.NewRequest(
		http.MethodGet,
		"/readyz",
		nil,
	)

	response := httptest.NewRecorder()

	// 5. Execute the readiness handler.
	server.readiness(
		response,
		request,
	)

	// 6. Dependency failure should make the service unavailable.
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusServiceUnavailable,
			response.Code,
		)
	}
}

// TestReadinessDoesNotCheckDependenciesWhenLifecycleIsNotReady
// verifies that dependency checks are skipped when the application
// is already refusing new work.
func TestReadinessDoesNotCheckDependenciesWhenLifecycleIsNotReady(
	t *testing.T,
) {
	// 1. Track whether the dependency function executes.
	checkCalled := false

	checks := []ReadinessCheck{
		{
			Name: "postgres",
			Check: func(
				ctx context.Context,
			) error {
				checkCalled = true

				return nil
			},
		},
	}

	// 2. Construct the server.
	server := NewServer(
		"realtime",
		":8081",
		10*time.Second,
		newTestLogger(),
		checks,
	)

	// 3. Do not call SetReady(true).
	//
	// Lifecycle readiness therefore remains false.

	// 4. Create the readiness request.
	request := httptest.NewRequest(
		http.MethodGet,
		"/readyz",
		nil,
	)

	response := httptest.NewRecorder()

	// 5. Execute the handler.
	server.readiness(
		response,
		request,
	)

	// 6. Lifecycle readiness should fail immediately.
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusServiceUnavailable,
			response.Code,
		)
	}

	// 7. PostgreSQL should not be checked because
	// the lifecycle gate already determined the result.
	if checkCalled {
		t.Fatal(
			"expected dependency check to be skipped",
		)
	}
}

// TestReadinessEndpointWhenDependencyTimesOut verifies that
// a dependency which does not respond within the readiness
// deadline makes the process unready.
func TestReadinessEndpointWhenDependencyTimesOut(
	t *testing.T,
) {
	// 1. Create a dependency that waits until
	// its context is cancelled.
	checks := []ReadinessCheck{
		{
			Name: "postgres",
			Check: func(
				ctx context.Context,
			) error {
				<-ctx.Done()

				return ctx.Err()
			},
		},
	}

	// 2. Construct the server.
	server := NewServer(
		"realtime",
		":8081",
		10*time.Second,
		newTestLogger(),
		checks,
	)

	// 3. Use a very small timeout specifically for this test.
	//
	// This keeps the test fast while still testing
	// the actual timeout behavior.
	server.readinessTimeout = 10 * time.Millisecond

	// 4. Open the lifecycle-readiness gate.
	server.SetReady(true)

	request := httptest.NewRequest(
		http.MethodGet,
		"/readyz",
		nil,
	)

	response := httptest.NewRecorder()

	// 5. Execute the readiness handler.
	server.readiness(
		response,
		request,
	)

	// 6. Timeout should make the process unready.
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusServiceUnavailable,
			response.Code,
		)
	}
}

// TestRunReturnsWhenContextIsCancelled verifies that
// application cancellation drives the graceful shutdown path.
func TestRunReturnsWhenContextIsCancelled(t *testing.T) {
	// 1. Use port 0 so the operating system can choose
	// an available ephemeral port.
	server := NewServer(
		"realtime",
		"127.0.0.1:0",
		1*time.Second,
		newTestLogger(),
		nil,
	)

	// 2. Create a cancellable application context.
	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	// 3. Cancel before Run starts so the shutdown path
	// can be tested without arbitrary sleeps.
	cancel()

	// 4. Run should observe cancellation
	// and complete graceful shutdown.
	if err := server.Run(ctx); err != nil {
		t.Fatalf(
			"expected graceful shutdown, got error: %v",
			err,
		)
	}
}
