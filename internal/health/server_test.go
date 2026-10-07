package health

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newTestLogger creates a logger that discards output.
//
// Tests generally should not fill the terminal with logs unless
// the log output itself is what we are testing.
func newTestLogger() *slog.Logger {
	// 1. Create a text handler whose output goes nowhere.
	handler := slog.NewTextHandler(
		io.Discard,
		nil,
	)

	// 2. Return a logger using that handler.
	return slog.New(handler)
}

// TestHealthEndpoint verifies that the liveness endpoint
// reports a healthy process using HTTP 200 and JSON.
func TestHealthEndpoint(t *testing.T) {
	// 1. Create the server under test.
	server := NewServer(
		"realtime",
		":8081",
		10*time.Second,
		newTestLogger(),
	)

	// 2. Create an in-memory HTTP request.
	request := httptest.NewRequest(
		http.MethodGet,
		"/healthz",
		nil,
	)

	// 3. Create an in-memory HTTP response recorder.
	response := httptest.NewRecorder()

	// 4. Execute the handler directly.
	server.health(
		response,
		request,
	)

	// 5. Verify the HTTP status code.
	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusOK,
			response.Code,
		)
	}

	// 6. Decode the JSON response body.
	var body map[string]string

	if err := json.NewDecoder(
		response.Body,
	).Decode(&body); err != nil {
		t.Fatalf(
			"decode response body: %v",
			err,
		)
	}

	// 7. Verify the liveness response.
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

// TestReadinessEndpointWhenNotReady verifies that a process
// which has not declared readiness returns HTTP 503.
func TestReadinessEndpointWhenNotReady(t *testing.T) {
	// 1. Construct a new server.
	//
	// atomic.Bool's zero value is false, so the server
	// begins in the not-ready state.
	server := NewServer(
		"realtime",
		":8081",
		10*time.Second,
		newTestLogger(),
	)

	// 2. Create an in-memory readiness request.
	request := httptest.NewRequest(
		http.MethodGet,
		"/readyz",
		nil,
	)

	// 3. Create an in-memory response recorder.
	response := httptest.NewRecorder()

	// 4. Execute the readiness handler.
	server.readiness(
		response,
		request,
	)

	// 5. The process is alive but should not receive new work.
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusServiceUnavailable,
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

	// 7. Verify the reported readiness state.
	if body["status"] != "not_ready" {
		t.Errorf(
			"expected status %q, got %q",
			"not_ready",
			body["status"],
		)
	}
}

// TestReadinessEndpointWhenReady verifies that SetReady(true)
// causes the readiness endpoint to return HTTP 200.
func TestReadinessEndpointWhenReady(t *testing.T) {
	// 1. Construct the server.
	server := NewServer(
		"realtime",
		":8081",
		10*time.Second,
		newTestLogger(),
	)

	// 2. Mark the process ready to receive work.
	server.SetReady(true)

	// 3. Create an in-memory readiness request.
	request := httptest.NewRequest(
		http.MethodGet,
		"/readyz",
		nil,
	)

	// 4. Create an in-memory response recorder.
	response := httptest.NewRecorder()

	// 5. Execute the readiness handler.
	server.readiness(
		response,
		request,
	)

	// 6. Ready services should return HTTP 200.
	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusOK,
			response.Code,
		)
	}

	// 7. Decode the response.
	var body map[string]string

	if err := json.NewDecoder(
		response.Body,
	).Decode(&body); err != nil {
		t.Fatalf(
			"decode response body: %v",
			err,
		)
	}

	// 8. Verify the readiness response.
	if body["status"] != "ready" {
		t.Errorf(
			"expected status %q, got %q",
			"ready",
			body["status"],
		)
	}
}

// TestRunReturnsWhenContextIsCancelled verifies that the server
// respects application cancellation and completes graceful shutdown.
func TestRunReturnsWhenContextIsCancelled(t *testing.T) {
	// 1. Create a server using port 0.
	//
	// Port 0 asks the operating system to choose an available
	// ephemeral port if the server reaches the listen stage.
	server := NewServer(
		"realtime",
		"127.0.0.1:0",
		1*time.Second,
		newTestLogger(),
	)

	// 2. Create a cancellable application context.
	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	// 3. Cancel it before Run starts.
	//
	// This deterministically exercises the shutdown path
	// without sleeps or timing assumptions.
	cancel()

	// 4. Run should observe cancellation and return cleanly.
	if err := server.Run(ctx); err != nil {
		t.Fatalf(
			"expected graceful shutdown, got error: %v",
			err,
		)
	}
}
