package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

const testTimeout = 3 * time.Second

// TestListenerAcceptsAndStops verifies that the listener transfers
// an accepted connection and exits when its context is cancelled.
func TestListenerAcceptsAndStops(t *testing.T) {
	// 1. Use an operating-system-assigned local port.
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	admitted := make(chan net.Conn, 1)
	done := make(chan error, 1)

	// 2. Run the actual accept loop.
	go func() {
		done <- listener.Serve(ctx, func(conn net.Conn) bool {
			admitted <- conn
			return true
		})
	}()

	// 3. Connect a simulated Asterisk client.
	client, err := net.DialTimeout(
		"tcp",
		listener.Addr().String(),
		testTimeout,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// 4. Verify the accepted socket was transferred.
	select {
	case conn := <-admitted:
		defer conn.Close()
	case <-time.After(testTimeout):
		t.Fatal("connection was not admitted")
	}

	// 5. Cancel the parent lifecycle.
	cancel()

	// 6. The accept loop must exit cleanly.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Serve did not stop after cancellation")
	}
}

// TestListenerClosesRejectedConnections checks the ownership
// contract when a session manager declines a new connection.
func TestListenerClosesRejectedConnections(t *testing.T) {
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rejected := make(chan struct{}, 1)
	done := make(chan error, 1)

	go func() {
		done <- listener.Serve(ctx, func(conn net.Conn) bool {
			rejected <- struct{}{}
			return false
		})
	}()

	// 1. Connect to the listener.
	client, err := net.DialTimeout(
		"tcp",
		listener.Addr().String(),
		testTimeout,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// 2. Wait until the callback rejects the connection.
	select {
	case <-rejected:
	case <-time.After(testTimeout):
		t.Fatal("admission callback was not called")
	}

	// 3. Rejection must result in connection closure.
	if err := client.SetReadDeadline(
		time.Now().Add(testTimeout),
	); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 1)
	_, err = client.Read(buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after rejection, got %v", err)
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Serve did not stop")
	}
}

// TestListenerReportsUnexpectedClose verifies that an
// unplanned listener failure is not treated as graceful shutdown.
func TestListenerReportsUnexpectedClose(t *testing.T) {
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- listener.Serve(ctx, func(conn net.Conn) bool {
			return false
		})
	}()

	// 1. Close the listener without cancelling its parent.
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. This should be reported as an unexpected stop.
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected unexpected-close error")
		}
	case <-time.After(testTimeout):
		t.Fatal("Serve did not report listener closure")
	}
}

// TestListenerReportsBindFailure verifies startup errors are
// returned immediately when the requested port is occupied.
func TestListenerReportsBindFailure(t *testing.T) {
	// 1. Reserve an operating-system-assigned local port.
	first, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	// 2. Attempt to bind the same TCP address again.
	second, err := Listen(first.Addr().String())
	if err == nil {
		second.Close()
		t.Fatal("expected second bind to fail")
	}
}
