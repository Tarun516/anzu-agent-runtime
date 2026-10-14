package transport_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/session"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/transport"
)

func TestListenerPreservesAdmittedSessionOnShutdown(t *testing.T) {
	const timeout = 3 * time.Second

	handlerStarted := make(chan struct{}, 1)

	// 1. Create a reusable manager with a tiny echo handler.
	manager, err := session.NewManager(func(conn net.Conn) {
		defer conn.Close()

		handlerStarted <- struct{}{}

		var data [1]byte
		if _, err := io.ReadFull(conn, data[:]); err != nil {
			return
		}

		_, _ = conn.Write(data[:])
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Bind a real local TCP listener.
	listener, err := transport.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer func() {
		cancel()
		_ = listener.Close()
		manager.BeginDrain()
		manager.CloseActive()
	}()

	// 3. Connect the listener directly to manager.Start.
	serveDone := make(chan error, 1)

	go func() {
		serveDone <- listener.Serve(ctx, manager.Start)
	}()

	client, err := net.DialTimeout(
		"tcp",
		listener.Addr().String(),
		timeout,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// 4. Prove the connection reached the handler.
	select {
	case <-handlerStarted:
	case <-time.After(timeout):
		t.Fatal("session handler did not start")
	}

	if got := manager.ActiveCount(); got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}

	// 5. Stop new admission and cancel the listener.
	manager.BeginDrain()
	cancel()

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("listener shutdown: %v", err)
		}
	case <-time.After(timeout):
		t.Fatal("listener did not stop")
	}

	// 6. Existing connection must still belong to the manager.
	if got := manager.ActiveCount(); got != 1 {
		t.Fatalf("active after listener stop = %d, want 1", got)
	}

	// 7. Send data AFTER the listener has stopped.
	if err := client.SetDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Write([]byte{0x42}); err != nil {
		t.Fatal(err)
	}

	var echoed [1]byte
	if _, err := io.ReadFull(client, echoed[:]); err != nil {
		t.Fatal(err)
	}

	if echoed[0] != 0x42 {
		t.Fatalf("echo = 0x%02x, want 0x42", echoed[0])
	}

	// 8. The handler should now exit naturally.
	waitDone := make(chan struct{})

	go func() {
		manager.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(timeout):
		t.Fatal("session did not finish")
	}

	if got := manager.ActiveCount(); got != 0 {
		t.Fatalf("remaining sessions = %d, want 0", got)
	}
}
