package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

// main is the entry point for the AudioSocket echo server,this is what gets connected to the asterisk server for the communication and data transfer.
//
// It initializes the TCP listener, accepts incoming connections,
// and delegates each connection to its own session handler.
//
// The listener's lifetime is controlled by OS shutdown signals.
func main() {
	// 1. Load the address on which the TCP server should listen.
	//
	// During development, we bind to the laptop's Tailscale IP
	// instead of exposing the listener on every interface.
	addr := os.Getenv("ANZU_AUDIO_LISTEN_ADDR")
	if addr == "" {
		log.Fatal("ANZU_AUDIO_LISTEN_ADDR is required")
	}

	// 2. Create the root context for the process.
	//
	// Ctrl+C sends SIGINT. Containers and process managers
	// commonly use SIGTERM to request shutdown.
	//
	// NotifyContext cancels ctx when either signal arrives.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// 3. Create the TCP listener.
	//
	// net.Listen binds the address and starts accepting
	// incoming TCP connections from clients such as Asterisk.
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("create AudioSocket listener: %v", err)
	}
	defer listener.Close()

	// 4. Track all accepted sessions and coordinate their shutdown.
	manager := newSessionManager()

	// 4. Stop accepting new connections when shutdown begins.
	//
	// Accept() is a blocking operation. Cancelling a context
	// alone does not interrupt a blocked Accept().
	//
	// Closing the listener wakes Accept() with an error,
	// allowing the main goroutine to exit.
	go func() {
		// 1. Wait for SIGINT or SIGTERM.
		<-ctx.Done()

		// 2. Reject new sessions before closing the listener.
		manager.BeginDrain()
		log.Println("shutdown requested; draining active sessions")

		// 3. Wake the blocked Accept call.
		if err := listener.Close(); err != nil &&
			!errors.Is(err, net.ErrClosed) {
			log.Printf("close listener: %v", err)
		}
	}()

	log.Printf("AudioSocket server listening on %s", addr)

	// 5. Continuously accept incoming TCP connections.
	//
	// Each accepted connection is independent.
	// Multiple phone calls may therefore be handled concurrently.
	for {
		conn, err := listener.Accept()
		if err != nil {
			// 6. Exit normally if the application is shutting down.
			//
			// Closing the listener during shutdown causes Accept()
			// to return an error. This is expected behavior.
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				// 1. Seal admission before waiting for handlers.
				manager.BeginDrain()

				// 2. Wait until every admitted session finishes.
				log.Printf(
					"listener stopped; waiting for %d active sessions",
					manager.ActiveCount(),
				)

				manager.Wait()

				// 3. Exit only after session handlers complete.
				log.Println("all AudioSocket sessions finished")
				return
			}

			// 7. Report unexpected accept failures without
			// immediately terminating the entire process.
			//
			// A production implementation should classify errors
			// and back off on repeated failures to avoid busy loops.
			log.Printf("accept connection failed: %v", err)
			continue
		}

		// 8. Prevent admission if the signal arrived during Accept.
		if ctx.Err() != nil {
			manager.BeginDrain()
		}

		// 9. Register the connection before starting its handler.
		// Start closes the connection when draining is active.
		if !manager.Start(conn) {
			log.Printf(
				"AudioSocket connection rejected: remote=%s",
				conn.RemoteAddr(),
			)
		}
	}
}
