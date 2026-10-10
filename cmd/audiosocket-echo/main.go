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

// main is the entry point for the AudioSocket echo server.
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

	// 4. Stop accepting new connections when shutdown begins.
	//
	// Accept() is a blocking operation. Cancelling a context
	// alone does not interrupt a blocked Accept().
	//
	// Closing the listener wakes Accept() with an error,
	// allowing the main goroutine to exit.
	go func() {
		<-ctx.Done()

		log.Println("shutdown requested; closing listener")

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
				log.Println("AudioSocket listener stopped")
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

		// 8. Start an independent handler for the new connection.
		//
		// Each handler owns its TCP connection and processes
		// one AudioSocket call session.
		//
		// The handler is implemented in session.go.
		go handleConnection(conn)
	}
}
