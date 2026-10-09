package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 1. Read the address on which our Go server listens.
	// We will bind to the laptop's Tailscale IP.
	addr := os.Getenv("ANZU_AUDIO_LISTEN_ADDR")
	if addr == "" {
		log.Fatal("ANZU_AUDIO_LISTEN_ADDR is required")
	}

	// 2. Open a TCP listener.
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// 3. Arrange for Ctrl+C or SIGTERM to stop the listener.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	log.Printf("AudioSocket lab listening on %s", addr)

	// 4. Accept incoming TCP connections.
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}

			log.Printf("accept error: %v", err)
			continue
		}

		// 5. Handle each connection independently.
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	log.Printf("client connected: %s", conn.RemoteAddr())

	// For CP2.1 only, consume data without interpreting it.
	// CP2.2 replaces this with AudioSocket frame parsing.
	n, err := io.Copy(io.Discard, conn)

	log.Printf(
		"client disconnected: remote=%s bytes=%d err=%v",
		conn.RemoteAddr(),
		n,
		err,
	)
}
