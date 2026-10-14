package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// Listener owns one bound TCP listening socket.
//
// It accepts connections but does not process AudioSocket frames.
// Once a connection is admitted, its ownership passes to the
// component responsible for that session.
type Listener struct {
	listener net.Listener
}

// Listen binds a TCP address immediately.
//
// Binding before Serve is important: the application can detect
// a missing interface or occupied port before declaring readiness.
func Listen(address string) (*Listener, error) {
	// 1. Ask the operating system to bind the TCP address.
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf(
			"bind AudioSocket listener %q: %w",
			address,
			err,
		)
	}

	// 2. Transfer ownership of the listening socket.
	return &Listener{listener: ln}, nil
}

// Addr returns the actual address of the bound listener.
// It is useful for logs and tests using an ephemeral port.
func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}

// Close releases the listening socket.
//
// Closing a listener interrupts a blocked Accept call,
// but does not close connections that were already admitted.
// Those connections belong to their respective session handlers.
func (l *Listener) Close() error {
	err := l.listener.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// Serve accepts connections until cancellation or failure.
//
// admit must quickly transfer accepted connections to a session
// manager and return true if ownership was accepted.
// If admit returns false, Serve closes the rejected connection.
//
// Serve does not launch session goroutines or own admitted sockets.
func (l *Listener) Serve(
	ctx context.Context,
	admit func(net.Conn) bool,
) error {
	// 1. Reject an invalid callback rather than panic later.
	if admit == nil {
		_ = l.Close()
		return errors.New("AudioSocket admission callback is nil")
	}

	// 2. Close the listener when the parent lifecycle ends.
	// Closing it wakes an otherwise blocked Accept call.
	stopClosing := context.AfterFunc(ctx, func() {
		_ = l.Close()
	})
	defer stopClosing()

	// 3. Always release the listening socket when Serve returns.
	defer l.Close()

	// 4. Stop before accepting work if already cancelled.
	if ctx.Err() != nil {
		return nil
	}

	for {
		// 5. Accept one incoming TCP connection.
		conn, err := l.listener.Accept()
		if err != nil {
			// Cancellation is a normal shutdown condition.
			if ctx.Err() != nil {
				return nil
			}

			// An unexpected listener failure must reach
			// the parent supervisor instead of being hidden.
			return fmt.Errorf(
				"accept AudioSocket connection: %w",
				err,
			)
		}

		// 6. Avoid admitting a connection after cancellation
		// if Accept completed at approximately the same time.
		if ctx.Err() != nil {
			_ = conn.Close()
			return nil
		}

		// 7. Transfer responsibility for this connection.
		// The future session manager will implement admission.
		if !admit(conn) {
			_ = conn.Close()
		}
	}
}
