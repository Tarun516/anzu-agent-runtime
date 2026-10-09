package audiosocket

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	TypeHangup = 0x00
	TypeUUID   = 0x01
	TypeDTMF   = 0x03
	TypeAudio  = 0x10 // 8 kHz mono PCM16
	TypeError  = 0xff

	// A lab safety limit; not the protocol maximum.
	MaxPayloadSize = 4096
)

type Frame struct {
	Type    byte
	Payload []byte
}

func ReadFrame(r io.Reader) (Frame, error) {
	var header [3]byte

	// TCP may deliver only part of the header.
	// ReadFull waits until all 3 bytes arrive.
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, fmt.Errorf("read header: %w", err)
	}

	frameType := header[0]

	// AudioSocket's length uses big-endian byte order.
	length := int(binary.BigEndian.Uint16(header[1:3]))

	if length > MaxPayloadSize {
		return Frame{}, fmt.Errorf(
			"payload too large: %d", length,
		)
	}

	payload := make([]byte, length)

	// Again, one Read() is not guaranteed to return
	// the complete payload.
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, fmt.Errorf("read payload: %w", err)
	}

	frame := Frame{
		Type:    frameType,
		Payload: payload,
	}

	if err := validateFrame(frame); err != nil {
		return Frame{}, err
	}

	return frame, nil
}

func WriteFrame(w io.Writer, frame Frame) error {
	if err := validateFrame(frame); err != nil {
		return err
	}

	var header [3]byte
	header[0] = frame.Type

	binary.BigEndian.PutUint16(
		header[1:3],
		uint16(len(frame.Payload)),
	)

	if err := writeAll(w, header[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	if err := writeAll(w, frame.Payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}

	return nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func validateFrame(f Frame) error {
	n := len(f.Payload)

	if n > MaxPayloadSize {
		return fmt.Errorf("payload too large: %d", n)
	}

	switch f.Type {
	case TypeHangup:
		if n != 0 {
			return fmt.Errorf("hangup must have empty payload")
		}
	case TypeUUID:
		if n != 16 {
			return fmt.Errorf("UUID requires 16 bytes, got %d", n)
		}
	case TypeDTMF:
		if n != 1 {
			return fmt.Errorf("DTMF requires 1 byte, got %d", n)
		}
	case TypeAudio:
		if n == 0 || n%2 != 0 {
			return fmt.Errorf("invalid PCM16 payload size: %d", n)
		}
	case TypeError:
		// Error payloads may contain protocol-specific details.
	default:
		return fmt.Errorf("unsupported frame type: 0x%02x", f.Type)
	}
	return nil
}
