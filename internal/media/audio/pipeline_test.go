package audio

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

// testInputPipeline configures the same 8kHz→16kHz, 20ms audio route planned
// for Anzu's AudioSocket shadow mode. A larger buffer avoids test overload
// unless a test deliberately chooses a smaller queue.
func testInputPipeline(t *testing.T, maxFrames int, buffer time.Duration) *InputPipeline {
	t.Helper()
	p, err := NewInputPipeline(InputPipelineConfig{
		SourceRate: 8000, TargetRate: 16000, Channels: 1,
		ChunkSamples: 320,
		Queue: QueueConfig{SampleRate: 16000, Channels: 1,
			MaxFrames: maxFrames, MaxBuffered: buffer, Overflow: OverflowReject},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// pcmPacket creates the requested number of signed little-endian mono samples.
func pcmPacket(sampleCount int, value int16) []byte {
	out := make([]byte, sampleCount*2)
	for i := 0; i < sampleCount; i++ {
		binary.LittleEndian.PutUint16(out[i*2:], uint16(value))
	}
	return out
}

func TestInputPipelineTwo20msFrames(t *testing.T) {
	p := testInputPipeline(t, 10, 200*time.Millisecond)
	a := pcmPacket(160, 1234)
	b := pcmPacket(160, 1234)
	if err := p.FeedPCM16LE(a); err != nil {
		t.Fatal(err)
	}
	if err := p.FeedPCM16LE(b); err != nil {
		t.Fatal(err)
	}
	// Changing a source buffer later must not modify any queued samples.
	a[0] = 0
	b[0] = 0
	if err := p.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := p.Finish(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		delivery, err := p.Queue().Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		f := delivery.Frame
		if f.Sequence != uint64(i) || f.Timestamp != time.Duration(i)*20*time.Millisecond || f.Duration() != 20*time.Millisecond || f.SampleRate != 16000 || f.Channels != 1 || len(f.Samples) != 320 {
			t.Fatalf("frame %d: %+v", i, f)
		}
		if delivery.DroppedBefore != 0 {
			t.Fatal("unexpected audio discontinuity")
		}
		for _, s := range f.Samples {
			if s != 1234 {
				t.Fatalf("sample corrupted: got %d", s)
			}
		}
	}
	if _, err := p.Queue().Pop(context.Background()); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want drained queue close, got %v", err)
	}
}

func TestInputPipelinePacketSplittingAndTail(t *testing.T) {
	// Packet boundaries differ, but 40 ms total still gives two 20-ms chunks.
	p := testInputPipeline(t, 10, 200*time.Millisecond)
	for _, n := range []int{40, 40, 80, 80, 80} {
		if err := p.FeedPCM16LE(pcmPacket(n, 222)); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Finish(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		d, err := p.Queue().Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if d.Frame.Timestamp != time.Duration(i)*20*time.Millisecond || len(d.Frame.Samples) != 320 {
			t.Fatalf("unexpected packet chunk: %+v", d.Frame)
		}
	}
	if _, err := p.Queue().Pop(context.Background()); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("got %v", err)
	}

	// Short normal hangup flushes a partial 10-ms frame; no added silence.
	tail := testInputPipeline(t, 10, 200*time.Millisecond)
	if err := tail.FeedPCM16LE(pcmPacket(80, 42)); err != nil {
		t.Fatal(err)
	}
	if err := tail.Finish(); err != nil {
		t.Fatal(err)
	}
	d, err := tail.Queue().Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Frame.Samples) != 160 || d.Frame.Duration() != 10*time.Millisecond {
		t.Fatalf("tail frame: %+v", d.Frame)
	}
}

func TestInputPipelineQueueFullIsTerminal(t *testing.T) {
	p := testInputPipeline(t, 1, 20*time.Millisecond)
	var err error
	for i := 0; i < 3; i++ {
		err = p.FeedPCM16LE(pcmPacket(160, 12))
		if err != nil {
			break
		}
	}
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want queue full error, got %v", err)
	}
	if next := p.FeedPCM16LE(pcmPacket(160, 12)); !errors.Is(next, ErrQueueFull) {
		t.Fatalf("fatal queue error lost: %v", next)
	}
	d, err := p.Queue().Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.Frame.Sequence != 0 {
		t.Fatalf("want first accepted frame, got %d", d.Frame.Sequence)
	}
	if _, err := p.Queue().Pop(context.Background()); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want closed queue, got %v", err)
	}
}

func TestInputPipelineRejectMalformedAndAbort(t *testing.T) {
	p := testInputPipeline(t, 10, 200*time.Millisecond)
	if err := p.FeedPCM16LE([]byte{1}); err == nil {
		t.Fatal("expected invalid source payload")
	}
	if err := p.FeedPCM16LE(pcmPacket(160, 1)); err == nil {
		t.Fatal("invalid stream must not be revived")
	}
	if _, err := p.Queue().Pop(context.Background()); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want closed queue, got %v", err)
	}
	other := testInputPipeline(t, 10, 200*time.Millisecond)
	if err := other.FeedPCM16LE(pcmPacket(40, 2)); err != nil {
		t.Fatal(err)
	}
	other.Abort()
	other.Abort()
	if _, err := other.Queue().Pop(context.Background()); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("aborted stream leaked tail: %v", err)
	}
	if err := other.FeedPCM16LE(pcmPacket(40, 2)); !errors.Is(err, ErrPipelineFinished) {
		t.Fatalf("expected finished, got %v", err)
	}
}

func TestInputPipelineRejectWrongOutputFormat(t *testing.T) {
	_, err := NewInputPipeline(InputPipelineConfig{
		SourceRate: 8000, TargetRate: 16000, Channels: 1, ChunkSamples: 320,
		Queue: QueueConfig{SampleRate: 8000, Channels: 1, MaxFrames: 10, MaxBuffered: 200 * time.Millisecond},
	})
	if err == nil {
		t.Fatal("expected queue format mismatch")
	}
}

func TestInputPipelineConsumerCancellation(t *testing.T) {
	p := testInputPipeline(t, 10, 200*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Queue().Pop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	p.Abort()
}
