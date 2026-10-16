package audio

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func queueTestConfig() QueueConfig {
	return QueueConfig{
		SampleRate:  8000,
		Channels:    1,
		MaxFrames:   2,
		MaxBuffered: 50 * time.Millisecond,
		Overflow:    OverflowReject,
	}
}

// queueTestFrame builds distinct frames without assuming every packet is 20ms.
func queueTestFrame(seq uint64, start time.Duration, samples int) AudioFrame {
	pcm := make([]int16, samples)
	for i := range pcm {
		pcm[i] = int16(seq + 1)
	}
	return AudioFrame{
		Sequence:   seq,
		Timestamp:  start,
		SampleRate: 8000,
		Channels:   1,
		Samples:    pcm,
	}
}

func TestFrameQueueRejectsBadConfig(t *testing.T) {
	good := queueTestConfig()
	tests := []struct {
		name string
		edit func(*QueueConfig)
	}{
		{"zero sample rate", func(c *QueueConfig) { c.SampleRate = 0 }},
		{"excessive sample rate", func(c *QueueConfig) { c.SampleRate = 200000 }},
		{"zero channels", func(c *QueueConfig) { c.Channels = 0 }},
		{"zero frame limit", func(c *QueueConfig) { c.MaxFrames = 0 }},
		{"too many frames", func(c *QueueConfig) { c.MaxFrames = 4097 }},
		{"zero duration", func(c *QueueConfig) { c.MaxBuffered = 0 }},
		{"too long duration", func(c *QueueConfig) { c.MaxBuffered = time.Minute + time.Nanosecond }},
		{"subsample duration", func(c *QueueConfig) { c.MaxBuffered = time.Nanosecond }},
		{"unsupported overflow", func(c *QueueConfig) { c.Overflow = 99 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := good
			tc.edit(&cfg)
			if _, err := NewFrameQueue(cfg); err == nil {
				t.Fatalf("expected invalid configuration to be rejected: %+v", cfg)
			}
		})
	}
}

func TestFrameQueueFIFOAndOwnership(t *testing.T) {
	q, err := NewFrameQueue(queueTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	first := queueTestFrame(0, 0, 80)
	second := queueTestFrame(1, 10*time.Millisecond, 160)
	if err := q.Push(first); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(second); err != nil {
		t.Fatal(err)
	}
	first.Samples[0] = 12345 // producer reuses its memory after Push
	s := q.Stats()
	if s.Depth != 2 || s.BufferedSamples != 240 || s.BufferedDuration != 30*time.Millisecond ||
		s.Accepted != 2 || s.PeakDepth != 2 || s.PeakBufferedSamples != 240 {
		t.Fatalf("unexpected buffered stats: %+v", s)
	}
	ctx := context.Background()
	got1, err := q.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got1.Frame.Sequence != 0 || got1.Frame.Timestamp != 0 || got1.Frame.Samples[0] != 1 || got1.DroppedBefore != 0 {
		t.Fatalf("unexpected first frame: %+v", got1)
	}
	got1.Frame.Samples[0] = 999
	got2, err := q.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got2.Frame.Sequence != 1 || got2.Frame.Timestamp != 10*time.Millisecond || got2.Frame.Samples[0] != 2 {
		t.Fatalf("unexpected second frame: %+v", got2)
	}
	s = q.Stats()
	if s.Depth != 0 || s.BufferedSamples != 0 || s.Accepted != 2 || s.Delivered != 2 || s.Dropped != 0 || s.Rejected != 0 {
		t.Fatalf("unexpected final stats: %+v", s)
	}
}

func TestFrameQueueRejectPolicyPreservesQueuedFrames(t *testing.T) {
	t.Run("duration capacity", func(t *testing.T) {
		cfg := queueTestConfig()
		cfg.MaxBuffered = 30 * time.Millisecond
		q, err := NewFrameQueue(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := q.Push(queueTestFrame(0, 0, 160)); err != nil {
			t.Fatal(err)
		}
		if err := q.Push(queueTestFrame(1, 20*time.Millisecond, 80)); err != nil {
			t.Fatal(err)
		}
		if err := q.Push(queueTestFrame(2, 30*time.Millisecond, 80)); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("expected queue full, got %v", err)
		}
		stats := q.Stats()
		if stats.Depth != 2 || stats.Dropped != 0 || stats.Rejected != 1 {
			t.Fatalf("unexpected reject stats: %+v", stats)
		}
		first, err := q.Pop(context.Background())
		if err != nil || first.Frame.Sequence != 0 {
			t.Fatalf("first frame lost: %+v / %v", first, err)
		}
		second, err := q.Pop(context.Background())
		if err != nil || second.Frame.Sequence != 1 {
			t.Fatalf("second frame lost: %+v / %v", second, err)
		}
	})
	t.Run("frame count capacity", func(t *testing.T) {
		cfg := queueTestConfig()
		q, err := NewFrameQueue(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if err := q.Push(queueTestFrame(uint64(i), time.Duration(i)*time.Millisecond, 8)); err != nil {
				t.Fatal(err)
			}
		}
		if err := q.Push(queueTestFrame(2, 2*time.Millisecond, 8)); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("expected frame count limit, got %v", err)
		}
	})
}

func TestFrameQueueDropOldestCarriesDiscontinuity(t *testing.T) {
	for _, frameLimit := range []int{1, 2} {
		t.Run(map[int]string{1: "single frame", 2: "two frames"}[frameLimit], func(t *testing.T) {
			cfg := queueTestConfig()
			cfg.MaxFrames = frameLimit
			cfg.Overflow = OverflowDropOldest
			q, err := NewFrameQueue(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for seq := uint64(0); seq < 4; seq++ {
				if err := q.Push(queueTestFrame(seq, time.Duration(seq)*10*time.Millisecond, 80)); err != nil {
					t.Fatal(err)
				}
			}
			stats := q.Stats()
			if stats.Dropped != uint64(4-frameLimit) || stats.Depth != frameLimit {
				t.Fatalf("unexpected eviction stats: %+v", stats)
			}
			first, err := q.Pop(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if first.Frame.Sequence != uint64(4-frameLimit) || first.DroppedBefore != uint64(4-frameLimit) {
				t.Fatalf("first survivor must announce dropped frames: %+v", first)
			}
			if frameLimit == 2 {
				second, err := q.Pop(context.Background())
				if err != nil || second.Frame.Sequence != 3 || second.DroppedBefore != 0 {
					t.Fatalf("unexpected final survivor: %+v / %v", second, err)
				}
			}
		})
	}
	t.Run("duration eviction", func(t *testing.T) {
		cfg := queueTestConfig()
		cfg.MaxFrames = 5
		cfg.MaxBuffered = 30 * time.Millisecond
		cfg.Overflow = OverflowDropOldest
		q, err := NewFrameQueue(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(0); i < 3; i++ {
			if err := q.Push(queueTestFrame(i, time.Duration(i)*20*time.Millisecond, 160)); err != nil {
				t.Fatal(err)
			}
		}
		v, err := q.Pop(context.Background())
		if err != nil || v.Frame.Sequence != 2 || v.DroppedBefore != 2 {
			t.Fatalf("unexpected duration eviction: %+v / %v", v, err)
		}
	})
}

func TestFrameQueueOversizedOrInvalidDoesNotEvict(t *testing.T) {
	cfg := queueTestConfig()
	cfg.Overflow = OverflowDropOldest
	cfg.MaxBuffered = 20 * time.Millisecond
	q, err := NewFrameQueue(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Push(queueTestFrame(0, 0, 80)); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(queueTestFrame(1, 10*time.Millisecond, 161)); !errors.Is(err, ErrFrameTooLong) {
		t.Fatalf("expected oversized rejection, got %v", err)
	}
	bad := queueTestFrame(2, 20*time.Millisecond, 80)
	bad.Channels = 2
	if err := q.Push(bad); err == nil {
		t.Fatal("expected format rejection")
	}
	if err := q.Push(AudioFrame{}); err == nil {
		t.Fatal("expected validation failure")
	}
	stats := q.Stats()
	if stats.Depth != 1 || stats.Dropped != 0 || stats.Rejected != 3 {
		t.Fatalf("bad frame evicted good data: %+v", stats)
	}
	got, err := q.Pop(context.Background())
	if err != nil || got.Frame.Sequence != 0 || got.DroppedBefore != 0 {
		t.Fatalf("good frame missing: %+v / %v", got, err)
	}
}

func TestFrameQueuePopBlocksUntilPush(t *testing.T) {
	q, err := NewFrameQueue(queueTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan QueueDelivery, 1)
	failure := make(chan error, 1)
	go func() { v, e := q.Pop(ctx); result <- v; failure <- e }()
	select {
	case <-result:
		t.Fatal("empty Pop returned before Push")
	case <-time.After(20 * time.Millisecond):
	}
	if err := q.Push(queueTestFrame(5, 100*time.Millisecond, 80)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if err := <-failure; err != nil || got.Frame.Sequence != 5 {
			t.Fatalf("unexpected wakeup: %+v / %v", got, err)
		}
	case <-ctx.Done():
		t.Fatal("Pop failed to wake on Push")
	}
}

func TestFrameQueueCloseDrainsThenWakes(t *testing.T) {
	q, err := NewFrameQueue(queueTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Push(queueTestFrame(0, 0, 80)); err != nil {
		t.Fatal(err)
	}
	q.Close()
	q.Close() // idempotent
	if err := q.Push(queueTestFrame(1, 10*time.Millisecond, 80)); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected closed push: %v", err)
	}
	if first, err := q.Pop(context.Background()); err != nil || first.Frame.Sequence != 0 {
		t.Fatalf("close discarded buffered frame: %+v / %v", first, err)
	}
	if _, err := q.Pop(context.Background()); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected EOF-like closed, got %v", err)
	}
	if !q.Stats().Closed {
		t.Fatal("stats not closed")
	}

	q2, err := NewFrameQueue(queueTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { _, err := q2.Pop(context.Background()); wait <- err }()
	q2.Close()
	select {
	case err := <-wait:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("expected close wake, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked pop not woken on close")
	}
}

func TestFrameQueueCancelledPop(t *testing.T) {
	q, err := NewFrameQueue(queueTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := q.Pop(ctx); result <- err }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected canceled: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel failed to wake pop")
	}
	if q.Stats().Depth != 0 {
		t.Fatal("cancellation modified the queue")
	}
}

func TestFrameQueueConcurrentProducerConsumer(t *testing.T) {
	cfg := queueTestConfig()
	cfg.MaxFrames = 1024
	cfg.MaxBuffered = time.Second
	q, err := NewFrameQueue(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const total = 1000
	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			if err := q.Push(queueTestFrame(uint64(i), time.Duration(i)*125*time.Microsecond, 1)); err != nil {
				errCh <- err
				return
			}
		}
		q.Close()
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			v, err := q.Pop(ctx)
			if err != nil {
				errCh <- err
				return
			}
			if v.Frame.Sequence != uint64(i) || v.DroppedBefore != 0 {
				errCh <- errors.New("sequence or drop marker mismatch")
				return
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	s := q.Stats()
	if s.Depth != 0 || s.Accepted != total || s.Delivered != total || s.Dropped != 0 || s.Rejected != 0 {
		t.Fatalf("queue lost data under concurrent load: %+v", s)
	}
}
