package audio

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// OverflowPolicy controls what happens when a producer fills the queue.
// Choosing a policy is a product decision: dropping speech can lose words.
type OverflowPolicy uint8

const (
	// OverflowReject leaves queued audio untouched and rejects new input.
	OverflowReject OverflowPolicy = iota
	// OverflowDropOldest evicts the oldest buffered frames until new input fits.
	// Consumers MUST inspect DroppedBefore because output is discontinuous.
	OverflowDropOldest
)

var (
	ErrQueueFull    = errors.New("audio queue full")
	ErrQueueClosed  = errors.New("audio queue closed")
	ErrFrameTooLong = errors.New("audio frame exceeds queue duration budget")
)

// QueueConfig bounds a SINGLE stream by both its frame count and its audio
// duration. SampleRate and Channels must match each admitted AudioFrame.
// The queue uses sample counts internally to avoid fractional-time drift.
type QueueConfig struct {
	SampleRate  int
	Channels    int
	MaxFrames   int
	MaxBuffered time.Duration
	Overflow    OverflowPolicy
}

// QueueStats is a concurrency-safe point-in-time snapshot of queue behavior.
// Buffered counts samples per channel, not individual interleaved values.
type QueueStats struct {
	Depth               int
	BufferedSamples     int
	BufferedDuration    time.Duration
	PeakDepth           int
	PeakBufferedSamples int
	Accepted            uint64
	Delivered           uint64
	Dropped             uint64
	Rejected            uint64
	Closed              bool
}

// QueueDelivery preserves any dropped-frame notification with the first
// surviving frame after a gap. A consumer must explicitly handle such a gap;
// silently concatenating frames would corrupt the perceived conversation.
type QueueDelivery struct {
	Frame         AudioFrame
	DroppedBefore uint64
}

type queueEntry struct {
	frame         AudioFrame
	droppedBefore uint64
}

// FrameQueue is a bounded, multi-producer, multi-consumer FIFO for one audio
// stream. It is safe for concurrent Push/Pop/Close/Stats, but producer ordering
// across goroutines is not guaranteed. It makes no I/O or goroutine by itself.
//
// A ring makes enqueue/dequeue/eviction O(1). Frames are copied on Push so a
// producer can safely reuse its own buffer after Push returns.
type FrameQueue struct {
	mu           sync.Mutex
	changed      chan struct{} // closed/replaced under mu whenever data arrives
	config       QueueConfig
	maxSamples   int
	entries      []queueEntry
	head         int
	size         int
	buffered     int
	pendingDrops uint64
	stats        QueueStats
	closed       bool
}

// NewFrameQueue validates and creates an empty queue. Buffered duration is
// floored to a whole sample; at least one sample must fit. The one-minute
// upper bound prevents accidentally configuring a many-minute realtime lag.
func NewFrameQueue(cfg QueueConfig) (*FrameQueue, error) {
	if cfg.SampleRate <= 0 || cfg.SampleRate > 192000 {
		return nil, fmt.Errorf("invalid sample rate: %d", cfg.SampleRate)
	}
	if cfg.Channels <= 0 || cfg.Channels > 8 {
		return nil, fmt.Errorf("invalid channel count: %d", cfg.Channels)
	}
	if cfg.MaxFrames <= 0 || cfg.MaxFrames > 4096 {
		return nil, fmt.Errorf("invalid queue frame capacity: %d", cfg.MaxFrames)
	}
	if cfg.MaxBuffered <= 0 || cfg.MaxBuffered > time.Minute {
		return nil, fmt.Errorf("invalid buffered duration: %s", cfg.MaxBuffered)
	}
	if cfg.Overflow != OverflowReject && cfg.Overflow != OverflowDropOldest {
		return nil, fmt.Errorf("invalid overflow policy: %d", cfg.Overflow)
	}

	// Use whole seconds plus the remainder to avoid duration*rate overflow.
	seconds := int(cfg.MaxBuffered / time.Second)
	remainder := int64(cfg.MaxBuffered % time.Second)
	maxSamples := seconds*cfg.SampleRate + int(remainder*int64(cfg.SampleRate)/int64(time.Second))
	if maxSamples < 1 {
		return nil, errors.New("buffered duration is shorter than one sample")
	}
	return &FrameQueue{
		config:     cfg,
		maxSamples: maxSamples,
		entries:    make([]queueEntry, cfg.MaxFrames),
		changed:    make(chan struct{}),
	}, nil
}

// Push immediately accepts or rejects one frame; it never blocks the TCP
// producer behind a slow model. A valid accepted frame's samples are copied.
// The caller must not mutate frame.Samples concurrently WITH the Push call.
//
// When OverflowDropOldest is selected, any evicted frames are reported by
// Stats.Dropped and the first surviving delivery's DroppedBefore counter.
func (q *FrameQueue) Push(frame AudioFrame) error {
	if q == nil {
		return errors.New("audio queue is nil")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	if err := frame.Validate(); err != nil {
		q.stats.Rejected++
		return fmt.Errorf("invalid audio frame: %w", err)
	}
	if frame.SampleRate != q.config.SampleRate || frame.Channels != q.config.Channels {
		q.stats.Rejected++
		return fmt.Errorf("audio format mismatch: got %d Hz/%d channels, want %d Hz/%d channels",
			frame.SampleRate, frame.Channels, q.config.SampleRate, q.config.Channels)
	}
	samples := frame.SamplesPerChannel()
	if samples > q.maxSamples {
		q.stats.Rejected++
		return ErrFrameTooLong
	}
	if q.config.Overflow == OverflowReject &&
		(q.size == len(q.entries) || q.buffered+samples > q.maxSamples) {
		q.stats.Rejected++
		return ErrQueueFull
	}

	// The incoming frame fits on its own. Under DropOldest, evict exactly
	// what is needed to satisfy BOTH independent capacity constraints.
	for q.size > 0 && (q.size == len(q.entries) || q.buffered+samples > q.maxSamples) {
		q.evictOldestLocked()
	}

	owned := frame
	owned.Samples = append([]int16(nil), frame.Samples...)
	index := (q.head + q.size) % len(q.entries)
	q.entries[index] = queueEntry{frame: owned, droppedBefore: q.pendingDrops}
	q.pendingDrops = 0
	q.size++
	q.buffered += samples
	q.stats.Accepted++
	if q.size > q.stats.PeakDepth {
		q.stats.PeakDepth = q.size
	}
	if q.buffered > q.stats.PeakBufferedSamples {
		q.stats.PeakBufferedSamples = q.buffered
	}
	q.signalLocked()
	return nil
}

// evictOldestLocked removes a frame and carries its drop marker forward.
// That way the next delivered frame reports a discontinuity even if several
// frames were evicted before the consumer observed any of them.
func (q *FrameQueue) evictOldestLocked() {
	old := q.entries[q.head]
	q.entries[q.head] = queueEntry{} // release the backing slice for GC
	q.head = (q.head + 1) % len(q.entries)
	q.size--
	q.buffered -= old.frame.SamplesPerChannel()
	q.stats.Dropped++
	gap := old.droppedBefore + 1
	if q.size > 0 {
		q.entries[q.head].droppedBefore += gap
	} else {
		q.pendingDrops += gap
	}
}

// Pop waits without busy polling. A closed queue drains already accepted
// frames, then returns ErrQueueClosed. Cancellation wakes the caller without
// creating a per-wait goroutine or consuming a frame.
func (q *FrameQueue) Pop(ctx context.Context) (QueueDelivery, error) {
	if q == nil {
		return QueueDelivery{}, errors.New("audio queue is nil")
	}
	if ctx == nil {
		return QueueDelivery{}, errors.New("context is nil")
	}
	for {
		if err := ctx.Err(); err != nil {
			return QueueDelivery{}, err
		}
		q.mu.Lock()
		if q.size > 0 {
			old := q.entries[q.head]
			q.entries[q.head] = queueEntry{}
			q.head = (q.head + 1) % len(q.entries)
			q.size--
			q.buffered -= old.frame.SamplesPerChannel()
			q.stats.Delivered++
			q.mu.Unlock()
			return QueueDelivery{Frame: old.frame, DroppedBefore: old.droppedBefore}, nil
		}
		if q.closed {
			q.mu.Unlock()
			return QueueDelivery{}, ErrQueueClosed
		}
		wake := q.changed
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return QueueDelivery{}, ctx.Err()
		case <-wake:
		}
	}
}

// Close prevents future Push calls and wakes idle Pop callers. It does not
// discard buffered data; Pop drains the remaining frames before ErrQueueClosed.
func (q *FrameQueue) Close() {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.signalLocked()
	}
}

// Stats reads all fields under the same lock to give a consistent snapshot.
func (q *FrameQueue) Stats() QueueStats {
	if q == nil {
		return QueueStats{}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	s := q.stats
	s.Depth = q.size
	s.BufferedSamples = q.buffered
	s.BufferedDuration = time.Duration(q.buffered) * time.Second / time.Duration(q.config.SampleRate)
	s.Closed = q.closed
	return s
}

// signalLocked notifies any blocked consumer of a state change.
// Caller must hold mu; Pop captures the channel while holding mu too,
// so no wakeup is lost between observing empty and going to sleep.
func (q *FrameQueue) signalLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}
