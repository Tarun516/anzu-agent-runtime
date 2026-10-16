package audio

import (
	"errors"
	"fmt"
)

var ErrPipelineFinished = errors.New("audio input pipeline is finished")

// InputPipelineConfig describes the source audio stream, target stream and
// output queue. One InputPipeline belongs to exactly one call.
type InputPipelineConfig struct {
	SourceRate   int
	TargetRate   int
	Channels     int
	ChunkSamples int // target samples per channel per output frame
	Queue        QueueConfig
}

// InputPipeline composes decoding, streaming rate conversion, rechunking and
// bounded buffering. It never reads from or writes to an AudioSocket connection.
//
// One producer goroutine owns FeedPCM16LE and Finish; do not call them
// concurrently. The Queue is safe for a separate consumer goroutine.
// The caller MUST handle a returned error; rejected speech is not recoverable
// from the queue and must never be presented as a complete transcript.
type InputPipeline struct {
	decoder   *PCM16Decoder
	converter *RateConverter
	chunker   *PCMChunker
	queue     *FrameQueue
	finished  bool
	failed    error
}

// NewInputPipeline validates the stages before accepting any audio.
// For the current Anzu lab use SourceRate=8000, TargetRate=16000,
// Channels=1, ChunkSamples=320, with a matching 16kHz mono queue.
func NewInputPipeline(cfg InputPipelineConfig) (*InputPipeline, error) {
	if cfg.Queue.SampleRate != cfg.TargetRate || cfg.Queue.Channels != cfg.Channels {
		return nil, errors.New("output queue format must match target audio format")
	}
	decoder, err := NewPCM16Decoder(cfg.SourceRate, cfg.Channels)
	if err != nil {
		return nil, fmt.Errorf("input decoder: %w", err)
	}
	converter, err := NewRateConverter(cfg.SourceRate, cfg.TargetRate, cfg.Channels)
	if err != nil {
		return nil, fmt.Errorf("rate converter: %w", err)
	}
	chunker, err := NewPCMChunker(cfg.TargetRate, cfg.Channels, cfg.ChunkSamples)
	if err != nil {
		return nil, fmt.Errorf("chunker: %w", err)
	}
	queue, err := NewFrameQueue(cfg.Queue)
	if err != nil {
		return nil, fmt.Errorf("output queue: %w", err)
	}
	return &InputPipeline{decoder: decoder, converter: converter, chunker: chunker, queue: queue}, nil
}

// Queue returns the bounded output queue. A separate consumer may Pop(ctx).
// The returned queue should not be manually closed during normal feeding;
// Finish closes it after flushing the converter and chunker.
func (p *InputPipeline) Queue() *FrameQueue {
	if p == nil {
		return nil
	}
	return p.queue
}

// FeedPCM16LE processes one raw source PCM payload. It must be called once for
// each source TypeAudio payload IN ORDER; the AudioSocket header/UUID/DTMF are
// deliberately handled by the transport adapter instead.
//
// A pipeline stage error or overload is terminal. Accepted output may already
// exist in the queue; a failed pipeline MUST NOT count as a complete stream.
func (p *InputPipeline) FeedPCM16LE(payload []byte) error {
	if p == nil {
		return errors.New("input pipeline is nil")
	}
	if p.failed != nil {
		return p.failed
	}
	if p.finished {
		return ErrPipelineFinished
	}

	frame, err := p.decoder.Decode(payload)
	if err != nil {
		return p.fail(fmt.Errorf("decode PCM16: %w", err))
	}
	converted, ready, err := p.converter.Feed(frame)
	if err != nil {
		return p.fail(fmt.Errorf("convert audio: %w", err))
	}
	if !ready {
		return nil
	}
	return p.pushConverted(converted)
}

// Finish flushes delayed interpolation and any incomplete final audio chunk.
// It closes the queue, allowing an existing consumer to drain accepted frames.
// Calling Finish again after a successful finish has no effect.
func (p *InputPipeline) Finish() error {
	if p == nil {
		return errors.New("input pipeline is nil")
	}
	if p.failed != nil {
		return p.failed
	}
	if p.finished {
		return nil
	}
	p.finished = true
	defer p.queue.Close()

	last, ready, err := p.converter.Finish()
	if err != nil {
		return p.fail(fmt.Errorf("finish converter: %w", err))
	}
	if ready {
		if err := p.pushConverted(last); err != nil {
			return err
		}
	}
	tail, ready, err := p.chunker.Finish()
	if err != nil {
		return p.fail(fmt.Errorf("finish chunker: %w", err))
	}
	if ready {
		if err := p.queue.Push(tail); err != nil {
			return p.fail(fmt.Errorf("queue trailing chunk: %w", err))
		}
	}
	return nil
}

// Abort stops the pipeline without flushing pending audio, useful for forced
// disconnects or canceled calls. Idempotent and producer-goroutine-owned.
func (p *InputPipeline) Abort() {
	if p == nil {
		return
	}
	p.finished = true
	p.queue.Close()
}

// pushConverted rechunks any newly resampled audio and offers chunks to the
// queue without ever blocking the media producer behind a slow consumer.
func (p *InputPipeline) pushConverted(f AudioFrame) error {
	chunks, err := p.chunker.Push(f)
	if err != nil {
		return p.fail(fmt.Errorf("chunk audio: %w", err))
	}
	for _, chunk := range chunks {
		if err := p.queue.Push(chunk); err != nil {
			return p.fail(fmt.Errorf("queue audio (incomplete stream): %w", err))
		}
	}
	return nil
}

// fail records a terminal error, prevents further input, and wakes the queue
// consumer. Buffered frames may still drain; the caller must propagate the
// error to call-level integrity policy instead of treating it as completion.
func (p *InputPipeline) fail(err error) error {
	p.failed = err
	p.finished = true
	p.queue.Close()
	return err
}
