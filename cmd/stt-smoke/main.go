// stt-smoke is a CP4.2 developer-only harness. It streams a 16kHz mono
// HEADERLESS signed PCM16LE recording to Deepgram at real-time speed.
// It does NOT modify Anzu's AudioSocket service or handle real phone calls.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audio"
	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt"
	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt/deepgram"
	dgws "github.com/Tarun516/anzu-agent-runtime/internal/speech/stt/deepgram/gorilla"
)

func main() {
	pcm := flag.String("pcm", "", "path to HEADERLESS 16kHz mono PCM16LE audio")
	lang := flag.String("language", "en-IN", "BCP-47 language recognized by the selected Deepgram model")
	model := flag.String("model", "nova-3", "Deepgram model")
	flag.Parse()
	if *pcm == "" {
		log.Fatal("required: -pcm /path/to/16k_mono.pcm; convert WAV with ffmpeg first")
	}
	if err := run(*pcm, *lang, *model); err != nil {
		log.Fatal(err)
	}
}

func run(path, lang, model string) error {
	apiKey := os.Getenv("DEEPGRAM_API_KEY")
	if apiKey == "" {
		return errors.New("DEEPGRAM_API_KEY not set (keep credentials outside the code)")
	}
	provider, err := deepgram.New(deepgram.Config{APIKey: apiKey, Model: model, Dialer: dgws.Dialer{}})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	s, err := provider.Start(ctx, stt.SpeechConfig{CallID: "stt-smoke", Language: lang, SampleRate: 16000, Channels: 1, Encoding: stt.EncodingPCM16})
	if err != nil {
		return err
	}
	defer s.Abort()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for evt := range s.Events() {
			switch evt.Kind {
			case stt.EventPartial:
				fmt.Printf("PARTIAL %s: %s\n", evt.SegmentID, evt.Text)
			case stt.EventFinal:
				fmt.Printf("FINAL   %s: %s\n", evt.SegmentID, evt.Text)
			case stt.EventError:
				fmt.Fprintf(os.Stderr, "STT error: %v\n", evt.Err)
			}
		}
	}()
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 640) // 320 samples = 20ms @ 16k mono
	var sequence, samples uint64
	for {
		n, readErr := io.ReadFull(f, buf)
		if readErr == io.EOF && n == 0 {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return readErr
		}
		if n%2 != 0 {
			return fmt.Errorf("odd PCM16LE byte count at end: %d", n)
		}
		raw := make([]int16, n/2)
		for i := range raw {
			raw[i] = int16(binary.LittleEndian.Uint16(buf[2*i:]))
		}
		frame := audio.AudioFrame{Sequence: sequence, Timestamp: time.Duration(samples/16000)*time.Second + time.Duration(samples%16000)*time.Second/16000, SampleRate: 16000, Channels: 1, Samples: raw}
		if err := s.WriteAudio(ctx, frame); err != nil {
			return err
		}
		sequence++
		samples += uint64(len(raw))
		// Simulate a realtime producer. Avoid sending entire file as a burst.
		delay := time.Duration(len(raw)) * time.Second / 16000
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	if samples == 0 {
		return errors.New("input PCM file empty")
	}
	finishCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	err = s.Finish(finishCtx)
	<-done // reader closes the events channel when it exits
	return err
}
