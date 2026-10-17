package stt

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/media/audio"
)

func testFrame(n uint64, offsetSamples uint64, rate int) audio.AudioFrame {
	return audio.AudioFrame{Sequence: n, Timestamp: fakeSampleTime(offsetSamples, rate), SampleRate: rate,
		Channels: 1, Samples: make([]int16, 320)}
}

func testConfig(id string) SpeechConfig {
	return SpeechConfig{CallID: id, Language: "en-IN", SampleRate: 16000, Channels: 1, Encoding: EncodingPCM16}
}

func startFake(t *testing.T, script []FakeEmission, callID string) *FakeSession {
	t.Helper()
	p, err := NewFakeTranscriber(script)
	if err != nil {
		t.Fatal(err)
	}
	iface, err := p.Start(context.Background(), testConfig(callID))
	if err != nil {
		t.Fatal(err)
	}
	s := iface.(*FakeSession)
	t.Cleanup(s.Abort)
	return s
}

func readAllEvents(t *testing.T, ch <-chan TranscriptEvent) []TranscriptEvent {
	t.Helper()
	result := []TranscriptEvent{}
	for e := range ch {
		if err := e.Validate(); err != nil {
			t.Fatalf("invalid event: %v", err)
		}
		result = append(result, e)
	}
	return result
}

func TestSpeechConfigAndTranscriptEventValidation(t *testing.T) {
	if err := testConfig("call-one").Validate(); err != nil {
		t.Fatal(err)
	}
	for label, mutate := range map[string]func(*SpeechConfig){
		"missing-call-id":    func(c *SpeechConfig) { c.CallID = " " },
		"missing-language":   func(c *SpeechConfig) { c.Language = "" },
		"unsupported-format": func(c *SpeechConfig) { c.Encoding = "opus" },
		"stereo":             func(c *SpeechConfig) { c.Channels = 2 },
		"rate":               func(c *SpeechConfig) { c.SampleRate = 44100 },
	} {
		t.Run(label, func(t *testing.T) {
			c := testConfig("c")
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatalf("accepted invalid config %+v", c)
			}
		})
	}
	if err := (TranscriptEvent{Kind: EventPartial, CallID: "c", SegmentID: "one", Text: "book", Start: 0, End: time.Second}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (TranscriptEvent{Kind: EventError, CallID: "c", Err: errors.New("outage")}).Validate(); err != nil {
		t.Fatal(err)
	}
	for i, e := range []TranscriptEvent{
		{Kind: EventPartial, CallID: "c", SegmentID: "", Text: "hey"},
		{Kind: EventFinal, CallID: "c", SegmentID: "s", Text: " "},
		{Kind: EventPartial, CallID: "c", SegmentID: "s", Text: "x", Start: 2 * time.Second, End: time.Second},
		{Kind: EventError, CallID: "c"},
		{Kind: EventKind(99), CallID: "c"},
	} {
		if err := e.Validate(); err == nil {
			t.Fatalf("invalid event %d accepted", i)
		}
	}
}

func TestFakeInterimRevisionFinalAndFinish(t *testing.T) {
	s := startFake(t, []FakeEmission{
		{AfterFrames: 1, Event: TranscriptEvent{Kind: EventPartial, SegmentID: "segment-1", Text: "Book", End: 20 * time.Millisecond}},
		{AfterFrames: 2, Event: TranscriptEvent{Kind: EventPartial, SegmentID: "segment-1", Text: "Book a", End: 40 * time.Millisecond}},
		{AfterFrames: 3, Event: TranscriptEvent{Kind: EventFinal, SegmentID: "segment-1", Text: "Book a table", End: 60 * time.Millisecond}},
		{OnFinish: true, Event: TranscriptEvent{Kind: EventFinal, SegmentID: "segment-2", Text: "Tomorrow please", Start: 60 * time.Millisecond, End: 60 * time.Millisecond}},
	}, "call-A")
	for i := 0; i < 3; i++ {
		if err := s.WriteAudio(context.Background(), testFrame(uint64(i), uint64(i*320), 16000)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := readAllEvents(t, s.Events())
	if len(got) != 4 {
		t.Fatalf("got %d events, want 4", len(got))
	}
	wantKinds := []EventKind{EventPartial, EventPartial, EventFinal, EventFinal}
	for i, e := range got {
		if e.Kind != wantKinds[i] || e.Sequence != uint64(i) || e.CallID != "call-A" {
			t.Fatalf("event %d unexpected: %+v", i, e)
		}
	}
	if got[0].Text != "Book" || got[1].Text != "Book a" || got[2].Text != "Book a table" {
		t.Fatal("partial revision/final text lost")
	}
	stats := s.Stats()
	if !stats.Finished || stats.AcceptedFrames != 3 || stats.AcceptedSamples != 960 {
		t.Fatalf("bad stats %+v", stats)
	}
	if err := s.WriteAudio(context.Background(), testFrame(3, 960, 16000)); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("expected closed, got %v", err)
	}
}

func TestFakeInvalidFrameDoesNotAdvanceTimeline(t *testing.T) {
	s := startFake(t, nil, "call-B")
	bad := testFrame(0, 0, 8000)
	if err := s.WriteAudio(context.Background(), bad); err == nil {
		t.Fatal("format mismatch was accepted")
	}
	bad = testFrame(9, 0, 16000)
	if err := s.WriteAudio(context.Background(), bad); !errors.Is(err, ErrDiscontinuity) {
		t.Fatalf("want sequence error, got %v", err)
	}
	bad = testFrame(0, 320, 16000)
	if err := s.WriteAudio(context.Background(), bad); !errors.Is(err, ErrDiscontinuity) {
		t.Fatalf("want timestamp error, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.WriteAudio(ctx, testFrame(0, 0, 16000)); !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	if st := s.Stats(); st.AcceptedFrames != 0 || st.AcceptedSamples != 0 {
		t.Fatalf("invalid input committed: %+v", st)
	}
	if err := s.WriteAudio(context.Background(), testFrame(0, 0, 16000)); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteAudio(context.Background(), testFrame(1, 320, 16000)); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.AcceptedFrames != 2 || st.AcceptedSamples != 640 {
		t.Fatalf("bad stats %+v", st)
	}
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if es := readAllEvents(t, s.Events()); len(es) != 0 {
		t.Fatalf("unexpected events %+v", es)
	}
}

func TestFakeProviderErrorClosesStream(t *testing.T) {
	cause := errors.New("simulated provider disconnected")
	s := startFake(t, []FakeEmission{
		{AfterFrames: 1, Event: TranscriptEvent{Kind: EventPartial, SegmentID: "s", Text: "Hello", End: 20 * time.Millisecond}},
		{AfterFrames: 2, Event: TranscriptEvent{Kind: EventError, Err: cause}},
	}, "call-error")
	for i := 0; i < 2; i++ {
		if err := s.WriteAudio(context.Background(), testFrame(uint64(i), uint64(i*320), 16000)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.WriteAudio(context.Background(), testFrame(2, 640, 16000)); !errors.Is(err, ErrSessionFailed) || !errors.Is(err, cause) {
		t.Fatalf("missing failure cause: %v", err)
	}
	if err := s.Finish(context.Background()); !errors.Is(err, ErrSessionFailed) {
		t.Fatalf("expected failed Finish, got %v", err)
	}
	es := readAllEvents(t, s.Events())
	if len(es) != 2 || es[1].Kind != EventError || !errors.Is(es[1].Err, cause) {
		t.Fatalf("events %+v", es)
	}
	if st := s.Stats(); !st.Failed || st.AcceptedFrames != 2 {
		t.Fatalf("wrong stats: %+v", st)
	}
}

func TestFakeEarlyFinishAndAbort(t *testing.T) {
	s := startFake(t, []FakeEmission{{AfterFrames: 2, Event: TranscriptEvent{Kind: EventFinal, SegmentID: "s", Text: "done", End: 40 * time.Millisecond}}}, "finish")
	if err := s.WriteAudio(context.Background(), testFrame(0, 0, 16000)); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background()); err == nil {
		t.Fatal("premature Finish silently skipped scripted transcript")
	}
	if err := s.WriteAudio(context.Background(), testFrame(1, 320, 16000)); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(readAllEvents(t, s.Events())) != 1 {
		t.Fatal("missing final transcript")
	}
	other := startFake(t, nil, "abort")
	other.Abort()
	other.Abort()
	if _, ok := <-other.Events(); ok {
		t.Fatal("aborted event channel not closed")
	}
	if err := other.Finish(context.Background()); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("want closed, got %v", err)
	}
	if err := other.WriteAudio(context.Background(), testFrame(0, 0, 16000)); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("want closed, got %v", err)
	}
}

func TestFakeParentCancellationTerminatesEvents(t *testing.T) {
	p, err := NewFakeTranscriber(nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	iface, err := p.Start(ctx, testConfig("call-cancel"))
	if err != nil {
		t.Fatal(err)
	}
	s := iface.(*FakeSession)
	cancel()
	select {
	case _, ok := <-s.Events():
		if ok {
			t.Fatal("unexpected event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parent cancellation failed to close Events")
	}
	if !s.Stats().Aborted {
		t.Fatal("cancel did not abort session")
	}
	if err := s.WriteAudio(context.Background(), testFrame(0, 0, 16000)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := p.Start(cancelled, testConfig("already-canceled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("start canceled: %v", err)
	}
}

func TestFakeIndependentConcurrentSessions(t *testing.T) {
	p, err := NewFakeTranscriber([]FakeEmission{{AfterFrames: 1, Event: TranscriptEvent{Kind: EventFinal, SegmentID: "s", Text: "hello", End: 20 * time.Millisecond}}})
	if err != nil {
		t.Fatal(err)
	}
	const count = 24
	var wg sync.WaitGroup
	problems := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("call-%d", i)
			ss, err := p.Start(context.Background(), testConfig(id))
			if err != nil {
				problems <- err
				return
			}
			defer ss.Abort()
			if err := ss.WriteAudio(context.Background(), testFrame(0, 0, 16000)); err != nil {
				problems <- err
				return
			}
			if err := ss.Finish(context.Background()); err != nil {
				problems <- err
				return
			}
			es := []TranscriptEvent{}
			for e := range ss.Events() {
				es = append(es, e)
			}
			if len(es) != 1 || es[0].CallID != id || es[0].Sequence != 0 {
				problems <- fmt.Errorf("call %s events: %v", id, es)
			}
		}(i)
	}
	wg.Wait()
	close(problems)
	for err := range problems {
		t.Error(err)
	}
}

func TestFakeScriptValidationAndBoundedEvents(t *testing.T) {
	good := TranscriptEvent{Kind: EventPartial, SegmentID: "x", Text: "hi"}
	badScripts := [][]FakeEmission{
		{{AfterFrames: 0, Event: good}},
		{{AfterFrames: 2, Event: good}, {AfterFrames: 1, Event: good}},
		{{OnFinish: true, Event: good}, {AfterFrames: 2, Event: good}},
		{{OnFinish: true, AfterFrames: 2, Event: good}},
		{{AfterFrames: 1, Event: TranscriptEvent{Kind: EventFinal, SegmentID: "", Text: "hi"}}},
	}
	for i, script := range badScripts {
		if _, err := NewFakeTranscriber(script); err == nil {
			t.Fatalf("accepted bad script %d", i)
		}
	}
	long := make([]FakeEmission, 257)
	for i := range long {
		long[i] = FakeEmission{AfterFrames: 1, Event: good}
	}
	if _, err := NewFakeTranscriber(long); err == nil {
		t.Fatal("unbounded fake script accepted")
	}
	// Many emitted events fit in the bounded, script-sized channel even if the
	// test does not read any events until after Finish. No production provider
	// gets this memory policy automatically: it must implement bounded egress.
	many := make([]FakeEmission, 128)
	for i := range many {
		many[i] = FakeEmission{AfterFrames: 1, Event: good}
	}
	s := startFake(t, many, "many")
	if err := s.WriteAudio(context.Background(), testFrame(0, 0, 16000)); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := readAllEvents(t, s.Events())
	if len(got) != len(many) {
		t.Fatalf("expected 128 events, got %d", len(got))
	}
	if got[0].Sequence != 0 || got[len(got)-1].Sequence != 127 {
		t.Fatal("event sequence incorrect")
	}
	// Script handed to the fake is copied, not retained as a mutable slice.
	input := []FakeEmission{{AfterFrames: 1, Event: good}}
	p, _ := NewFakeTranscriber(input)
	input[0].Event.Text = "changed externally"
	if p.script[0].Event.Text != "hi" {
		t.Fatal("script was aliased to external slice")
	}
}
