package audio

import (
	"reflect"
	"testing"
	"time"
)

func newTestFrame(seq uint64, ts time.Duration, rate, channels int, samples ...int16) AudioFrame {
	return AudioFrame{
		Sequence: seq, Timestamp: ts, SampleRate: rate,
		Channels: channels, Samples: samples,
	}
}

func requireSamples(t *testing.T, actual, want []int16) {
	t.Helper()
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("samples = %v, want %v", actual, want)
	}
}

func TestRateConverter8kTo16kAcrossFrames(t *testing.T) {
	r, err := NewRateConverter(8000, 16000, 1)
	if err != nil {
		t.Fatal(err)
	}

	first, ok, err := r.Feed(newTestFrame(0, 0, 8000, 1, 0, 1000))
	if err != nil || !ok {
		t.Fatalf("first conversion: ready=%v err=%v", ok, err)
	}
	requireSamples(t, first.Samples, []int16{0, 500})
	if first.Sequence != 0 || first.Timestamp != 0 || first.Duration() != 125*time.Microsecond {
		t.Fatalf("unexpected first frame metadata: %+v", first)
	}

	second, ok, err := r.Feed(newTestFrame(1, 250*time.Microsecond, 8000, 1, 2000))
	if err != nil || !ok {
		t.Fatalf("second conversion: ready=%v err=%v", ok, err)
	}
	requireSamples(t, second.Samples, []int16{1000, 1500})
	if second.Sequence != 1 || second.Timestamp != 125*time.Microsecond {
		t.Fatalf("unexpected second frame: %+v", second)
	}

	last, ok, err := r.Finish()
	if err != nil || !ok {
		t.Fatalf("finish: ready=%v err=%v", ok, err)
	}
	requireSamples(t, last.Samples, []int16{2000, 2000})
	if last.Timestamp != 250*time.Microsecond || last.Sequence != 2 {
		t.Fatalf("unexpected finish frame: %+v", last)
	}
	if _, ready, err := r.Finish(); err != nil || ready {
		t.Fatalf("second Finish must be empty, got ready=%v err=%v", ready, err)
	}
	if _, _, err := r.Feed(newTestFrame(2, 375*time.Microsecond, 8000, 1, 1000)); err == nil {
		t.Fatal("Feed after Finish must fail")
	}
}

func TestRateConverter16kTo8kAcrossFramesAndOddTail(t *testing.T) {
	r, err := NewRateConverter(16000, 8000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ready, err := r.Feed(newTestFrame(0, 0, 16000, 1, 1000)); err != nil || ready {
		t.Fatalf("first single sample must buffer: ready=%v err=%v", ready, err)
	}
	out, ready, err := r.Feed(newTestFrame(1, 62500*time.Nanosecond, 16000, 1, 3000, 5000))
	if err != nil || !ready {
		t.Fatalf("pair output: ready=%v err=%v", ready, err)
	}
	requireSamples(t, out.Samples, []int16{2000})
	if out.Timestamp != 0 || out.Duration() != 125*time.Microsecond {
		t.Fatalf("bad output timing: %+v", out)
	}

	end, ready, err := r.Finish()
	if err != nil || !ready {
		t.Fatalf("odd-tail Finish: ready=%v err=%v", ready, err)
	}
	requireSamples(t, end.Samples, []int16{5000})
	if end.Timestamp != 125*time.Microsecond {
		t.Fatalf("unexpected end time: %v", end.Timestamp)
	}
}

func TestRateConverterStereoAndOverflowSafeAverage(t *testing.T) {
	r, err := NewRateConverter(8000, 16000, 2)
	if err != nil {
		t.Fatal(err)
	}
	f, ready, err := r.Feed(newTestFrame(0, 0, 8000, 2,
		-32768, 32767, -32768, 32767))
	if err != nil || !ready {
		t.Fatalf("stereo Feed: ready=%v err=%v", ready, err)
	}
	requireSamples(t, f.Samples, []int16{-32768, 32767, -32768, 32767})
	if f.SamplesPerChannel() != 2 || f.Duration() != 125*time.Microsecond {
		t.Fatalf("unexpected stereo timing: %+v", f)
	}

	down, err := NewRateConverter(16000, 8000, 1)
	if err != nil {
		t.Fatal(err)
	}
	out, ready, err := down.Feed(newTestFrame(0, 0, 16000, 1, -32768, 32767, -32768, -32768))
	if err != nil || !ready {
		t.Fatalf("downsample: ready=%v err=%v", ready, err)
	}
	requireSamples(t, out.Samples, []int16{0, -32768})
}

func TestRateConverterPacketizationDoesNotChangeSamples(t *testing.T) {
	for _, rates := range [][2]int{{8000, 16000}, {16000, 8000}} {
		input := make([]int16, 257)
		for i := range input {
			input[i] = int16(i*29 - 3000)
		}

		run := func(packetSizes []int) []int16 {
			t.Helper()
			r, err := NewRateConverter(rates[0], rates[1], 1)
			if err != nil {
				t.Fatal(err)
			}
			var got []int16
			offset := 0
			for seq, size := range packetSizes {
				if size > len(input)-offset {
					t.Fatal("test packet sizes overrun input")
				}
				frame, ok, err := r.Feed(newTestFrame(uint64(seq),
					streamTimestamp(uint64(offset), rates[0]), rates[0], 1,
					input[offset:offset+size]...))
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					got = append(got, frame.Samples...)
				}
				offset += size
			}
			if offset != len(input) {
				t.Fatalf("test did not provide all samples: %d", offset)
			}
			last, ok, err := r.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				got = append(got, last.Samples...)
			}
			return got
		}

		whole := run([]int{257})
		fragmented := run([]int{1, 2, 3, 20, 111, 120})
		requireSamples(t, fragmented, whole)
		if rates[0] == 8000 && len(whole) != 2*len(input) {
			t.Fatalf("upsampled count = %d, want %d", len(whole), 2*len(input))
		}
		if rates[0] == 16000 && len(whole) != (len(input)+1)/2 {
			t.Fatalf("downsampled count = %d, want %d", len(whole), (len(input)+1)/2)
		}
	}
}

func TestRateConverterRejectsDiscontinuityWithoutAdvancing(t *testing.T) {
	r, _ := NewRateConverter(8000, 16000, 1)
	invalid := []AudioFrame{
		newTestFrame(7, 0, 8000, 1, 1),
		newTestFrame(0, time.Millisecond, 8000, 1, 1),
		newTestFrame(0, 0, 16000, 1, 1),
		newTestFrame(0, 0, 8000, 2, 1, 2),
	}
	for _, f := range invalid {
		if _, _, err := r.Feed(f); err == nil {
			t.Fatalf("wanted validation error for %+v", f)
		}
	}
	f, ready, err := r.Feed(newTestFrame(0, 0, 8000, 1, 1, 3))
	if err != nil || !ready || f.Sequence != 0 || f.Timestamp != 0 {
		t.Fatalf("valid frame after rejects: %+v ready=%v err=%v", f, ready, err)
	}
	if _, err := NewRateConverter(11025, 16000, 1); err == nil {
		t.Fatal("unsupported resample ratio accepted")
	}
}

func TestRateConverterPassthroughCopiesInput(t *testing.T) {
	r, _ := NewRateConverter(8000, 8000, 1)
	input := []int16{100, -200}
	f, ready, err := r.Feed(newTestFrame(0, 0, 8000, 1, input...))
	if err != nil || !ready {
		t.Fatal(err)
	}
	input[0] = 1000
	requireSamples(t, f.Samples, []int16{100, -200})
	if _, ready, err := r.Finish(); err != nil || ready {
		t.Fatalf("passthrough Finish should be empty: ready=%v err=%v", ready, err)
	}
}

func TestPCMChunkerAggregateSplitAndFlush(t *testing.T) {
	c, err := NewPCMChunker(8000, 1, 160) // 20 ms target
	if err != nil {
		t.Fatal(err)
	}
	firstInput := make([]int16, 80)
	for i := range firstInput {
		firstInput[i] = int16(i)
	}
	got, err := c.Push(newTestFrame(0, 0, 8000, 1, firstInput...))
	if err != nil || len(got) != 0 {
		t.Fatalf("wanted buffered partial, got %d chunks, err=%v", len(got), err)
	}
	// Mutating source after Push cannot corrupt chunker's pending samples.
	firstInput[0] = 32767
	secondInput := make([]int16, 80)
	for i := range secondInput {
		secondInput[i] = int16(i + 80)
	}
	got, err = c.Push(newTestFrame(1, 10*time.Millisecond, 8000, 1, secondInput...))
	if err != nil || len(got) != 1 {
		t.Fatalf("aggregation produced %d chunks, err=%v", len(got), err)
	}
	if got[0].Samples[0] != 0 || got[0].Samples[159] != 159 || got[0].Duration() != 20*time.Millisecond {
		t.Fatalf("unexpected aggregate: %+v", got[0])
	}
	kept := got[0].Samples

	// 360 new samples -> 2 full frames + 40-sample remainder.
	input := make([]int16, 360)
	for i := range input {
		input[i] = int16(i + 160)
	}
	got, err = c.Push(newTestFrame(2, 20*time.Millisecond, 8000, 1, input...))
	if err != nil || len(got) != 2 {
		t.Fatalf("split produced %d chunks, err=%v", len(got), err)
	}
	for i, f := range got {
		if f.Sequence != uint64(i+1) || f.Timestamp != time.Duration(i+1)*20*time.Millisecond || f.Duration() != 20*time.Millisecond {
			t.Fatalf("bad chunk metadata: %+v", f)
		}
	}
	input[0] = 9000
	if kept[0] != 0 || kept[159] != 159 || got[0].Samples[0] != 160 || got[1].Samples[0] != 320 {
		t.Fatalf("output buffers reused or wrong samples: %v, %v", got[0].Samples[0], got[1].Samples[0])
	}

	tail, ok, err := c.Finish()
	if err != nil || !ok {
		t.Fatalf("finish partial: ready=%v err=%v", ok, err)
	}
	if tail.Sequence != 3 || tail.Timestamp != 60*time.Millisecond || tail.Duration() != 5*time.Millisecond || len(tail.Samples) != 40 {
		t.Fatalf("unexpected remainder: %+v", tail)
	}
	if _, err := c.Push(newTestFrame(3, 65*time.Millisecond, 8000, 1, 1)); err == nil {
		t.Fatal("Push after Finish should fail")
	}
}

func TestPCMChunkerRejectsBadInputWithoutAdvancing(t *testing.T) {
	c, _ := NewPCMChunker(8000, 2, 2)
	for _, f := range []AudioFrame{
		newTestFrame(1, 0, 8000, 2, 1, 2),
		newTestFrame(0, 2*time.Millisecond, 8000, 2, 1, 2),
		newTestFrame(0, 0, 8000, 2, 1),
		newTestFrame(0, 0, 16000, 2, 1, 2),
	} {
		if _, err := c.Push(f); err == nil {
			t.Fatalf("expected invalid chunk for %+v", f)
		}
	}
	out, err := c.Push(newTestFrame(0, 0, 8000, 2, 1, 2, 3, 4))
	if err != nil || len(out) != 1 || out[0].SamplesPerChannel() != 2 {
		t.Fatalf("valid chunk after errors: %+v err=%v", out, err)
	}
	if _, ready, err := c.Finish(); err != nil || ready {
		t.Fatalf("exactly-aligned stream must not have remainder: ready=%v err=%v", ready, err)
	}
	for _, args := range [][3]int{{8000, 1, 0}, {8000, 1, 8001}, {0, 1, 1}, {8000, 0, 1}} {
		if _, err := NewPCMChunker(args[0], args[1], args[2]); err == nil {
			t.Fatalf("invalid chunker config accepted: %v", args)
		}
	}
}

func TestResampleThenChunkToSTT20ms(t *testing.T) {
	r, _ := NewRateConverter(8000, 16000, 1)
	c, _ := NewPCMChunker(16000, 1, 320) // 20ms at 16k
	var chunks []AudioFrame
	for seq := 0; seq < 2; seq++ {
		input := make([]int16, 160) // 20 ms at 8 kHz
		for i := range input {
			input[i] = int16(seq*160 + i)
		}
		resampled, ready, err := r.Feed(newTestFrame(uint64(seq), time.Duration(seq)*20*time.Millisecond, 8000, 1, input...))
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			got, err := c.Push(resampled)
			if err != nil {
				t.Fatal(err)
			}
			chunks = append(chunks, got...)
		}
	}
	last, ready, err := r.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		got, err := c.Push(last)
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, got...)
	}
	if tail, ready, err := c.Finish(); err != nil {
		t.Fatal(err)
	} else if ready {
		chunks = append(chunks, tail)
	}
	if len(chunks) != 2 {
		t.Fatalf("wanted two 20ms chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if chunk.SampleRate != 16000 || chunk.Sequence != uint64(i) || chunk.Timestamp != time.Duration(i)*20*time.Millisecond || len(chunk.Samples) != 320 {
			t.Fatalf("unexpected STT-ready chunk: %+v", chunk)
		}
	}
}
