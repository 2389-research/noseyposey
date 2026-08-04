// ABOUTME: Tests for the router package using in-package fakes.
// ABOUTME: Covers new-thread, thread-reuse, and dedup scenarios.
package router

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/2389-ai/noseyposey/internal/transcript"
)

// fakeStore is an in-memory ThreadStore.
type fakeStore struct {
	threads map[string]string // device|date -> ts
	posted  map[string]bool   // device|tsKey -> true
}

func newFakeStore() *fakeStore {
	return &fakeStore{threads: map[string]string{}, posted: map[string]bool{}}
}
func (f *fakeStore) ThreadTS(device, date string) (string, bool, error) {
	ts, ok := f.threads[device+"|"+date]
	return ts, ok, nil
}
func (f *fakeStore) SaveThread(device, date, channel, ts string) error {
	f.threads[device+"|"+date] = ts
	return nil
}
func (f *fakeStore) AlreadyPosted(device, tsKey string) (bool, error) {
	return f.posted[device+"|"+tsKey], nil
}
func (f *fakeStore) MarkPosted(device, tsKey string) error {
	f.posted[device+"|"+tsKey] = true
	return nil
}

// fakePoster records calls and returns incrementing timestamps.
type fakePoster struct {
	calls []call
	n     int
}
type call struct{ channel, threadTS, text string }

func (p *fakePoster) Post(_ context.Context, channel, threadTS, text string) (string, error) {
	p.calls = append(p.calls, call{channel, threadTS, text})
	p.n++
	return "ts-" + itoa(p.n), nil
}
func itoa(n int) string { return strconv.Itoa(n) }

func chicago(t *testing.T) *time.Location {
	loc, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func utter(device, text string) transcript.Utterance {
	return transcript.Utterance{
		Device:    device,
		Text:      text,
		Timestamp: time.Date(2026, 8, 3, 20, 28, 54, 331766000, time.UTC),
	}
}

func TestHandleCreatesParentThenReply(t *testing.T) {
	store := newFakeStore()
	poster := &fakePoster{}
	r := New(store, poster, "C123", chicago(t))

	if err := r.Handle(context.Background(), utter("ivan-desk", "Whoa.")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(poster.calls) != 2 {
		t.Fatalf("expected 2 posts (parent+reply), got %d", len(poster.calls))
	}
	// first call = parent (no threadTS), correct label
	if poster.calls[0].threadTS != "" || poster.calls[0].text != "🎙️ ivan-desk — Mon Aug 3" {
		t.Errorf("parent call = %+v", poster.calls[0])
	}
	// second call = reply under parent's ts, correct line
	if poster.calls[1].threadTS != "ts-1" || poster.calls[1].text != "15:28  Whoa." {
		t.Errorf("reply call = %+v", poster.calls[1])
	}
}

func TestHandleReusesThreadForSameDay(t *testing.T) {
	store := newFakeStore()
	poster := &fakePoster{}
	r := New(store, poster, "C123", chicago(t))

	_ = r.Handle(context.Background(), utter("ivan-desk", "one"))
	_ = r.Handle(context.Background(), utter("ivan-desk", "two"))

	// parent posted once, two replies
	parents := 0
	for _, c := range poster.calls {
		if c.threadTS == "" {
			parents++
		}
	}
	if parents != 1 {
		t.Errorf("expected 1 parent, got %d", parents)
	}
	if len(poster.calls) != 3 {
		t.Errorf("expected 3 posts total, got %d", len(poster.calls))
	}
}

func TestHandleDedupsRepeat(t *testing.T) {
	store := newFakeStore()
	poster := &fakePoster{}
	r := New(store, poster, "C123", chicago(t))

	u := utter("ivan-desk", "Whoa.")
	_ = r.Handle(context.Background(), u)
	before := len(poster.calls)
	_ = r.Handle(context.Background(), u) // identical → deduped
	if len(poster.calls) != before {
		t.Errorf("expected no new posts on duplicate, got %d extra", len(poster.calls)-before)
	}
}
