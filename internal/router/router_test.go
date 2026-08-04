// ABOUTME: Tests for the router package using in-package fakes.
// ABOUTME: Covers new-thread, thread-reuse, dedup, and all error-path invariants.
package router

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/2389-ai/noseyposey/internal/transcript"
)

// fakeStore is an in-memory ThreadStore.
type fakeStore struct {
	threads map[string]string // device|date -> ts
	posted  map[string]bool   // device|tsKey -> true

	// error injection
	alreadyPostedErr error // if non-nil, AlreadyPosted returns this error

	// call counters for assertions
	saveThreadCalls int
	markPostedCalls int
}

func newFakeStore() *fakeStore {
	return &fakeStore{threads: map[string]string{}, posted: map[string]bool{}}
}
func (f *fakeStore) ThreadTS(device, date string) (string, bool, error) {
	ts, ok := f.threads[device+"|"+date]
	return ts, ok, nil
}
func (f *fakeStore) SaveThread(device, date, channel, ts string) error {
	f.saveThreadCalls++
	f.threads[device+"|"+date] = ts
	return nil
}
func (f *fakeStore) AlreadyPosted(device, tsKey string) (bool, error) {
	if f.alreadyPostedErr != nil {
		return false, f.alreadyPostedErr
	}
	return f.posted[device+"|"+tsKey], nil
}
func (f *fakeStore) MarkPosted(device, tsKey string) error {
	f.markPostedCalls++
	f.posted[device+"|"+tsKey] = true
	return nil
}

// fakePoster records calls and returns incrementing timestamps.
// If failCall > 0, the call at that 1-based index returns an error.
type fakePoster struct {
	calls    []call
	n        int
	failCall int // 1-based index of the Post call that should fail; 0 = never
}
type call struct{ channel, threadTS, text string }

var errPost = errors.New("post failed")

func (p *fakePoster) Post(_ context.Context, channel, threadTS, text string) (string, error) {
	p.n++
	p.calls = append(p.calls, call{channel, threadTS, text})
	if p.failCall > 0 && p.n == p.failCall {
		return "", errPost
	}
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

	if err := r.Handle(context.Background(), utter("ivan-desk", "one")); err != nil {
		t.Fatal(err)
	}
	if err := r.Handle(context.Background(), utter("ivan-desk", "two")); err != nil {
		t.Fatal(err)
	}

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
	if err := r.Handle(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	before := len(poster.calls)
	_ = r.Handle(context.Background(), u) // identical → deduped
	if len(poster.calls) != before {
		t.Errorf("expected no new posts on duplicate, got %d extra", len(poster.calls)-before)
	}
}

// TestHandleParentPostFailsNoSaveThread: when the parent Post fails, SaveThread must not be
// called, no reply is posted, and Handle returns an error.
func TestHandleParentPostFailsNoSaveThread(t *testing.T) {
	store := newFakeStore()
	poster := &fakePoster{failCall: 1} // first Post (parent) fails
	r := New(store, poster, "C123", chicago(t))

	err := r.Handle(context.Background(), utter("ivan-desk", "Whoa."))
	if err == nil {
		t.Fatal("expected Handle to return an error, got nil")
	}
	if store.saveThreadCalls != 0 {
		t.Errorf("SaveThread should not be called after parent post failure, called %d time(s)", store.saveThreadCalls)
	}
	if len(poster.calls) != 1 {
		t.Errorf("expected exactly 1 post attempt (parent only), got %d", len(poster.calls))
	}
}

// TestHandleReplyPostFailsNoMarkPosted: when the reply Post fails, MarkPosted must not be
// called so the utterance can be retried.
func TestHandleReplyPostFailsNoMarkPosted(t *testing.T) {
	store := newFakeStore()
	poster := &fakePoster{failCall: 2} // second Post (reply) fails
	r := New(store, poster, "C123", chicago(t))

	err := r.Handle(context.Background(), utter("ivan-desk", "Whoa."))
	if err == nil {
		t.Fatal("expected Handle to return an error, got nil")
	}
	if store.markPostedCalls != 0 {
		t.Errorf("MarkPosted should not be called after reply post failure, called %d time(s)", store.markPostedCalls)
	}
}

// TestHandleAlreadyPostedErrorPropagates: when AlreadyPosted returns an error, Handle must
// propagate it and must not post anything.
func TestHandleAlreadyPostedErrorPropagates(t *testing.T) {
	sentinel := errors.New("dedup store down")
	store := newFakeStore()
	store.alreadyPostedErr = sentinel
	poster := &fakePoster{}
	r := New(store, poster, "C123", chicago(t))

	err := r.Handle(context.Background(), utter("ivan-desk", "Whoa."))
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got: %v", err)
	}
	if len(poster.calls) != 0 {
		t.Errorf("expected no posts when AlreadyPosted errors, got %d", len(poster.calls))
	}
}
