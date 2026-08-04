// ABOUTME: Tests for the Slack client using a real httptest fake server.
// ABOUTME: Exercises success, 429 retry (Retry-After honored), 5xx backoff, and permanent-error paths.
package slackclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSlack serves chat.postMessage, optionally 429-ing the first N calls.
func fakeSlack(t *testing.T, fail429 int32) (*httptest.Server, *int32) {
	t.Helper()
	var calls int32
	failsLeft := fail429
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		if atomic.AddInt32(&failsLeft, -1) >= 0 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"ok":false,"error":"ratelimited"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C123","ts":"1785854349.600129"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestPostSuccess(t *testing.T) {
	srv, calls := fakeSlack(t, 0)
	c := New("xoxb-test", WithAPIURL(srv.URL+"/"), WithMinInterval(0))
	ts, err := c.Post(context.Background(), "C123", "", "hello")
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if ts != "1785854349.600129" {
		t.Errorf("ts = %q", ts)
	}
	if *calls != 1 {
		t.Errorf("calls = %d, want 1", *calls)
	}
}

// fakeSlackStatus serves chat.postMessage, returning failStatus for the first
// failN calls, then a successful JSON response. Use failN=0 for always-fail.
func fakeSlackStatus(t *testing.T, failStatus int, failN int32) (*httptest.Server, *int32) {
	t.Helper()
	var calls int32
	var succeeded int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if failN == 0 || n <= failN {
			w.WriteHeader(failStatus)
			return
		}
		atomic.AddInt32(&succeeded, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C123","ts":"1785854349.600129"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestPostRetriesOn429(t *testing.T) {
	srv, calls := fakeSlack(t, 1) // fail once, then succeed
	c := New("xoxb-test", WithAPIURL(srv.URL+"/"), WithMinInterval(0), WithMaxRetries(3))
	start := time.Now()
	ts, err := c.Post(context.Background(), "C123", "", "hello")
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if ts == "" {
		t.Error("expected a ts")
	}
	if *calls != 2 {
		t.Errorf("calls = %d, want 2", *calls)
	}
	if time.Since(start) < 900*time.Millisecond {
		t.Error("expected to honor Retry-After ~1s")
	}
}

func TestPostRetriesOn5xx(t *testing.T) {
	srv, calls := fakeSlackStatus(t, http.StatusInternalServerError, 1) // 500 once, then succeed
	c := New("xoxb-test", WithAPIURL(srv.URL+"/"), WithMinInterval(0), WithMaxRetries(3))
	ts, err := c.Post(context.Background(), "C123", "", "hello")
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if ts == "" {
		t.Error("expected a ts")
	}
	if *calls != 2 {
		t.Errorf("calls = %d, want 2 (1 failure + 1 success)", *calls)
	}
}

func TestPostDoesNotRetryPermanentError(t *testing.T) {
	// HTTP 401 is a permanent 4xx; the client must return immediately without retrying.
	srv, calls := fakeSlackStatus(t, http.StatusUnauthorized, 0) // always-fail
	c := New("xoxb-test", WithAPIURL(srv.URL+"/"), WithMinInterval(0), WithMaxRetries(5))
	_, err := c.Post(context.Background(), "C123", "", "hello")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if *calls != 1 {
		t.Errorf("calls = %d, want 1 (no retries on permanent error)", *calls)
	}
}

func TestPostRateLimitSpacing(t *testing.T) {
	// Two posts with a positive min interval are spaced by at least that interval.
	// This is a lower-bound timing assertion: a slow machine only adds delay, it
	// never removes it, so the test does not flake under load. The first post is
	// immediate (no prior call); the second must wait ~minInterval.
	srv, calls := fakeSlack(t, 0) // never fails
	c := New("xoxb-test", WithAPIURL(srv.URL+"/"), WithMinInterval(100*time.Millisecond))
	start := time.Now()
	for i := 0; i < 2; i++ {
		if _, err := c.Post(context.Background(), "C123", "", "hi"); err != nil {
			t.Fatalf("Post %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed < 90*time.Millisecond {
		t.Errorf("two posts took %v, want >= ~100ms spacing", elapsed)
	}
	if *calls != 2 {
		t.Errorf("calls = %d, want 2", *calls)
	}
}
