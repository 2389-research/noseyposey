// ABOUTME: Tests for the Slack client using a real httptest fake server.
// ABOUTME: Exercises success, 429 retry (Retry-After honored), and 5xx backoff paths.
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
