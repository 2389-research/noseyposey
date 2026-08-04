// ABOUTME: Tests for the SQLite-backed thread and dedup store.
// ABOUTME: Uses t.TempDir() so no test databases are committed to git.
package store

import (
	"path/filepath"
	"testing"
	"time"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestThreadRoundTrip(t *testing.T) {
	s := openTemp(t)
	if _, ok, err := s.ThreadTS("ivan-desk", "2026-08-03"); err != nil || ok {
		t.Fatalf("expected miss, got ok=%v err=%v", ok, err)
	}
	if err := s.SaveThread("ivan-desk", "2026-08-03", "C123", "111.222"); err != nil {
		t.Fatalf("SaveThread: %v", err)
	}
	ts, ok, err := s.ThreadTS("ivan-desk", "2026-08-03")
	if err != nil || !ok || ts != "111.222" {
		t.Fatalf("ThreadTS = (%q,%v,%v)", ts, ok, err)
	}
	// different day is a different thread
	if _, ok, _ := s.ThreadTS("ivan-desk", "2026-08-04"); ok {
		t.Fatal("expected miss for different day")
	}
}

func TestDedup(t *testing.T) {
	s := openTemp(t)
	posted, err := s.AlreadyPosted("ivan-desk", "k1")
	if err != nil || posted {
		t.Fatalf("expected not posted, got %v err=%v", posted, err)
	}
	if err := s.MarkPosted("ivan-desk", "k1"); err != nil {
		t.Fatalf("MarkPosted: %v", err)
	}
	posted, err = s.AlreadyPosted("ivan-desk", "k1")
	if err != nil || !posted {
		t.Fatalf("expected posted, got %v err=%v", posted, err)
	}
	// MarkPosted is idempotent
	if err := s.MarkPosted("ivan-desk", "k1"); err != nil {
		t.Fatalf("second MarkPosted: %v", err)
	}
}

func TestPrune(t *testing.T) {
	s := openTemp(t)
	if err := s.MarkPosted("ivan-desk", "old"); err != nil {
		t.Fatal(err)
	}
	// prune everything posted before "now + 1h" → removes the row
	if err := s.Prune(time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Prune: %v", err)
	}
	posted, _ := s.AlreadyPosted("ivan-desk", "old")
	if posted {
		t.Fatal("expected pruned row to be gone")
	}
}

func TestRestartSurvival(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "survive.db")

	// First handle: write thread and posted record.
	s1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open s1: %v", err)
	}
	if err := s1.SaveThread("ivan-desk", "2026-08-04", "C999", "500.600"); err != nil {
		t.Fatalf("SaveThread: %v", err)
	}
	if err := s1.MarkPosted("ivan-desk", "ts|abc12345"); err != nil {
		t.Fatalf("MarkPosted: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close s1: %v", err)
	}

	// Second handle at the same path: verify state survived.
	s2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open s2: %v", err)
	}
	defer func() { _ = s2.Close() }()

	ts, ok, err := s2.ThreadTS("ivan-desk", "2026-08-04")
	if err != nil || !ok || ts != "500.600" {
		t.Fatalf("ThreadTS after restart = (%q, %v, %v); want (\"500.600\", true, nil)", ts, ok, err)
	}
	posted, err := s2.AlreadyPosted("ivan-desk", "ts|abc12345")
	if err != nil || !posted {
		t.Fatalf("AlreadyPosted after restart = (%v, %v); want (true, nil)", posted, err)
	}
}
