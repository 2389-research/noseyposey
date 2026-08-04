// ABOUTME: SQLite-backed state for noseyposey: per-day thread ids and post dedup.
// ABOUTME: Uses the CGo-free modernc.org/sqlite driver so the binary stays static.
package store

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Store persists device→thread mappings and a dedup ledger.
type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS threads (
    device     TEXT NOT NULL,
    date       TEXT NOT NULL,
    channel    TEXT NOT NULL,
    thread_ts  TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (device, date)
);
CREATE TABLE IF NOT EXISTS posted (
    device    TEXT NOT NULL,
    ts_key    TEXT NOT NULL,
    posted_at TEXT NOT NULL,
    PRIMARY KEY (device, ts_key)
);`

// Open opens (creating if needed) the SQLite database and applies the schema.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // serialize writes; avoids "database is locked"
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// ThreadTS returns the stored thread timestamp for a device+date, if present.
func (s *Store) ThreadTS(device, date string) (string, bool, error) {
	var ts string
	err := s.db.QueryRow(
		`SELECT thread_ts FROM threads WHERE device = ? AND date = ?`,
		device, date,
	).Scan(&ts)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("query thread: %w", err)
	}
	return ts, true, nil
}

// SaveThread records the thread root for a device+date.
func (s *Store) SaveThread(device, date, channel, ts string) error {
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO threads (device, date, channel, thread_ts, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		device, date, channel, ts, time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("save thread: %w", err)
	}
	return nil
}

// AlreadyPosted reports whether (device, tsKey) has been posted.
func (s *Store) AlreadyPosted(device, tsKey string) (bool, error) {
	var one int
	err := s.db.QueryRow(
		`SELECT 1 FROM posted WHERE device = ? AND ts_key = ?`,
		device, tsKey,
	).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query posted: %w", err)
	}
	return true, nil
}

// MarkPosted records (device, tsKey) as posted. Idempotent.
func (s *Store) MarkPosted(device, tsKey string) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO posted (device, ts_key, posted_at) VALUES (?, ?, ?)`,
		device, tsKey, time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("mark posted: %w", err)
	}
	return nil
}

// Prune deletes dedup rows recorded before the given time.
func (s *Store) Prune(before time.Time) error {
	_, err := s.db.Exec(
		`DELETE FROM posted WHERE posted_at < ?`,
		before.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("prune posted: %w", err)
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }
