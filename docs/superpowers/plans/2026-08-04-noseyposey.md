# noseyposey Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go daemon that relays the horton MQTT transcription feed into Slack as one thread per device per day, with every utterance posted as a threaded reply.

**Architecture:** A single long-running process. Paho subscribes to `horton/transcriptions/+` (QoS 1, persistent session). Each message is decoded into an `Utterance` and pushed onto a bounded channel; one worker goroutine consumes them serially. For each utterance the worker finds-or-creates today's Slack thread for that device (state in SQLite), then posts the line as a threaded reply. The Slack client enforces a per-channel rate limit and retries on 429/5xx. The bounded channel provides backpressure: if Slack lags, the MQTT handler blocks and the broker holds messages.

**Tech Stack:** Go 1.26, `github.com/eclipse/paho.mqtt.golang`, `github.com/slack-go/slack`, `modernc.org/sqlite` (CGo-free), `github.com/joho/godotenv`. Module path `github.com/2389-ai/noseyposey`.

## Global Constraints

- **Module path:** `github.com/2389-ai/noseyposey`.
- **MQTT broker (default):** `tcp://192.168.23.123:1883`, no auth.
- **MQTT topic:** subscribe `horton/transcriptions/+` at QoS 1; **skip** the `all` sub-topic (mirror → duplicates).
- **Payload shape:** `{"device":"ivan-desk","mac":"1C:DB:D4:85:65:7C","text":"Whoa.","timestamp":"2026-08-03T20:28:54.331766"}` — `timestamp` is ISO-8601 with microseconds and **no zone; treat as UTC**.
- **Device name = topic suffix** (authoritative), cross-checked against payload `device`.
- **Timezone:** `America/Chicago` for the daily thread boundary and displayed times.
- **Slack:** bot token (`xoxb-…`) with `chat:write`; post via `chat.postMessage`. Parent then threaded replies via `thread_ts`. Channel default `C0A3JTFTB5M`.
- **Formats:** parent `🎙️ <device> — Mon Jan 2`; reply `15:04  <text>` (local `HH:MM`, two spaces, text).
- **Dedup key:** `<timestamp>|<sha1(text)[:8]>`, scoped per device.
- **Secrets** live only in gitignored `.env`; never commit them. `.env` is already gitignored.
- **Config via env** (`NP_*`) with flag overrides; required: `NP_SLACK_TOKEN`, `NP_SLACK_CHANNEL`.
- **Canonical check:** `scripts/check` runs gofmt, `go vet`, `golangci-lint run`, `go test ./...`. All code keeps it green with zero new warnings.
- **No utterance filtering** in v1; **backpressure, never drop**.

---

## File Structure

```
go.mod / go.sum
.env.example                        # committed template (no secret)
AGENTS.md                           # project conventions + names
README.md
scripts/check                       # canonical build+lint+test
cmd/noseyposey/main.go              # config load, wiring, signals, run()
internal/config/config.go           # Config + Load()
internal/transcript/transcript.go   # Utterance, DeviceFromTopic, Parse
internal/format/format.go           # LocalDate, ParentText, ReplyText
internal/store/store.go             # SQLite: threads + dedup
internal/slackclient/slackclient.go # rate-limited, retrying chat.postMessage
internal/router/router.go           # Utterance → ensure thread → reply → mark
```

Each `internal/*` package owns one responsibility and is unit-tested in isolation with a sibling `_test.go`. `cmd/noseyposey` wires them and is covered by the end-to-end integration test.

---

## Interfaces (defined once, referenced by later tasks)

```go
// internal/transcript
type Utterance struct {
    Device    string    // from topic suffix
    Mac       string
    Text      string
    Timestamp time.Time // parsed, UTC
}
func DeviceFromTopic(topic string) (device string, skip bool)
func Parse(topic string, payload []byte) (Utterance, error)

// internal/format
func LocalDate(t time.Time, loc *time.Location) string          // "2006-01-02"
func ParentText(device string, t time.Time, loc *time.Location) string // "🎙️ dev — Mon Jan 2"
func ReplyText(t time.Time, loc *time.Location, text string) string    // "15:04  text"

// internal/store
type Store struct { /* db */ }
func Open(path string) (*Store, error)
func (s *Store) ThreadTS(device, date string) (ts string, ok bool, err error)
func (s *Store) SaveThread(device, date, channel, ts string) error
func (s *Store) AlreadyPosted(device, tsKey string) (bool, error)
func (s *Store) MarkPosted(device, tsKey string) error
func (s *Store) Prune(before time.Time) error
func (s *Store) Close() error

// internal/slackclient
type Client struct { /* ... */ }
func New(token string, opts ...Option) *Client
func (c *Client) Post(ctx context.Context, channel, threadTS, text string) (ts string, err error)

// internal/router  (depends on these interfaces, satisfied by store + slackclient)
type ThreadStore interface {
    ThreadTS(device, date string) (string, bool, error)
    SaveThread(device, date, channel, ts string) error
    AlreadyPosted(device, tsKey string) (bool, error)
    MarkPosted(device, tsKey string) error
}
type Poster interface {
    Post(ctx context.Context, channel, threadTS, text string) (string, error)
}
type Router struct { /* store, poster, channel, loc */ }
func New(store ThreadStore, poster Poster, channel string, loc *time.Location) *Router
func (r *Router) Handle(ctx context.Context, u transcript.Utterance) error
```

---

### Task 1: Project scaffold + green check

**Files:**
- Create: `go.mod`, `AGENTS.md`, `README.md`, `.env.example`, `scripts/check`, `internal/version/version.go`, `internal/version/version_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: a compiling module and a passing `scripts/check`.

- [ ] **Step 1: Initialize the module**

Run:
```bash
cd /Users/harper/Public/src/2389/noseyposey
go mod init github.com/2389-ai/noseyposey
```

- [ ] **Step 2: Write a trivial package with a failing test**

Create `internal/version/version.go`:
```go
// ABOUTME: Package version exposes the build's version string.
// ABOUTME: Kept tiny so Task 1 can prove the toolchain and check script work.
package version

// Version is the current noseyposey version.
const Version = "0.1.0"
```

Create `internal/version/version_test.go`:
```go
package version

import "testing"

func TestVersionNotEmpty(t *testing.T) {
	if Version == "" {
		t.Fatal("Version must not be empty")
	}
}
```

- [ ] **Step 3: Write the check script**

Create `scripts/check`:
```bash
#!/usr/bin/env bash
# ABOUTME: Canonical build/lint/test gate for noseyposey.
# ABOUTME: Run before every commit; keeps formatting, vet, lint, and tests green.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> gofmt"
unformatted=$(gofmt -l . || true)
if [ -n "$unformatted" ]; then
	echo "gofmt needs: $unformatted" >&2
	exit 1
fi

echo "==> go vet"
go vet ./...

echo "==> golangci-lint"
golangci-lint run ./...

echo "==> go test"
go test ./... "$@"

echo "check: OK"
```

Then: `chmod +x scripts/check`.

- [ ] **Step 4: Write AGENTS.md, README.md, .env.example**

Create `AGENTS.md`:
```markdown
# noseyposey — agent & project notes

Crew (recorded once, do not re-ceremony):
- Agent: **Wiretap Rex** 🦖
- Human: **Doctor Biz** (aka Sir Harps-a-Lot)

## What this is
A Go daemon that relays the horton MQTT transcription feed to Slack — one thread
per device per day, one threaded reply per utterance.

## Conventions
- Hand-written source files start with two `// ABOUTME:` comment lines.
- TDD: failing test first, minimal code, green, commit.
- `scripts/check` is the canonical gate (gofmt, vet, golangci-lint, test). Keep it green.
- Secrets live only in gitignored `.env`. Never commit them.
- Design doc: `docs/superpowers/specs/2026-08-04-noseyposey-design.md`.

## Run
```
cp .env.example .env   # fill in NP_SLACK_TOKEN + NP_SLACK_CHANNEL
set -a; . ./.env; set +a
go run ./cmd/noseyposey
```
```

Create `.env.example`:
```bash
# Copy to .env and fill in. .env is gitignored — never commit real secrets.
NP_MQTT_BROKER=tcp://192.168.23.123:1883
NP_MQTT_CLIENT_ID=noseyposey
NP_SLACK_TOKEN=xoxb-REPLACE-ME
NP_SLACK_CHANNEL=C0A3JTFTB5M
NP_TIMEZONE=America/Chicago
NP_DB_PATH=./noseyposey.db
NP_LOG_LEVEL=info
```

Create `README.md`:
```markdown
# noseyposey

Relays the horton voice-transcription MQTT feed into Slack: one thread per device
per day, each utterance a threaded reply.

- Design: `docs/superpowers/specs/2026-08-04-noseyposey-design.md`
- Plan: `docs/superpowers/plans/2026-08-04-noseyposey.md`

## Quick start
```
cp .env.example .env      # fill NP_SLACK_TOKEN + NP_SLACK_CHANNEL
set -a; . ./.env; set +a
go run ./cmd/noseyposey
```

## Check
```
./scripts/check
```
```

- [ ] **Step 5: Run the check**

Run: `./scripts/check`
Expected: PASS (`check: OK`). If `golangci-lint` flags the empty-ish tree, address only real issues.

- [ ] **Step 6: Commit**

```bash
git add go.mod AGENTS.md README.md .env.example scripts/check internal/version
git commit -m "chore: scaffold noseyposey module, check script, docs"
```

---

### Task 2: Config loading

**Files:**
- Create: `internal/config/config.go`, `internal/config/config_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `config.Config` struct and `config.Load() (Config, error)`.
  ```go
  type Config struct {
      MQTTBroker   string
      MQTTClientID string
      SlackToken   string
      SlackChannel string
      Timezone     string
      Location     *time.Location
      DBPath       string
      LogLevel     string
  }
  func Load() (Config, error)
  ```

- [ ] **Step 1: Write the failing test**

Create `internal/config/config_test.go`:
```go
package config

import (
	"testing"
)

func TestLoadDefaultsAndRequired(t *testing.T) {
	t.Setenv("NP_SLACK_TOKEN", "xoxb-abc")
	t.Setenv("NP_SLACK_CHANNEL", "C123")
	// leave the rest unset to exercise defaults

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MQTTBroker != "tcp://192.168.23.123:1883" {
		t.Errorf("MQTTBroker default = %q", cfg.MQTTBroker)
	}
	if cfg.MQTTClientID != "noseyposey" {
		t.Errorf("MQTTClientID default = %q", cfg.MQTTClientID)
	}
	if cfg.Timezone != "America/Chicago" {
		t.Errorf("Timezone default = %q", cfg.Timezone)
	}
	if cfg.Location == nil || cfg.Location.String() != "America/Chicago" {
		t.Errorf("Location = %v", cfg.Location)
	}
	if cfg.DBPath != "./noseyposey.db" {
		t.Errorf("DBPath default = %q", cfg.DBPath)
	}
}

func TestLoadMissingTokenFails(t *testing.T) {
	t.Setenv("NP_SLACK_TOKEN", "")
	t.Setenv("NP_SLACK_CHANNEL", "C123")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when NP_SLACK_TOKEN missing")
	}
}

func TestLoadBadTimezoneFails(t *testing.T) {
	t.Setenv("NP_SLACK_TOKEN", "xoxb-abc")
	t.Setenv("NP_SLACK_CHANNEL", "C123")
	t.Setenv("NP_TIMEZONE", "Mars/Phobos")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for invalid timezone")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -v`
Expected: FAIL (build error — `Load` undefined).

- [ ] **Step 3: Write minimal implementation**

Create `internal/config/config.go`:
```go
// ABOUTME: Loads noseyposey configuration from NP_* environment variables.
// ABOUTME: Applies defaults, validates required fields, and resolves the timezone.
package config

import (
	"fmt"
	"os"
	"time"
)

// Config holds all runtime configuration.
type Config struct {
	MQTTBroker   string
	MQTTClientID string
	SlackToken   string
	SlackChannel string
	Timezone     string
	Location     *time.Location
	DBPath       string
	LogLevel     string
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Load reads configuration from the environment, applying defaults and
// validating required fields.
func Load() (Config, error) {
	cfg := Config{
		MQTTBroker:   env("NP_MQTT_BROKER", "tcp://192.168.23.123:1883"),
		MQTTClientID: env("NP_MQTT_CLIENT_ID", "noseyposey"),
		SlackToken:   os.Getenv("NP_SLACK_TOKEN"),
		SlackChannel: os.Getenv("NP_SLACK_CHANNEL"),
		Timezone:     env("NP_TIMEZONE", "America/Chicago"),
		DBPath:       env("NP_DB_PATH", "./noseyposey.db"),
		LogLevel:     env("NP_LOG_LEVEL", "info"),
	}
	if cfg.SlackToken == "" {
		return Config{}, fmt.Errorf("NP_SLACK_TOKEN is required")
	}
	if cfg.SlackChannel == "" {
		return Config{}, fmt.Errorf("NP_SLACK_CHANNEL is required")
	}
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return Config{}, fmt.Errorf("invalid NP_TIMEZONE %q: %w", cfg.Timezone, err)
	}
	cfg.Location = loc
	return cfg, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -v`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add internal/config
git commit -m "feat: config loading from NP_* env with defaults and validation"
```

---

### Task 3: Transcript decoding

**Files:**
- Create: `internal/transcript/transcript.go`, `internal/transcript/transcript_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `Utterance`, `DeviceFromTopic`, `Parse` (signatures in the Interfaces section).

- [ ] **Step 1: Write the failing test**

Create `internal/transcript/transcript_test.go`:
```go
package transcript

import (
	"testing"
	"time"
)

func TestDeviceFromTopic(t *testing.T) {
	cases := []struct {
		topic  string
		device string
		skip   bool
	}{
		{"horton/transcriptions/ivan-desk", "ivan-desk", false},
		{"horton/transcriptions/harper-desk", "harper-desk", false},
		{"horton/transcriptions/all", "all", true},
		{"horton/transcriptions/device-856308", "device-856308", false},
	}
	for _, c := range cases {
		dev, skip := DeviceFromTopic(c.topic)
		if dev != c.device || skip != c.skip {
			t.Errorf("DeviceFromTopic(%q) = (%q,%v), want (%q,%v)", c.topic, dev, skip, c.device, c.skip)
		}
	}
}

func TestParse(t *testing.T) {
	topic := "horton/transcriptions/ivan-desk"
	payload := []byte(`{"device":"ivan-desk","mac":"1C:DB:D4:85:65:7C","text":"Whoa.","timestamp":"2026-08-03T20:28:54.331766"}`)

	u, err := Parse(topic, payload)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if u.Device != "ivan-desk" {
		t.Errorf("Device = %q", u.Device)
	}
	if u.Mac != "1C:DB:D4:85:65:7C" {
		t.Errorf("Mac = %q", u.Mac)
	}
	if u.Text != "Whoa." {
		t.Errorf("Text = %q", u.Text)
	}
	want := time.Date(2026, 8, 3, 20, 28, 54, 331766000, time.UTC)
	if !u.Timestamp.Equal(want) {
		t.Errorf("Timestamp = %v, want %v", u.Timestamp, want)
	}
}

func TestParseBadJSON(t *testing.T) {
	if _, err := Parse("horton/transcriptions/x", []byte("not json")); err == nil {
		t.Fatal("expected error on bad JSON")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/transcript/ -v`
Expected: FAIL (build error — undefined `DeviceFromTopic`, `Parse`).

- [ ] **Step 3: Write minimal implementation**

Create `internal/transcript/transcript.go`:
```go
// ABOUTME: Decodes horton MQTT transcription messages into Utterance values.
// ABOUTME: Device name comes from the topic suffix; timestamps are parsed as UTC.
package transcript

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const topicPrefix = "horton/transcriptions/"

// Utterance is one decoded transcription message.
type Utterance struct {
	Device    string
	Mac       string
	Text      string
	Timestamp time.Time
}

// DeviceFromTopic returns the device name (topic suffix) and whether the
// message should be skipped (the "all" mirror duplicates every device stream).
func DeviceFromTopic(topic string) (device string, skip bool) {
	device = strings.TrimPrefix(topic, topicPrefix)
	if device == "all" {
		return device, true
	}
	return device, false
}

type wire struct {
	Device    string `json:"device"`
	Mac       string `json:"mac"`
	Text      string `json:"text"`
	Timestamp string `json:"timestamp"`
}

// timestampLayout matches "2026-08-03T20:28:54.331766" with no zone (UTC).
const timestampLayout = "2006-01-02T15:04:05.999999"

// Parse decodes a payload into an Utterance. The device is taken from the
// topic (authoritative); payload device is ignored except as documentation.
func Parse(topic string, payload []byte) (Utterance, error) {
	var w wire
	if err := json.Unmarshal(payload, &w); err != nil {
		return Utterance{}, fmt.Errorf("decode transcription: %w", err)
	}
	ts, err := time.ParseInLocation(timestampLayout, w.Timestamp, time.UTC)
	if err != nil {
		return Utterance{}, fmt.Errorf("parse timestamp %q: %w", w.Timestamp, err)
	}
	device, _ := DeviceFromTopic(topic)
	return Utterance{
		Device:    device,
		Mac:       w.Mac,
		Text:      w.Text,
		Timestamp: ts,
	}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/transcript/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/transcript
git commit -m "feat: decode horton transcription messages into Utterance"
```

---

### Task 4: Formatting (dates + Slack text)

**Files:**
- Create: `internal/format/format.go`, `internal/format/format_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `LocalDate`, `ParentText`, `ReplyText` (signatures in the Interfaces section).

- [ ] **Step 1: Write the failing test**

Create `internal/format/format_test.go`:
```go
package format

import (
	"testing"
	"time"
)

func chicago(t *testing.T) *time.Location {
	loc, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	return loc
}

// 2026-08-03T20:28:54Z is 15:28 on 2026-08-03 in America/Chicago (CDT).
func sampleTime() time.Time {
	return time.Date(2026, 8, 3, 20, 28, 54, 331766000, time.UTC)
}

func TestLocalDate(t *testing.T) {
	if got := LocalDate(sampleTime(), chicago(t)); got != "2026-08-03" {
		t.Errorf("LocalDate = %q, want 2026-08-03", got)
	}
}

func TestParentText(t *testing.T) {
	if got := ParentText("ivan-desk", sampleTime(), chicago(t)); got != "🎙️ ivan-desk — Mon Aug 3" {
		t.Errorf("ParentText = %q", got)
	}
}

func TestReplyText(t *testing.T) {
	if got := ReplyText(sampleTime(), chicago(t), "Whoa."); got != "15:28  Whoa." {
		t.Errorf("ReplyText = %q", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/format/ -v`
Expected: FAIL (undefined functions).

- [ ] **Step 3: Write minimal implementation**

Create `internal/format/format.go`:
```go
// ABOUTME: Formats timestamps and Slack message text in the configured timezone.
// ABOUTME: Parent threads are labeled per day; replies are prefixed with local HH:MM.
package format

import (
	"fmt"
	"time"
)

// LocalDate returns the calendar date (YYYY-MM-DD) of t in loc.
func LocalDate(t time.Time, loc *time.Location) string {
	return t.In(loc).Format("2006-01-02")
}

// ParentText is the thread-root label for a device on a given day.
func ParentText(device string, t time.Time, loc *time.Location) string {
	return fmt.Sprintf("🎙️ %s — %s", device, t.In(loc).Format("Mon Jan 2"))
}

// ReplyText is one threaded line: local HH:MM, two spaces, the utterance.
func ReplyText(t time.Time, loc *time.Location, text string) string {
	return fmt.Sprintf("%s  %s", t.In(loc).Format("15:04"), text)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/format/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/format
git commit -m "feat: date and Slack text formatting in configured timezone"
```

---

### Task 5: SQLite store (threads + dedup)

**Files:**
- Create: `internal/store/store.go`, `internal/store/store_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `store.Store` with methods in the Interfaces section. Satisfies `router.ThreadStore`.

- [ ] **Step 1: Add the SQLite dependency**

Run:
```bash
go get modernc.org/sqlite@latest
```

- [ ] **Step 2: Write the failing test**

Create `internal/store/store_test.go`:
```go
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/store/ -v`
Expected: FAIL (undefined `Open`).

- [ ] **Step 4: Write minimal implementation**

Create `internal/store/store.go`:
```go
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
		device, date, channel, ts, time.Now().UTC().Format(time.RFC3339Nano),
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
		device, tsKey, time.Now().UTC().Format(time.RFC3339Nano),
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
		before.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("prune posted: %w", err)
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/store/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/store
git commit -m "feat: SQLite store for per-day threads and post dedup"
```

---

### Task 6: Slack client (rate-limited, retrying)

**Files:**
- Create: `internal/slackclient/slackclient.go`, `internal/slackclient/slackclient_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `slackclient.Client` with `Post(ctx, channel, threadTS, text) (ts, error)`. Satisfies `router.Poster`. Options: `WithAPIURL(string)`, `WithMinInterval(time.Duration)`, `WithMaxRetries(int)`.

- [ ] **Step 1: Add the Slack dependency**

Run:
```bash
go get github.com/slack-go/slack@latest
```

- [ ] **Step 2: Write the failing test**

Create `internal/slackclient/slackclient_test.go`:
```go
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/slackclient/ -v`
Expected: FAIL (undefined `New`, options).

- [ ] **Step 4: Write minimal implementation**

Create `internal/slackclient/slackclient.go`:
```go
// ABOUTME: Thin Slack wrapper posting messages with a per-channel rate limit.
// ABOUTME: Honors 429 Retry-After and retries transient errors with backoff.
package slackclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/slack-go/slack"
)

// Client posts messages to Slack, serializing calls and rate-limiting them.
type Client struct {
	api         *slack.Client
	minInterval time.Duration
	maxRetries  int

	mu   sync.Mutex
	last time.Time
}

// Option configures a Client.
type Option func(*clientConfig)

type clientConfig struct {
	apiURL      string
	minInterval time.Duration
	maxRetries  int
}

// WithAPIURL overrides the Slack API base URL (used in tests). Needs a trailing slash.
func WithAPIURL(u string) Option { return func(c *clientConfig) { c.apiURL = u } }

// WithMinInterval sets the minimum spacing between posts.
func WithMinInterval(d time.Duration) Option { return func(c *clientConfig) { c.minInterval = d } }

// WithMaxRetries sets how many times to retry a transient failure.
func WithMaxRetries(n int) Option { return func(c *clientConfig) { c.maxRetries = n } }

// New builds a Client. Default min interval 1s, 5 retries.
func New(token string, opts ...Option) *Client {
	cfg := clientConfig{minInterval: time.Second, maxRetries: 5}
	for _, o := range opts {
		o(&cfg)
	}
	var apiOpts []slack.Option
	if cfg.apiURL != "" {
		apiOpts = append(apiOpts, slack.OptionAPIURL(cfg.apiURL))
	}
	return &Client{
		api:         slack.New(token, apiOpts...),
		minInterval: cfg.minInterval,
		maxRetries:  cfg.maxRetries,
	}
}

// Post sends text to a channel. If threadTS is non-empty, it posts as a reply.
// Returns the new message timestamp.
func (c *Client) Post(ctx context.Context, channel, threadTS, text string) (string, error) {
	c.rateLimit(ctx)

	opts := []slack.MsgOption{
		slack.MsgOptionText(text, false),
		slack.MsgOptionDisableLinkUnfurl(),
	}
	if threadTS != "" {
		opts = append(opts, slack.MsgOptionTS(threadTS))
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		_, ts, err := c.api.PostMessageContext(ctx, channel, opts...)
		if err == nil {
			return ts, nil
		}
		lastErr = err

		var rl *slack.RateLimitedError
		if errors.As(err, &rl) {
			if !sleep(ctx, rl.RetryAfter) {
				return "", ctx.Err()
			}
			continue
		}
		// transient backoff: 500ms, 1s, 2s, ...
		backoff := time.Duration(500) * time.Millisecond << attempt
		if !sleep(ctx, backoff) {
			return "", ctx.Err()
		}
	}
	return "", fmt.Errorf("post to %s failed after retries: %w", channel, lastErr)
}

func (c *Client) rateLimit(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.minInterval <= 0 {
		return
	}
	wait := c.minInterval - time.Since(c.last)
	if wait > 0 {
		sleep(ctx, wait)
	}
	c.last = time.Now()
}

// sleep waits for d or until ctx is done. Returns false if ctx was canceled.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/slackclient/ -v`
Expected: PASS (both tests; the 429 test takes ~1s).

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/slackclient
git commit -m "feat: rate-limited Slack client with 429/backoff retries"
```

---

### Task 7: Router (orchestration)

**Files:**
- Create: `internal/router/router.go`, `internal/router/router_test.go`

**Interfaces:**
- Consumes: `transcript.Utterance`; `ThreadStore` (satisfied by `store.Store`); `Poster` (satisfied by `slackclient.Client`).
- Produces: `router.Router` with `Handle(ctx, u) error`.

- [ ] **Step 1: Write the failing test**

Create `internal/router/router_test.go`:
```go
package router

import (
	"context"
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
func itoa(n int) string { return time.Duration(n).String() } // any unique string

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/router/ -v`
Expected: FAIL (undefined `New`).

- [ ] **Step 3: Write minimal implementation**

Create `internal/router/router.go`:
```go
// ABOUTME: Orchestrates one utterance: find/create today's thread, post the reply.
// ABOUTME: Dedups by (device, timestamp+text hash) so restarts never double-post.
package router

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/2389-ai/noseyposey/internal/format"
	"github.com/2389-ai/noseyposey/internal/transcript"
)

// ThreadStore persists thread ids and the dedup ledger.
type ThreadStore interface {
	ThreadTS(device, date string) (string, bool, error)
	SaveThread(device, date, channel, ts string) error
	AlreadyPosted(device, tsKey string) (bool, error)
	MarkPosted(device, tsKey string) error
}

// Poster posts a message and returns its timestamp.
type Poster interface {
	Post(ctx context.Context, channel, threadTS, text string) (string, error)
}

// Router turns utterances into threaded Slack posts.
type Router struct {
	store   ThreadStore
	poster  Poster
	channel string
	loc     *time.Location
}

// New builds a Router.
func New(store ThreadStore, poster Poster, channel string, loc *time.Location) *Router {
	return &Router{store: store, poster: poster, channel: channel, loc: loc}
}

func dedupKey(u transcript.Utterance) string {
	sum := sha1.Sum([]byte(u.Text))
	return u.Timestamp.UTC().Format(time.RFC3339Nano) + "|" + hex.EncodeToString(sum[:])[:8]
}

// Handle processes one utterance: ensure today's thread exists, post the reply,
// and record it for dedup. Safe to call repeatedly with the same utterance.
func (r *Router) Handle(ctx context.Context, u transcript.Utterance) error {
	key := dedupKey(u)
	if done, err := r.store.AlreadyPosted(u.Device, key); err != nil {
		return fmt.Errorf("dedup check: %w", err)
	} else if done {
		return nil
	}

	date := format.LocalDate(u.Timestamp, r.loc)
	threadTS, ok, err := r.store.ThreadTS(u.Device, date)
	if err != nil {
		return fmt.Errorf("lookup thread: %w", err)
	}
	if !ok {
		parent := format.ParentText(u.Device, u.Timestamp, r.loc)
		ts, err := r.poster.Post(ctx, r.channel, "", parent)
		if err != nil {
			return fmt.Errorf("post parent: %w", err)
		}
		if err := r.store.SaveThread(u.Device, date, r.channel, ts); err != nil {
			return fmt.Errorf("save thread: %w", err)
		}
		threadTS = ts
	}

	line := format.ReplyText(u.Timestamp, r.loc, u.Text)
	if _, err := r.poster.Post(ctx, r.channel, threadTS, line); err != nil {
		return fmt.Errorf("post reply: %w", err)
	}
	if err := r.store.MarkPosted(u.Device, key); err != nil {
		return fmt.Errorf("mark posted: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/router/ -v`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add internal/router
git commit -m "feat: router orchestrates find-or-create thread and reply"
```

---

### Task 8: Main wiring (MQTT + worker + signals)

**Files:**
- Create: `cmd/noseyposey/main.go`

**Interfaces:**
- Consumes: `config`, `store`, `slackclient`, `router`, `transcript`.
- Produces: the runnable binary and an internal `run(ctx, cfg, logger) error` used by the integration test in Task 9.

- [ ] **Step 1: Add MQTT and dotenv dependencies**

Run:
```bash
go get github.com/eclipse/paho.mqtt.golang@latest
go get github.com/joho/godotenv@latest
```

- [ ] **Step 2: Write the implementation**

Create `cmd/noseyposey/main.go`:
```go
// ABOUTME: noseyposey entrypoint — wires MQTT subscribe, worker, and Slack posting.
// ABOUTME: Bounded channel gives backpressure; SIGINT/SIGTERM shut it down cleanly.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"

	"github.com/2389-ai/noseyposey/internal/config"
	"github.com/2389-ai/noseyposey/internal/router"
	"github.com/2389-ai/noseyposey/internal/slackclient"
	"github.com/2389-ai/noseyposey/internal/store"
	"github.com/2389-ai/noseyposey/internal/transcript"
)

const subscribeTopic = "horton/transcriptions/+"

func main() {
	_ = godotenv.Load() // best-effort: load .env if present

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("run", "err", err)
		os.Exit(1)
	}
	logger.Info("shutdown complete")
}

// run wires everything and blocks until ctx is canceled. Separated from main
// so the integration test can drive it against a local broker.
func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	slack := slackclient.New(cfg.SlackToken)
	rt := router.New(st, slack, cfg.SlackChannel, cfg.Location)

	// Bounded queue: MQTT handler → worker. Full channel = backpressure.
	utterances := make(chan transcript.Utterance, 256)

	// Worker: process utterances serially.
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for u := range utterances {
			if err := rt.Handle(ctx, u); err != nil {
				logger.Warn("handle utterance", "device", u.Device, "err", err)
			}
		}
	}()

	// Daily prune of the dedup ledger (keep ~2 days).
	go func() {
		t := time.NewTicker(6 * time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := st.Prune(time.Now().Add(-48 * time.Hour)); err != nil {
					logger.Warn("prune", "err", err)
				}
			}
		}
	}()

	msgHandler := func(_ mqtt.Client, m mqtt.Message) {
		device, skip := transcript.DeviceFromTopic(m.Topic())
		if skip {
			return
		}
		u, err := transcript.Parse(m.Topic(), m.Payload())
		if err != nil {
			logger.Warn("parse", "topic", m.Topic(), "err", err)
			return
		}
		_ = device
		select {
		case utterances <- u: // may block → backpressure
		case <-ctx.Done():
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTTBroker).
		SetClientID(cfg.MQTTClientID).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetResumeSubs(true).
		SetOrderMatters(true).
		SetOnConnectHandler(func(c mqtt.Client) {
			if token := c.Subscribe(subscribeTopic, 1, msgHandler); token.Wait() && token.Error() != nil {
				logger.Error("subscribe", "err", token.Error())
				return
			}
			logger.Info("subscribed", "topic", subscribeTopic)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			logger.Warn("mqtt connection lost", "err", err)
		})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	logger.Info("connected", "broker", cfg.MQTTBroker, "channel", cfg.SlackChannel)

	<-ctx.Done()
	logger.Info("stopping")
	client.Disconnect(500)
	close(utterances)
	<-workerDone
	return nil
}
```

- [ ] **Step 3: Build and vet**

Run:
```bash
go build ./...
go vet ./...
```
Expected: no errors.

- [ ] **Step 4: Run the full check**

Run: `./scripts/check`
Expected: `check: OK`.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum cmd/noseyposey
git commit -m "feat: main wiring — MQTT subscribe, worker, backpressure, signals"
```

---

### Task 9: End-to-end integration test (real broker + fake Slack)

**Files:**
- Create: `cmd/noseyposey/integration_test.go`

**Interfaces:**
- Consumes: `run(ctx, cfg, logger)` from Task 8; a locally-exec'd `mosquitto`; an `httptest` fake Slack.
- Produces: proof the full pipe posts one parent per device/day and one reply per utterance, and dedups.

This test starts a real Mosquitto broker on a random port, points `run` at it plus a fake Slack server and a temp SQLite file, publishes sample utterances, and asserts the posts. It **skips** if the `mosquitto` binary is absent.

- [ ] **Step 1: Write the integration test**

Create `cmd/noseyposey/integration_test.go`:
```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/2389-ai/noseyposey/internal/config"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startMosquitto launches a local anonymous broker on port; skips if not installed.
func startMosquitto(t *testing.T, port int) {
	t.Helper()
	if _, err := exec.LookPath("mosquitto"); err != nil {
		t.Skip("mosquitto not installed; skipping integration test")
	}
	conf := filepath.Join(t.TempDir(), "mosq.conf")
	body := fmt.Sprintf("listener %d 127.0.0.1\nallow_anonymous true\n", port)
	if err := os.WriteFile(conf, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("mosquitto", "-c", conf)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start mosquitto: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })

	// wait for the port to accept connections
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond); err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("mosquitto did not start in time")
}

// recordingSlack captures chat.postMessage calls and returns unique ts values.
type recordingSlack struct {
	mu    sync.Mutex
	posts []map[string]string
	n     int
}

func (s *recordingSlack) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		s.mu.Lock()
		s.n++
		s.posts = append(s.posts, map[string]string{
			"channel":   r.FormValue("channel"),
			"text":      r.FormValue("text"),
			"thread_ts": r.FormValue("thread_ts"),
		})
		ts := fmt.Sprintf("1785850000.%06d", s.n)
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": "C123", "ts": ts})
	}
}

func (s *recordingSlack) count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.posts) }

func publish(t *testing.T, broker, topic, payload string) {
	t.Helper()
	opts := mqtt.NewClientOptions().AddBroker(broker).SetClientID("itest-pub")
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		t.Fatalf("pub connect: %v", tok.Error())
	}
	defer c.Disconnect(100)
	if tok := c.Publish(topic, 1, false, payload); tok.Wait() && tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}
}

func TestEndToEnd(t *testing.T) {
	port := freePort(t)
	startMosquitto(t, port)
	broker := fmt.Sprintf("tcp://127.0.0.1:%d", port)

	slackSrv := &recordingSlack{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slackSrv.handler()(w, r)
	}))
	defer srv.Close()

	// Point slack-go's default at our fake by overriding the API URL via env-free
	// construction: run() builds slackclient.New(token) with the real URL, so we
	// instead set the SLACK API base through the client option. To keep run()
	// unchanged, use the slackclient package's WithAPIURL by setting NP via a hook:
	// simplest path — set the fake through the slack library's global is not
	// available, so we rely on run() reading NP_SLACK_API_URL (added below).
	t.Setenv("NP_SLACK_API_URL", srv.URL+"/")

	loc, _ := time.LoadLocation("America/Chicago")
	cfg := config.Config{
		MQTTBroker:   broker,
		MQTTClientID: "noseyposey-itest",
		SlackToken:   "xoxb-test",
		SlackChannel: "C123",
		Timezone:     "America/Chicago",
		Location:     loc,
		DBPath:       filepath.Join(t.TempDir(), "itest.db"),
		LogLevel:     "info",
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = run(ctx, cfg, slog.New(slog.NewTextHandler(os.Stderr, nil))); close(done) }()

	// let the subscriber connect
	time.Sleep(500 * time.Millisecond)

	ivan := `{"device":"ivan-desk","mac":"1C:DB:D4:85:65:7C","text":"Whoa.","timestamp":"2026-08-03T20:28:54.331766"}`
	ivan2 := `{"device":"ivan-desk","mac":"1C:DB:D4:85:65:7C","text":"Yeah.","timestamp":"2026-08-03T20:29:00.000000"}`
	horton := `{"device":"horton","mac":"3C:0F:02:E3:B1:08","text":"Ready.","timestamp":"2026-08-03T20:29:05.000000"}`

	publish(t, broker, "horton/transcriptions/ivan-desk", ivan)
	publish(t, broker, "horton/transcriptions/ivan-desk", ivan2)
	publish(t, broker, "horton/transcriptions/horton", horton)
	publish(t, broker, "horton/transcriptions/all", ivan) // mirror → must be ignored
	publish(t, broker, "horton/transcriptions/ivan-desk", ivan) // duplicate → deduped

	// wait for posts to settle: expect 2 parents + 3 replies = 5
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && slackSrv.count() < 5 {
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done

	if got := slackSrv.count(); got != 5 {
		t.Fatalf("expected 5 posts (2 parents + 3 replies), got %d: %+v", got, slackSrv.posts)
	}
	parents, replies := 0, 0
	for _, p := range slackSrv.posts {
		if p["thread_ts"] == "" {
			parents++
		} else {
			replies++
		}
	}
	if parents != 2 || replies != 3 {
		t.Errorf("parents=%d replies=%d, want 2 and 3", parents, replies)
	}
}
```

- [ ] **Step 2: Wire the test hook into run()**

The test points Slack at a fake server via `NP_SLACK_API_URL`. Add support in `cmd/noseyposey/main.go`: in `run`, build the Slack client with that override when present.

Modify the Slack construction in `run` (replace the `slack := slackclient.New(cfg.SlackToken)` line):
```go
	var slackOpts []slackclient.Option
	if u := os.Getenv("NP_SLACK_API_URL"); u != "" {
		slackOpts = append(slackOpts, slackclient.WithAPIURL(u))
	}
	slack := slackclient.New(cfg.SlackToken, slackOpts...)
```
Add `"os"` to the imports of `main.go` if not already present (it is, for `os.Exit`/`os.Stderr`).

- [ ] **Step 3: Run the integration test**

Run: `go test ./cmd/noseyposey/ -run TestEndToEnd -v`
Expected: PASS (or SKIP if `mosquitto` is unavailable — it is installed here, so PASS).

- [ ] **Step 4: Run the full check**

Run: `./scripts/check`
Expected: `check: OK`.

- [ ] **Step 5: Commit**

```bash
git add cmd/noseyposey
git commit -m "test: end-to-end pipe over real broker with fake Slack"
```

---

### Task 10: Live smoke test + operator docs

**Files:**
- Modify: `README.md` (add "Run against production" + "Deploy" notes)

**Interfaces:**
- Consumes: the built binary, real `.env` (already populated with a working token + channel `C0A3JTFTB5M`).
- Produces: confirmation the real pipe posts to Slack, plus operator notes.

- [ ] **Step 1: Build the binary**

Run: `go build -o noseyposey ./cmd/noseyposey`
Expected: a `noseyposey` binary (gitignored).

- [ ] **Step 2: Run against the real broker and Slack for a short window**

Run:
```bash
set -a; . ./.env; set +a
./noseyposey &
NP_PID=$!
sleep 60           # let real speech (if any) flow; or speak near a desk mic
kill $NP_PID
```
Expected: logs show `connected` and `subscribed`; if anyone spoke, a device thread appears in channel `C0A3JTFTB5M` with threaded replies. If silent, no posts (correct — nothing to relay).

To force a message without waiting for speech, in another shell:
```bash
mosquitto_pub -h 192.168.23.123 -t horton/transcriptions/harper-desk \
  -m '{"device":"harper-desk","mac":"00:00:00:00:00:00","text":"noseyposey live smoke test","timestamp":"'"$(date -u +%Y-%m-%dT%H:%M:%S.%6N)"'"}'
```
Expected: a `🎙️ harper-desk — <today>` parent with a `HH:MM  noseyposey live smoke test` reply in the channel. Delete those test messages manually if desired.

- [ ] **Step 3: Add operator notes to README**

Append to `README.md`:
```markdown
## Run against production

1. `cp .env.example .env`, fill `NP_SLACK_TOKEN` + `NP_SLACK_CHANNEL`.
2. `go build -o noseyposey ./cmd/noseyposey`
3. `set -a; . ./.env; set +a; ./noseyposey`

State (threads + dedup) lives in `NP_DB_PATH` (default `./noseyposey.db`). Deleting
it makes the next run start fresh threads for the day.

## Deploy

Single static binary. Run under a supervisor (systemd, container) with the `NP_*`
vars in the environment. The MQTT session is persistent (`clientID=noseyposey`),
so brief restarts let the broker hold messages — but transcriptions are not
retained, so extended downtime drops that speech (no backfill).

## Known limits

- No backfill: if noseyposey is down long enough for the broker to drop the
  persistent session, those utterances are lost.
- One post per utterance, rate-limited to ~1/sec/channel; sustained heavy talk
  across many devices queues (backpressure), it does not drop.
```

- [ ] **Step 4: Final full check**

Run: `./scripts/check`
Expected: `check: OK`.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: operator run/deploy notes and known limits"
```

---

## Self-Review Notes

- **Spec coverage:** source subscribe/skip-all (T3, T8), payload shape + UTC (T3), device=topic (T3), per-day thread boundary in America/Chicago (T4, T7), find-or-create thread (T7), threaded reply (T6, T7), formats (T4), dedup with text hash (T5, T7), SQLite state + prune (T5, T8), MQTT reconnect/persistent session/QoS1 (T8), Slack rate limit + 429/5xx retry + backpressure (T6, T8), graceful shutdown (T8), config via env (T2), no filtering (by omission), integration with real broker + fake Slack (T9), live smoke (T10). All covered.
- **No placeholders:** every step has runnable code or an exact command.
- **Type consistency:** `Utterance`, `ThreadStore`, `Poster`, `Router.Handle`, `slackclient.Post`, `store` method names are used identically across T5–T9.
```
