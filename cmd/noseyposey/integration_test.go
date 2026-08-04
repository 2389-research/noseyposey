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
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
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

// waitFor polls cond until it holds or the timeout elapses.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
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

	// run() reads these env hooks (see slackOptsFromEnv in main.go): point Slack
	// at the fake server and disable the inter-post delay so the test is fast and
	// deterministic.
	t.Setenv("NP_SLACK_API_URL", srv.URL+"/")
	t.Setenv("NP_SLACK_MIN_INTERVAL_MS", "0")

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
	publish(t, broker, "horton/transcriptions/all", ivan)       // mirror → must be ignored
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

// TestPersistentSessionReconnect proves a restarted daemon relays the
// transcriptions the broker queued while it was offline — in order, none dropped.
//
// With CleanSession(false) the broker holds QoS-1 messages for the offline client
// and floods them on reconnect. Two failure modes:
//
//   - Ordering: with OrderMatters=false paho dispatches each message on its own
//     goroutine, so the flood posts out of sequence. This reproduces reliably
//     here and is the regression this test was written to catch.
//   - Drop: if a flooded message reaches paho before OnConnect's Subscribe()
//     reaches addRoute, and no DefaultPublishHandler catches it, paho drops it
//     unacknowledged (router.go matchAndDispatch: "no handler was available.
//     Message will NOT be acknowledged"). The window is narrow — route
//     registration is in-memory with no network wait — so this rarely fires in
//     the test, but the source shows it is real; the default handler closes it.
func TestPersistentSessionReconnect(t *testing.T) {
	port := freePort(t)
	startMosquitto(t, port)
	broker := fmt.Sprintf("tcp://127.0.0.1:%d", port)

	slackSrv := &recordingSlack{}
	srv := httptest.NewServer(slackSrv.handler())
	defer srv.Close()

	t.Setenv("NP_SLACK_API_URL", srv.URL+"/")
	t.Setenv("NP_SLACK_MIN_INTERVAL_MS", "0")

	loc, _ := time.LoadLocation("America/Chicago")
	cfg := config.Config{
		MQTTBroker:   broker,
		MQTTClientID: "noseyposey-reconnect-itest", // fixed → same persistent session across runs
		SlackToken:   "xoxb-test",
		SlackChannel: "C123",
		Timezone:     "America/Chicago",
		Location:     loc,
		DBPath:       filepath.Join(t.TempDir(), "reconnect.db"),
		LogLevel:     "info",
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	runOnce := func() (context.CancelFunc, chan struct{}) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = run(ctx, cfg, logger); close(done) }()
		return cancel, done
	}

	// Phase 1: establish the persistent session. Warm up with one message and wait
	// for it to post (parent + reply = 2), proving the subscription is live, then
	// shut down cleanly so the broker retains the session and its subscription.
	// The sleep lets OnConnect's Subscribe register before we publish: on a fresh
	// session a message sent to a not-yet-subscribed topic has no subscriber and is
	// lost, not queued.
	cancel1, done1 := runOnce()
	time.Sleep(1500 * time.Millisecond)
	warm := `{"device":"ivan-desk","mac":"1C:DB:D4:85:65:7C","text":"warmup","timestamp":"2026-08-03T20:00:00.000000"}`
	publish(t, broker, "horton/transcriptions/ivan-desk", warm)
	waitFor(t, func() bool { return slackSrv.count() >= 2 }, 5*time.Second, "warmup to post")
	cancel1()
	<-done1

	// Phase 2: publish while the daemon is offline. The broker queues these QoS-1
	// messages for the retained session. Distinct ascending timestamps → no dedup.
	const m = 20
	for i := 0; i < m; i++ {
		payload := fmt.Sprintf(`{"device":"ivan-desk","mac":"1C:DB:D4:85:65:7C","text":"offline-%02d","timestamp":"2026-08-03T20:%02d:00.000000"}`, i, i+1)
		publish(t, broker, "horton/transcriptions/ivan-desk", payload)
	}

	// Phase 3: restart. The broker floods the queued messages on reconnect. Every
	// one must post (as a reply under the existing thread), and in publish order.
	before := slackSrv.count()
	cancel2, done2 := runOnce()
	waitFor(t, func() bool { return slackSrv.count() >= before+m }, 10*time.Second, "queued messages to post")
	cancel2()
	<-done2

	if got := slackSrv.count() - before; got != m {
		t.Fatalf("queued-while-offline delivery: got %d posts, want %d — messages dropped on reconnect", got, m)
	}
	var seq []string
	for _, p := range slackSrv.posts[before:] {
		if p["thread_ts"] != "" { // replies only
			seq = append(seq, p["text"])
		}
	}
	for i := 1; i < len(seq); i++ {
		if seq[i] < seq[i-1] {
			t.Errorf("out-of-order replies: %q before %q at index %d", seq[i-1], seq[i], i)
			break
		}
	}
}
