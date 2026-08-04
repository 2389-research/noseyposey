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
