// ABOUTME: noseyposey entrypoint — wires MQTT subscribe, worker, and Slack posting.
// ABOUTME: Bounded channel gives backpressure; SIGINT/SIGTERM shut it down cleanly.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

// parseLevel maps a string log level to slog.Level, defaulting to Info.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// slackOptsFromEnv reads optional Slack overrides. These are test/ops hooks,
// deliberately not part of the documented NP_* config: NP_SLACK_API_URL points
// the client at a fake server; NP_SLACK_MIN_INTERVAL_MS tunes (or disables) the
// inter-post delay.
func slackOptsFromEnv() []slackclient.Option {
	var opts []slackclient.Option
	if u := os.Getenv("NP_SLACK_API_URL"); u != "" {
		opts = append(opts, slackclient.WithAPIURL(u))
	}
	if v := os.Getenv("NP_SLACK_MIN_INTERVAL_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			opts = append(opts, slackclient.WithMinInterval(time.Duration(ms)*time.Millisecond))
		}
	}
	return opts
}

func main() {
	_ = godotenv.Load() // best-effort: load .env if present

	// Bootstrap logger for errors before we know the configured level.
	boot := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg, err := config.Load()
	if err != nil {
		boot.Error("config", "err", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)}))

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

	slack := slackclient.New(cfg.SlackToken, slackOptsFromEnv()...)
	rt := router.New(st, slack, cfg.SlackChannel, cfg.Location)

	// Bounded queue: MQTT handler → worker. Full channel = backpressure.
	utterances := make(chan transcript.Utterance, 256)

	// Worker: process utterances serially, exit on ctx cancellation.
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for {
			select {
			case u := <-utterances:
				if err := rt.Handle(ctx, u); err != nil {
					logger.Warn("handle utterance", "device", u.Device, "err", err)
				}
			case <-ctx.Done():
				return
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
		if _, skip := transcript.DeviceFromTopic(m.Topic()); skip {
			return
		}
		u, err := transcript.Parse(m.Topic(), m.Payload())
		if err != nil {
			// Drop-with-warning on any unparseable payload. Malformed JSON or missing
			// text has nothing to relay. The deferred case: the design spec wants a
			// receipt-time fallback when only the timestamp is bad — post under "now"
			// instead of dropping. That's post-v1 (Harper ruled ship-as-is); horton
			// timestamps have been well-formed in practice, and these warnings are the
			// cue to build it if they ever fire.
			logger.Warn("parse", "topic", m.Topic(), "err", err)
			return
		}
		select {
		case utterances <- u: // may block → backpressure
		case <-ctx.Done():
		}
	}

	// Persistent session: CleanSession(false) + a stable client ID makes the
	// broker hold our subscription and queue QoS-1 messages while we are
	// offline, then flood them on reconnect. Two options below handle the
	// consequences.
	//
	// SetDefaultPublishHandler closes a replay race. On (re)connect paho starts
	// its message router and opens the incoming pipe before it runs OnConnect —
	// which registers the per-topic route — in a separate goroutine that does
	// not block that pipe. A queued message arriving before OnConnect's
	// Subscribe reaches addRoute would hit an empty route table and be dropped
	// unacknowledged. Wiring the same handler as the default catches anything in
	// that window; once the route is live, paho routes to it and never also
	// calls the default, so no message posts twice. (We do not set ResumeSubs:
	// it governs only SUBSCRIBE packets issued while disconnected, and we always
	// subscribe from OnConnect while connected — it defaults to false anyway.)
	//
	// SetOrderMatters(true) preserves utterance order. With it false paho
	// dispatches each message on its own goroutine and the reconnect flood posts
	// out of sequence (TestPersistentSessionReconnect proves this). paho warns
	// that with order on, handlers must not block — but here a blocking handler
	// is the point. It only does a bounded channel send, and blocking the router
	// is exactly the backpressure we want under overload. The worst case, a
	// handler stalled past the keepalive interval, stops the network reader and
	// makes paho reconnect; with CleanSession(false) the broker then re-delivers
	// the queued messages in order, so backpressure degrades without loss.
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTTBroker).
		SetClientID(cfg.MQTTClientID).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetOrderMatters(true).
		SetDefaultPublishHandler(msgHandler).
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
	// NOTE: do not close(utterances). paho's Disconnect can return before its
	// message router stops, so a late msgHandler could still send; closing the
	// channel would risk a send-on-closed-channel panic. The worker instead exits
	// on ctx.Done() and the channel is never closed, so every send stays safe.
	<-workerDone
	return nil
}
