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
