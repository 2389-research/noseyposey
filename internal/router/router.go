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
