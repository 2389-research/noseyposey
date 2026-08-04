# noseyposey — MQTT → Slack transcript bridge

**Status:** approved — Go confirmed; Slack pipe (token + channel `C0A3JTFTB5M` + threading) verified live 2026-08-04
**Date:** 2026-08-04

## Purpose

A small, always-on daemon that relays the horton voice-transcription feed from MQTT into Slack. Every mic ("device") gets **one Slack thread per day**; each transcribed utterance is posted as a threaded reply under that day's parent message. This keeps the channel to one parent per device per day while every word lands in a readable running log.

## Source (verified live on 2026-08-03)

- **Broker:** `192.168.23.123:1883` — Mosquitto 2.1.2, no auth.
- **Topic:** `horton/transcriptions/<device>` — one stream per mic. Also mirrored to `horton/transcriptions/all` (identical payload); the mirror is **skipped** to avoid double-delivery.
- **Payload:**
  ```json
  {"device":"ivan-desk","mac":"1C:DB:D4:85:65:7C","text":"Whoa.","timestamp":"2026-08-03T20:28:54.331766"}
  ```
  - `device` (str), `mac` (str), `text` (utterance), `timestamp` (ISO-8601, microseconds, **UTC**).
  - **Not retained** — live only, one message per finalized utterance. Utterances are chatty and fragmentary.
- **Devices seen:** horton, harper-desk, ivan-desk, sugi-desk, desk-intern, kitchen, sofa, hacking-area, plus `device-<id>` for unnamed mics. New devices appear dynamically and must be handled without config changes.
- Related but out of scope: `horton/summaries/<device>/<window>` (retained digests). Not used by v1.

## Behavior

For each incoming utterance:

1. Subscribe to `horton/transcriptions/+`; ignore the literal `all` topic. `device` = topic suffix (cross-checked against payload `device`).
2. Compute the **local date** in `America/Chicago` from the message timestamp (or receipt time as fallback if timestamp is unparseable).
3. Look up `(device, date)` in SQLite.
   - **Missing:** post the parent message, store its `thread_ts`.
   - **Present:** reuse the stored `thread_ts`.
4. Post the utterance as a threaded reply.
5. Record the utterance as posted (dedup key `device` + `timestamp` + short hash of `text`), so a restart or MQTT redelivery never double-posts, while two distinct utterances that happen to share a timestamp both still post.

**Formats (configurable, these are defaults):**
- Parent: `🎙️ <device> — Mon Jan 2`
- Reply: `15:28  <text>`  (local `HH:MM`, two spaces, text)

No content filtering in v1 — every utterance is posted, including one-word noise. A filter toggle is a future option, not built now (YAGNI).

## Slack integration

- **Bot token** (`xoxb-…`) with `chat:write`, calling `chat.postMessage`. Incoming webhooks are ruled out — they cannot thread.
- Single target **channel** (id or name), bot invited as a member.
- Token and channel supplied via environment/secret, never committed. Harper creates the app (steps in Appendix A).

## State — SQLite (single file)

- `threads(device TEXT, date TEXT, channel TEXT, thread_ts TEXT, created_at, UNIQUE(device, date))` — find/resume today's thread per device across restarts.
- `posted(device TEXT, ts_key TEXT, posted_at, UNIQUE(device, ts_key))` — dedup, where `ts_key = "<timestamp>|<sha1(text)[:8]>"`; pruned after ~2 days.

Rationale: one file, no server, atomic, trivially restart-safe. `ONE SOURCE OF TRUTH` — the thread mapping lives only here.

## Robustness (the core requirement)

- **MQTT:** auto-reconnect with exponential backoff, resubscribe on reconnect, QoS 1. Fixed client ID + persistent session (`clean_session=false`) so brief bridge restarts let the broker hold queued messages instead of dropping them (best-effort — depends on publisher QoS).
- **Slack:** a bounded outbound queue feeding a single sender that respects Slack's ~1 msg/sec/channel guidance, honors `429 Retry-After` exactly, and retries `5xx` with backoff. Bursts are absorbed by the queue. If the queue approaches saturation, apply **backpressure** by pausing MQTT consumption (let the broker's session hold messages) rather than dropping.
- **Restart-safe:** SQLite resumes threads and dedups posts.
- **Graceful shutdown:** drain the queue, close connections cleanly.
- **Observability:** structured logging (`slog`); log the exact utterance count posted/queued/retried.

**Known limitation (not hidden):** transcriptions are not retained. If noseyposey is fully down, those utterances are gone — no backfill from the source. The persistent session covers only brief blips.

## Configuration (env, with flag overrides)

| Var | Default | Notes |
|-----|---------|-------|
| `NP_MQTT_BROKER` | `tcp://192.168.23.123:1883` | |
| `NP_MQTT_CLIENT_ID` | `noseyposey` | stable, for persistent session |
| `NP_SLACK_TOKEN` | — (required, secret) | `xoxb-…` |
| `NP_SLACK_CHANNEL` | — (required) | channel id or name |
| `NP_TIMEZONE` | `America/Chicago` | daily boundary + timestamps |
| `NP_DB_PATH` | `./noseyposey.db` | SQLite file |
| `NP_LOG_LEVEL` | `info` | |

## Tech stack

- **Go.** Single static binary, mature MQTT (`eclipse/paho.mqtt.golang`) and Slack (`slack-go/slack`) libraries, strong fit for a forever-running daemon. (Alternative considered: Python, to match the horton stack — rejected unless Harper prefers stack consistency.)
- **SQLite** via a CGo-free driver (`modernc.org/sqlite`) to keep the static-binary property.

## Testing (real dependencies, per house rules)

- **Unit:** date-boundary/timezone logic, dedup keys, message formatting, topic→device parsing, config parsing.
- **Integration:** a real Mosquitto (container) publishing sample utterances → a **fake Slack HTTP server** capturing posts. Assert: parent posted once per device/day, replies carry the right `thread_ts`, dedup holds across a simulated restart, `429`/`5xx` handling retries correctly.
- **Manual e2e smoke:** post to the real Slack channel once wired.
- **`scripts/check`:** `go vet`, `golangci-lint`, `go test ./...` — the canonical check.

## Project layout

```
cmd/noseyposey/main.go        # wiring, signals, config load
internal/config/              # env/flag parsing
internal/mqttsource/          # subscribe, reconnect, decode
internal/store/               # SQLite: threads + dedup
internal/slacksink/           # queue, rate limit, ret/backoff, postMessage
internal/router/              # utterance → find/create thread → reply
scripts/check                 # canonical build+lint+test
AGENTS.md                     # project conventions + agent/human names
README.md
```

## Deploy (noted, not built in v1)

Single binary under systemd or a container; secrets via env. Deployment automation is out of scope for the first cut.

## Settled decisions

1. **Go** — confirmed.
2. **Backpressure-not-drop** on queue saturation.
3. **No utterance filtering** in v1.
4. Slack pipe verified end to end (parent + threaded reply + delete) against channel `C0A3JTFTB5M`.

---

## Appendix A — Slack app setup (Harper)

1. **Create the app:** <https://api.slack.com/apps> → *Create New App* → *From scratch*. Name it `noseyposey`, pick the workspace.
2. **Add scope:** *OAuth & Permissions* → *Scopes* → *Bot Token Scopes* → add **`chat:write`**.
3. **Install:** *Install to Workspace* → *Allow*. Copy the **Bot User OAuth Token** (`xoxb-…`).
4. **Channel:** create or choose the target channel. In that channel, run `/invite @noseyposey` — the bot must be a member to post.
5. **Channel id:** open the channel → *View channel details* → copy the id (`C…`) at the bottom (or just use the channel name).
6. **Hand off:** give me the `xoxb-…` token and the channel id/name. The token goes in a gitignored `.env`/secret and is never committed.
