# noseyposey

Relays the horton voice-transcription MQTT feed into Slack: one thread per device
per day, each utterance a threaded reply.

- Design: `docs/superpowers/specs/2026-08-04-noseyposey-design.md`
- Plan: `docs/superpowers/plans/2026-08-04-noseyposey.md`

## Quick start
```
cp .env.example .env      # fill NP_SLACK_TOKEN + NP_SLACK_CHANNEL
go run ./cmd/noseyposey    # auto-loads .env from the working directory
```

## Check
```
./scripts/check
```

## Run against production

1. `cp .env.example .env`, fill `NP_SLACK_TOKEN` + `NP_SLACK_CHANNEL`.
2. `go build -o noseyposey ./cmd/noseyposey`
3. `./noseyposey` — it auto-loads `.env` from the working directory.

State (threads + dedup) lives in `NP_DB_PATH` (default `./noseyposey.db`). Deleting
it makes the next run start fresh threads for the day.

## Deploy

Single static binary. Run under a supervisor (systemd, container) with the `NP_*`
vars in the environment, or a `.env` beside the binary. The MQTT session is
persistent (`clientID=noseyposey`), so brief restarts let the broker hold
messages — but transcriptions are not retained, so extended downtime drops that
speech (no backfill).

## Known limits

- No backfill: if noseyposey is down long enough for the broker to drop the
  persistent session, those utterances are lost.
- One post per utterance, rate-limited to ~1/sec/channel; sustained heavy talk
  across many devices queues (backpressure), it does not drop.
- A prolonged Slack outage fills that queue and stalls the MQTT read loop, so
  the client reconnects on its keepalive timer. Nothing is lost — the backlog
  posts once Slack recovers — but expect reconnect churn in the logs meanwhile.
