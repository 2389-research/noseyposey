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
