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
