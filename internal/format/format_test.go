// ABOUTME: Tests for the format package — date keys and Slack message strings.
// ABOUTME: Anchored to a fixed UTC time to verify correct America/Chicago conversion.
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
