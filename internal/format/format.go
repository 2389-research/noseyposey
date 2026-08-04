// ABOUTME: Formats timestamps and Slack message text in the configured timezone.
// ABOUTME: Parent threads are labeled per day; replies are prefixed with local HH:MM.
package format

import (
	"fmt"
	"time"
)

// LocalDate returns the calendar date (YYYY-MM-DD) of t in loc.
func LocalDate(t time.Time, loc *time.Location) string {
	return t.In(loc).Format("2006-01-02")
}

// ParentText is the thread-root label for a device on a given day.
func ParentText(device string, t time.Time, loc *time.Location) string {
	return fmt.Sprintf("🎙️ %s — %s", device, t.In(loc).Format("Mon Jan 2"))
}

// ReplyText is one threaded line: local HH:MM, two spaces, the utterance.
func ReplyText(t time.Time, loc *time.Location, text string) string {
	return fmt.Sprintf("%s  %s", t.In(loc).Format("15:04"), text)
}
