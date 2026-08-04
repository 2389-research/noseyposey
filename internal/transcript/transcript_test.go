// ABOUTME: Tests for horton transcription message decoding.
// ABOUTME: Covers topic parsing, JSON decoding, and timestamp UTC handling.
package transcript

import (
	"testing"
	"time"
)

func TestDeviceFromTopic(t *testing.T) {
	cases := []struct {
		topic  string
		device string
		skip   bool
	}{
		{"horton/transcriptions/ivan-desk", "ivan-desk", false},
		{"horton/transcriptions/harper-desk", "harper-desk", false},
		{"horton/transcriptions/all", "all", true},
		{"horton/transcriptions/device-856308", "device-856308", false},
	}
	for _, c := range cases {
		dev, skip := DeviceFromTopic(c.topic)
		if dev != c.device || skip != c.skip {
			t.Errorf("DeviceFromTopic(%q) = (%q,%v), want (%q,%v)", c.topic, dev, skip, c.device, c.skip)
		}
	}
}

func TestParse(t *testing.T) {
	topic := "horton/transcriptions/ivan-desk"
	payload := []byte(`{"device":"ivan-desk","mac":"1C:DB:D4:85:65:7C","text":"Whoa.","timestamp":"2026-08-03T20:28:54.331766"}`)

	u, err := Parse(topic, payload)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if u.Device != "ivan-desk" {
		t.Errorf("Device = %q", u.Device)
	}
	if u.Mac != "1C:DB:D4:85:65:7C" {
		t.Errorf("Mac = %q", u.Mac)
	}
	if u.Text != "Whoa." {
		t.Errorf("Text = %q", u.Text)
	}
	want := time.Date(2026, 8, 3, 20, 28, 54, 331766000, time.UTC)
	if !u.Timestamp.Equal(want) {
		t.Errorf("Timestamp = %v, want %v", u.Timestamp, want)
	}
}

func TestParseBadJSON(t *testing.T) {
	if _, err := Parse("horton/transcriptions/x", []byte("not json")); err == nil {
		t.Fatal("expected error on bad JSON")
	}
}

func TestParseBadTimestamp(t *testing.T) {
	payload := []byte(`{"device":"ivan-desk","mac":"1C:DB:D4:85:65:7C","text":"Whoa.","timestamp":"not-a-time"}`)
	if _, err := Parse("horton/transcriptions/ivan-desk", payload); err == nil {
		t.Fatal("expected error on unparseable timestamp")
	}
}
