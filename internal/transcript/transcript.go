// ABOUTME: Decodes horton MQTT transcription messages into Utterance values.
// ABOUTME: Device name comes from the topic suffix; timestamps are parsed as UTC.
package transcript

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const topicPrefix = "horton/transcriptions/"

// Utterance is one decoded transcription message.
type Utterance struct {
	Device    string
	Mac       string
	Text      string
	Timestamp time.Time
}

// DeviceFromTopic returns the device name (topic suffix) and whether the
// message should be skipped (the "all" mirror duplicates every device stream).
func DeviceFromTopic(topic string) (device string, skip bool) {
	device = strings.TrimPrefix(topic, topicPrefix)
	if device == "all" {
		return device, true
	}
	return device, false
}

type wire struct {
	Device    string `json:"device"`
	Mac       string `json:"mac"`
	Text      string `json:"text"`
	Timestamp string `json:"timestamp"`
}

// timestampLayout matches "2026-08-03T20:28:54.331766" with no zone (UTC).
const timestampLayout = "2006-01-02T15:04:05.999999"

// Parse decodes a payload into an Utterance. The device is taken from the
// topic (authoritative); payload device is ignored except as documentation.
func Parse(topic string, payload []byte) (Utterance, error) {
	var w wire
	if err := json.Unmarshal(payload, &w); err != nil {
		return Utterance{}, fmt.Errorf("decode transcription: %w", err)
	}
	ts, err := time.ParseInLocation(timestampLayout, w.Timestamp, time.UTC)
	if err != nil {
		return Utterance{}, fmt.Errorf("parse timestamp %q: %w", w.Timestamp, err)
	}
	device, _ := DeviceFromTopic(topic)
	return Utterance{
		Device:    device,
		Mac:       w.Mac,
		Text:      w.Text,
		Timestamp: ts,
	}, nil
}
