package config

import (
	"testing"
)

func TestLoadDefaultsAndRequired(t *testing.T) {
	t.Setenv("NP_SLACK_TOKEN", "xoxb-abc")
	t.Setenv("NP_SLACK_CHANNEL", "C123")
	// leave the rest unset to exercise defaults

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MQTTBroker != "tcp://192.168.23.123:1883" {
		t.Errorf("MQTTBroker default = %q", cfg.MQTTBroker)
	}
	if cfg.MQTTClientID != "noseyposey" {
		t.Errorf("MQTTClientID default = %q", cfg.MQTTClientID)
	}
	if cfg.Timezone != "America/Chicago" {
		t.Errorf("Timezone default = %q", cfg.Timezone)
	}
	if cfg.Location == nil || cfg.Location.String() != "America/Chicago" {
		t.Errorf("Location = %v", cfg.Location)
	}
	if cfg.DBPath != "./noseyposey.db" {
		t.Errorf("DBPath default = %q", cfg.DBPath)
	}
}

func TestLoadMissingTokenFails(t *testing.T) {
	t.Setenv("NP_SLACK_TOKEN", "")
	t.Setenv("NP_SLACK_CHANNEL", "C123")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when NP_SLACK_TOKEN missing")
	}
}

func TestLoadBadTimezoneFails(t *testing.T) {
	t.Setenv("NP_SLACK_TOKEN", "xoxb-abc")
	t.Setenv("NP_SLACK_CHANNEL", "C123")
	t.Setenv("NP_TIMEZONE", "Mars/Phobos")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for invalid timezone")
	}
}
