// ABOUTME: Loads noseyposey configuration from NP_* environment variables.
// ABOUTME: Applies defaults, validates required fields, and resolves the timezone.
package config

import (
	"fmt"
	"os"
	"time"
)

// Config holds all runtime configuration.
type Config struct {
	MQTTBroker   string
	MQTTClientID string
	SlackToken   string
	SlackChannel string
	Timezone     string
	Location     *time.Location
	DBPath       string
	LogLevel     string
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Load reads configuration from the environment, applying defaults and
// validating required fields.
func Load() (Config, error) {
	cfg := Config{
		MQTTBroker:   env("NP_MQTT_BROKER", "tcp://192.168.23.123:1883"),
		MQTTClientID: env("NP_MQTT_CLIENT_ID", "noseyposey"),
		SlackToken:   os.Getenv("NP_SLACK_TOKEN"),
		SlackChannel: os.Getenv("NP_SLACK_CHANNEL"),
		Timezone:     env("NP_TIMEZONE", "America/Chicago"),
		DBPath:       env("NP_DB_PATH", "./noseyposey.db"),
		LogLevel:     env("NP_LOG_LEVEL", "info"),
	}
	if cfg.SlackToken == "" {
		return Config{}, fmt.Errorf("NP_SLACK_TOKEN is required")
	}
	if cfg.SlackChannel == "" {
		return Config{}, fmt.Errorf("NP_SLACK_CHANNEL is required")
	}
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return Config{}, fmt.Errorf("invalid NP_TIMEZONE %q: %w", cfg.Timezone, err)
	}
	cfg.Location = loc
	return cfg, nil
}
