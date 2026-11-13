package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// TargetKind selects the poller for a target.
type TargetKind string

const (
	KindPrometheus TargetKind = "prometheus" // scrape a /metrics endpoint
	KindOllama     TargetKind = "ollama"     // poll /api/tags + /api/ps
	KindOB         TargetKind = "ob"         // poll OpenBrouter JSON admin API
)

// Target is one monitored endpoint.
type Target struct {
	Name   string            `yaml:"name"`
	Kind   TargetKind        `yaml:"kind"`
	URL    string            `yaml:"url"`
	Labels map[string]string `yaml:"labels"`

	// ViaSSH is an ssh host alias (or user@host). When set, the scraper opens
	// a background `ssh -N -L <localPort>:127.0.0.1:<remotePort>` tunnel and
	// rewrites URL's host:port to the local end. Use it when the metrics port
	// is only bound on the remote box (sparky's vLLM publishes nothing).
	ViaSSH string `yaml:"via_ssh"`

	APIKeyEnv   string   `yaml:"api_key_env"`  // env var holding the bearer key
	InsecureTLS bool     `yaml:"insecure_tls"` // trust self-signed certs
	Interval    Duration `yaml:"interval"`     // per-target override of scrape_interval
}

// Config is the whole YAML file.
type Config struct {
	Listen         string   `yaml:"listen"`
	ScrapeInterval Duration `yaml:"scrape_interval"`
	Retention      Duration `yaml:"retention"`
	SSHKey         string   `yaml:"ssh_key"`
	DB             string   `yaml:"db"` // SQLite journal path ("" = llmwatch.db)
	Targets        []Target `yaml:"targets"`
}

// Duration un/marshals Go duration strings ("15s", "1m").
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("bad duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// LoadConfig reads and validates the YAML config with sane defaults.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, err
	}
	if cfg.Listen == "" {
		cfg.Listen = "0.0.0.0:8799"
	}
	if cfg.ScrapeInterval == 0 {
		cfg.ScrapeInterval = Duration(15 * time.Second)
	}
	if cfg.Retention == 0 {
		cfg.Retention = Duration(24 * time.Hour)
	}
	if cfg.SSHKey == "" {
		cfg.SSHKey = "~/.ssh/id_ed25519"
	}
	if home := os.Getenv("HOME"); home != "" {
		cfg.SSHKey = strings.Replace(cfg.SSHKey, "~", home, 1)
	}
	seen := map[string]bool{}
	for i := range cfg.Targets {
		t := &cfg.Targets[i]
		if t.Name == "" || t.URL == "" {
			return nil, fmt.Errorf("target %d: name and url are required", i)
		}
		if seen[t.Name] {
			return nil, fmt.Errorf("duplicate target name %q", t.Name)
		}
		seen[t.Name] = true
		switch t.Kind {
		case KindPrometheus, KindOllama, KindOB:
		default:
			return nil, fmt.Errorf("target %q: unknown kind %q", t.Name, t.Kind)
		}
		if _, err := filepath.Abs(t.URL); err != nil {
			return nil, err
		}
	}
	if len(cfg.Targets) == 0 {
		return nil, fmt.Errorf("no targets configured")
	}
	if cfg.DB == "" {
		cfg.DB = "llmwatch.db"
	}
	if home := os.Getenv("HOME"); home != "" && strings.HasPrefix(cfg.DB, "~") {
		cfg.DB = strings.Replace(cfg.DB, "~", home, 1)
	}
	return cfg, nil
}
