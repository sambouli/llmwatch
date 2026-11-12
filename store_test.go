package main

import (
	"math"
	"os"
	"testing"
	"time"
)

func TestStoreAddAndSnapshot(t *testing.T) {
	s := NewStore(Duration(time.Hour))
	now := time.Now()
	s.AddSeries("eng", []Sample{
		{TS: now, Name: "vllm:num_requests_running", Labels: map[string]string{"model": "m1"}, Value: 3},
		{TS: now, Name: "vllm:request_success_total", Labels: map[string]string{"reason": "stop"}, Value: 100},
	})
	snap := s.Snapshot("eng")
	if len(snap["eng"]) != 2 {
		t.Fatalf("want 2 series, got %d", len(snap["eng"]))
	}
	for _, sv := range snap["eng"] {
		if sv.Last == 0 {
			t.Errorf("series %s last value should be set", sv.Name)
		}
	}
}

func TestCounterRate(t *testing.T) {
	s := NewStore(Duration(time.Hour))
	base := time.Now().Add(-time.Minute)
	s.AddSeries("eng", []Sample{{TS: base, Name: "x_total", Value: 10}})
	s.AddSeries("eng", []Sample{{TS: base.Add(time.Minute), Name: "x_total", Value: 70}})
	snap := s.Snapshot("eng")
	if len(snap["eng"]) != 1 {
		t.Fatalf("want 1 series got %d", len(snap["eng"]))
	}
	if rate := snap["eng"][0].PerSec; math.Abs(rate-1.0) > 0.01 {
		t.Errorf("want ~1/s, got %v", rate)
	}
}

func TestStaleSeriesDropped(t *testing.T) {
	s := NewStore(Duration(time.Hour))
	now := time.Now()
	s.AddSeries("eng", []Sample{{TS: now, Name: "gone", Value: 1}})
	s.AddSeries("eng", []Sample{{TS: now.Add(time.Second), Name: "kept", Value: 2}})
	snap := s.Snapshot("eng")
	for _, sv := range snap["eng"] {
		if sv.Name == "gone" {
			t.Fatal("series that disappeared from the scrape should be dropped")
		}
	}
	if len(snap["eng"]) != 1 {
		t.Fatalf("want 1, got %d", len(snap["eng"]))
	}
}

func TestRetentionPrunes(t *testing.T) {
	s := NewStore(Duration(50 * time.Millisecond))
	old := time.Now().Add(-time.Hour)
	s.AddSeries("eng", []Sample{{TS: old, Name: "a_total", Value: 1}})
	s.AddSeries("eng", []Sample{{TS: time.Now(), Name: "a_total", Value: 2}})
	snap := s.Snapshot("eng")
	if len(snap["eng"]) != 1 || snap["eng"][0].Last != 2 || len(snap["eng"][0].Values) != 1 {
		t.Fatalf("expired sample not pruned: %+v", snap["eng"])
	}
}

func TestRenderLabels(t *testing.T) {
	got := renderLabels(map[string]string{"b": "2", "a": `x"y`})
	want := `{a="x\"y",b="2"}`
	if got != want {
		t.Errorf("got %s want %s", got, want)
	}
}

func TestSanitize(t *testing.T) {
	if got := sanitize("vllm:num_requests{running}"); got != "vllm:num_requests_running_" {
		t.Errorf("got %q", got)
	}
}

func TestConfigValidation(t *testing.T) {
	dir := t.TempDir()
	p := dir + "/c.yaml"

	os.WriteFile(p, []byte("listen: 127.0.0.1:1\ntargets:\n  - name: a\n    kind: prometheus\n    url: http://x/metrics\n"), 0o600)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Retention.Duration() != 24*time.Hour {
		t.Errorf("default retention wrong: %v", cfg.Retention)
	}

	os.WriteFile(p, []byte("targets:\n  - name: a\n    kind: bogus\n    url: http://x\n"), 0o600)
	if _, err := LoadConfig(p); err == nil {
		t.Error("unknown kind should fail")
	}

	os.WriteFile(p, []byte("targets:\n  - name: a\n    kind: prometheus\n    url: http://x\n  - name: a\n    kind: ollama\n    url: http://y\n"), 0o600)
	if _, err := LoadConfig(p); err == nil {
		t.Error("duplicate target name should fail")
	}
}
