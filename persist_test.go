package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------- scraper fixtures (httptest) ----------

func TestScrapePrometheusFixture(t *testing.T) {
	body := `# HELP vllm:num_requests_running Number of requests currently running.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{engine="0",model_name="m1"} 3.0
# HELP vllm:request_success_total completed requests
# TYPE vllm:request_success_total counter
vllm:request_success_total{finished_reason="stop"} 42.0
# TYPE vllm:ttft_seconds summary
vllm:ttft_seconds{quantile="0.5"} 0.25
vllm:ttft_seconds_sum 100.0
vllm:ttft_seconds_count 400.0
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(body))
	}))
	defer srv.Close()

	s := NewScheduler(&Config{ScrapeInterval: Duration(time.Hour), SSHKey: "/dev/null"}, NewStore(Duration(time.Hour)))
	st := &TargetState{}
	samples, err := s.poll(Target{Name: "eng", Kind: KindPrometheus, URL: srv.URL + "/metrics"}, st)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"vllm:num_requests_running":       3,
		"vllm:request_success_total":      42,
		"vllm:ttft_seconds{quantile=0.5}": 0.25,
		"vllm:ttft_seconds_sum":           100,
		"vllm:ttft_seconds_count":         400,
	}
	got := map[string]float64{}
	for _, sm := range samples {
		got[sm.Name] = sm.Value
	}
	for name, v := range want {
		if g, ok := got[name]; !ok || g != v {
			t.Errorf("series %s: want %v got %v (have %d series)", name, v, g, len(got))
		}
	}
	// labels survive
	for _, sm := range samples {
		if sm.Name == "vllm:num_requests_running" && sm.Labels["model_name"] != "m1" {
			t.Errorf("labels lost: %v", sm.Labels)
		}
	}
}

func TestScrapeOllamaFixture(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/tags", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"models":[{"name":"d1:3b","size":123,"details":{"parameter_size":"3B","quantization_level":"Q4"}}]}`))
	})
	mux.HandleFunc("/api/ps", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"models":[{"name":"d1:3b","size":123,"vram":456789,"context_length":4096,"parallel":4}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	s := NewScheduler(&Config{ScrapeInterval: Duration(time.Hour)}, NewStore(Duration(time.Hour)))
	samples, err := s.poll(Target{Name: "oll", Kind: KindOllama, URL: srv.URL}, &TargetState{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, sm := range samples {
		got[sm.Name] = sm.Value
	}
	for _, name := range []string{"ollama_model_installed", "ollama_model_loaded", "ollama_model_vram_bytes", "ollama_model_context_length"} {
		if _, ok := got[name]; !ok {
			t.Errorf("missing %s in %v", name, got)
		}
	}
	if got["ollama_model_context_length"] != 4096 {
		t.Errorf("context length: %v", got["ollama_model_context_length"])
	}
}

func TestScrapeOBFixture(t *testing.T) {
	// Shapes copied from ob-live src/stats.ts StatsReport + server.ts /v1/spend.
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"ok":true}`)) })
	mux.HandleFunc("/v1/stats", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer sekret" {
			w.WriteHeader(401)
			w.Write([]byte(`{"error":{"message":"invalid or missing API key"}}`))
			return
		}
		w.Write([]byte(`{"window":"7d","requests":100,
			"totals":{"requests":100,"inputTokens":900,"outputTokens":100,"reasoningTokens":10,"cacheReadTokens":500,"cacheWriteTokens":0,"cacheHitRate":0.5,"costUsd":1.25,"errors":3},
			"latency":{"samples":50,"ttftP50":300,"ttftP95":900,"latencyP50":1500,"latencyP95":9000,"tokPerSecP50":40},
			"accuracy":{"samples":0,"meanBias":null,"calibration":null},
			"finish":{"truncated":2,"truncationRate":0.02,"reasons":{"length":2}},
			"models":[{"model":"m1","provider":"p1","requests":100,"costUsd":1.25,"errors":3}],
			"sessions":[],"oldestLogAt":null,"logCouldBeTruncated":false}`))
	})
	mux.HandleFunc("/v1/spend", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"providers":[{"provider":"p1","spent_usd":1.25,"exhausted":true}],"models":[{"model":"m1","spent_usd":1.25}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	s := NewScheduler(&Config{ScrapeInterval: Duration(time.Hour)}, NewStore(Duration(time.Hour)))
	t.Setenv("OB_TEST_KEY", "sekret")
	st := &TargetState{}
	samples, err := s.poll(Target{Name: "ob", Kind: KindOB, URL: srv.URL, APIKeyEnv: "OB_TEST_KEY"}, st)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, sm := range samples {
		got[sm.Name] = sm.Value
	}
	checks := map[string]float64{
		"ob_up":                 1,
		"ob_requests_total":     100,
		"ob_errors_total":       3,
		"ob_cost_usd_total":     1.25,
		"ob_cache_hit_rate":     0.5,
		"ob_ttft_p95_ms":        900,
		"ob_tok_per_sec_p50":    40,
		"ob_truncation_rate":    0.02,
		"ob_spend_usd_all_time": 1.25,
	}
	for name, v := range checks {
		if g, ok := got[name]; !ok || g != v {
			t.Errorf("%s: want %v got %v", name, v, g)
		}
	}
	var sawExhausted, sawModel bool
	for _, sm := range samples {
		if sm.Name == "ob_provider_budget_exhausted" {
			sawExhausted = true
		}
		if sm.Name == "ob_model_requests_total" && sm.Labels["model"] == "m1" {
			sawModel = true
		}
	}
	if !sawExhausted || !sawModel {
		t.Errorf("per-provider/per-model series missing (exhausted=%v model=%v)", sawExhausted, sawModel)
	}
}

func TestScrapeOBAuthFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte(`{"error":{"message":"invalid or missing API key"}}`))
	}))
	defer srv.Close()
	s := NewScheduler(&Config{ScrapeInterval: Duration(time.Hour)}, NewStore(Duration(time.Hour)))
	if _, err := s.poll(Target{Name: "ob", Kind: KindOB, URL: srv.URL, APIKeyEnv: "UNSET_KEY"}, &TargetState{}); err == nil {
		t.Error("401 on /health must surface as an error (target goes DOWN)")
	}
}

// ---------- persistence round-trip ----------

func TestPersistRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.db")
	retention := Duration(24 * time.Hour)
	now := time.Now().Truncate(time.Second)

	p, err := OpenPersister(path, retention.Duration())
	if err != nil {
		t.Fatal(err)
	}
	// two scrapes: gauge flat, counter climbing
	p.writeBatch(persistJob{target: "eng", samples: []Sample{
		{TS: now, Name: "gauge_a", Labels: map[string]string{"m": "x"}, Value: 5},
		{TS: now, Name: "c_total", Value: 10},
	}})
	p.writeBatch(persistJob{target: "eng", samples: []Sample{
		{TS: now.Add(15 * time.Second), Name: "gauge_a", Labels: map[string]string{"m": "x"}, Value: 5}, // dedup'd, ts advances
		{TS: now.Add(15 * time.Second), Name: "c_total", Value: 20},                                     // kept
	}})
	p.Close()

	// reopen and replay into a fresh store
	p2, err := OpenPersister(path, retention.Duration())
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	store := NewStore(retention)
	n, err := p2.Load(store, retention.Duration())
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("want 3 replayed samples (flat gauge collapsed), got %d", n)
	}
	snap := store.Snapshot("eng")
	for _, sv := range snap["eng"] {
		switch sv.Name {
		case "gauge_a":
			if len(sv.Values) != 1 || sv.Last != 5 || sv.Labels["m"] != "x" {
				t.Errorf("gauge replay wrong: %+v", sv)
			}
		case "c_total":
			if len(sv.Values) != 2 || sv.Values[1] != 20 {
				t.Errorf("counter staircase lost: %+v", sv)
			}
		}
	}
}

func TestLoadHistoricalKeepsUnreachedSeries(t *testing.T) {
	s := NewStore(Duration(time.Hour))
	now := time.Now()
	// A live scrape reports only 'a' — it marks everything else for dropping.
	s.AddSeries("eng", []Sample{{TS: now, Name: "a", Value: 1}})
	// Replay then delivers 'b'; it must survive despite not being in the last scrape.
	s.LoadHistorical("eng", []Sample{{TS: now.Add(-time.Minute), Name: "b", Value: 2}})
	snap := s.Snapshot("eng")
	names := map[string]bool{}
	for _, sv := range snap["eng"] {
		names[sv.Name] = true
	}
	if !names["a"] || !names["b"] {
		t.Fatalf("replayed series dropped: %v", names)
	}
}

func TestCanonicalLabelsRoundTrip(t *testing.T) {
	in := map[string]string{"z": "26", "a": "1", "model_name": "m-1"}
	s := canonicalLabels(in)
	if !strings.HasPrefix(s, "a=1,") {
		t.Errorf("not sorted: %s", s)
	}
	back := parseLabels(s)
	if fmt.Sprint(back) != fmt.Sprint(in) {
		t.Errorf("round trip lost data: %v -> %s -> %v", in, s, back)
	}
}
