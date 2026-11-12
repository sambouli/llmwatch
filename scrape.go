package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Scheduler runs one poll loop per target.
type Scheduler struct {
	cfg     *Config
	store   *Store
	client  *http.Client
	tunnels map[string]*tunnel // ssh host+port -> tunnel
	stop    chan struct{}
	wg      sync.WaitGroup
}

func NewScheduler(cfg *Config, store *Store) *Scheduler {
	return &Scheduler{
		cfg:     cfg,
		store:   store,
		client:  newHTTPClient(),
		tunnels: map[string]*tunnel{},
		stop:    make(chan struct{}),
	}
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 12 * time.Second,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			TLSHandshakeTimeout: 5 * time.Second,
			MaxIdleConnsPerHost: 4,
			DisableKeepAlives:   false,
		},
	}
}

func (s *Scheduler) Start() {
	for _, t := range s.cfg.Targets {
		s.wg.Add(1)
		go s.loop(t)
	}
}

func (s *Scheduler) Stop() {
	close(s.stop)
	s.wg.Wait()
	for _, tn := range s.tunnels {
		tn.close()
	}
}

func (s *Scheduler) intervalFor(t Target) time.Duration {
	if t.Interval != 0 {
		return t.Interval.Duration()
	}
	return s.cfg.ScrapeInterval.Duration()
}

func (s *Scheduler) loop(t Target) {
	defer s.wg.Done()
	interval := s.intervalFor(t)
	s.scrape(t) // immediate first pass
	timer := time.NewTicker(interval)
	defer timer.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-timer.C:
			s.scrape(t)
		}
	}
}

func (s *Scheduler) scrape(t Target) {
	started := time.Now()
	st := &TargetState{Name: t.Name, Kind: t.Kind, URL: t.URL, Labels: t.Labels, ScrapedAt: started}
	var samples []Sample
	var err error

	switch t.Kind {
	case KindPrometheus:
		samples, err = s.scrapePrometheus(t, st)
	case KindOllama:
		samples, err = s.scrapeOllama(t, st)
	case KindOB:
		samples, err = s.scrapeOB(t, st)
	}

	st.DurationMs = time.Since(started).Milliseconds()
	st.Series = len(samples)
	if err != nil {
		st.Up = false
		st.Error = truncate(err.Error(), 300)
		log.Printf("scrape %s: %v", t.Name, err)
	} else {
		st.Up = true
		s.store.AddSeries(t.Name, samples)
	}
	s.store.SetState(st)
}

// ---------- prometheus ----------

func (s *Scheduler) scrapePrometheus(t Target, st *TargetState) ([]Sample, error) {
	target := t.URL
	if t.ViaSSH != "" {
		host, port, err := hostPortFromURL(t.URL)
		if err != nil {
			return nil, err
		}
		local, err := s.ensureTunnel(t.ViaSSH, host, port)
		if err != nil {
			return nil, fmt.Errorf("ssh tunnel: %w", err)
		}
		u, _ := url.Parse(t.URL)
		u.Host = local
		target = u.String()
		st.URL = target + " (via ssh " + t.ViaSSH + ")"
	}
	body, err := s.fetch(target, t, "text/plain;version=0.0.4")
	if err != nil {
		return nil, err
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("parse metrics: %w", err)
	}
	now := time.Now()
	var out []Sample
	for name, fam := range families {
		for _, m := range fam.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			add := func(v float64, suffix string) {
				cp := map[string]string{}
				for k, v2 := range labels {
					cp[k] = v2
				}
				out = append(out, Sample{TS: now, Name: name + suffix, Labels: cp, Value: v})
			}
			switch fam.GetType() {
			case dto.MetricType_UNTYPED, dto.MetricType_GAUGE, dto.MetricType_COUNTER:
				if m.GetUntyped() != nil || fam.GetType() != dto.MetricType_UNTYPED {
					add(valueOf(m), "")
				}
			case dto.MetricType_SUMMARY:
				sm := m.GetSummary()
				add(sm.GetSampleSum(), "_sum")
				add(float64(sm.GetSampleCount()), "_count")
				for _, q := range sm.GetQuantile() {
					add(q.GetValue(), fmt.Sprintf("{quantile=%g}", q.GetQuantile()))
				}
			case dto.MetricType_HISTOGRAM:
				h := m.GetHistogram()
				add(h.GetSampleSum(), "_sum")
				add(float64(h.GetSampleCount()), "_count")
				// Buckets are kept only as cumulative sum/count — the dashboard
				// uses _sum/_count for averages; full buckets would blow memory.
			}
		}
	}
	return out, nil
}

func valueOf(m *dto.Metric) float64 {
	if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	}
	if m.GetGauge() != nil {
		return m.GetGauge().GetValue()
	}
	if m.GetUntyped() != nil {
		return m.GetUntyped().GetValue()
	}
	return 0
}

// ---------- ollama ----------

// Ollama has no /metrics on 0.40.x; /api/tags lists installed models and
// /api/ps lists loaded ones with VRAM usage. We export those as gauges.
func (s *Scheduler) scrapeOllama(t Target, st *TargetState) ([]Sample, error) {
	base := strings.TrimRight(t.URL, "/")
	now := time.Now()
	var out []Sample

	var tags struct {
		Models []struct {
			Name    string `json:"name"`
			Size    int64  `json:"size"`
			Details struct {
				ParameterSize string `json:"parameter_size"`
				Quantization  string `json:"quantization_level"`
			} `json:"details"`
		} `json:"models"`
	}
	if err := s.getJSON(base+"/api/tags", t, &tags); err != nil {
		return nil, err
	}
	st.Info = map[string]string{"installed": strconv.Itoa(len(tags.Models))}
	for _, m := range tags.Models {
		out = append(out, Sample{TS: now, Name: "ollama_model_installed",
			Labels: map[string]string{"model": m.Name}, Value: 1})
		out = append(out, Sample{TS: now, Name: "ollama_model_size_bytes",
			Labels: map[string]string{"model": m.Name}, Value: float64(m.Size)})
	}

	var ps struct {
		Models []struct {
			Name          string  `json:"name"`
			Size          int64   `json:"size"`
			VRAM          float64 `json:"vram"`
			ExpiresAt     string  `json:"expires_at"`
			Processor     string  `json:"processor"`
			Par           int     `json:"parallel"`
			ContextLength int     `json:"context_length"`
		} `json:"models"`
	}
	if err := s.getJSON(base+"/api/ps", t, &ps); err != nil {
		// /api/ps failing shouldn't lose /api/tags data.
		log.Printf("scrape %s: /api/ps: %v", t.Name, err)
	} else {
		loaded := []string{}
		for _, m := range ps.Models {
			out = append(out, Sample{TS: now, Name: "ollama_model_loaded",
				Labels: map[string]string{"model": m.Name}, Value: 1})
			out = append(out, Sample{TS: now, Name: "ollama_model_vram_bytes",
				Labels: map[string]string{"model": m.Name}, Value: m.VRAM})
			out = append(out, Sample{TS: now, Name: "ollama_model_context_length",
				Labels: map[string]string{"model": m.Name}, Value: float64(m.ContextLength)})
			loaded = append(loaded, m.Name)
		}
		st.Info["loaded"] = strings.Join(loaded, ", ")
	}
	return out, nil
}

// ---------- openbrouter ----------

// ob exposes no Prometheus surface; poll its JSON admin endpoints and re-export.
func (s *Scheduler) scrapeOB(t Target, st *TargetState) ([]Sample, error) {
	base := strings.TrimRight(t.URL, "/")
	now := time.Now()
	var out []Sample

	var health map[string]any
	if err := s.getJSON(base+"/health", t, &health); err != nil {
		return nil, err
	}
	out = append(out, Sample{TS: now, Name: "ob_up", Value: 1})
	st.Info = map[string]string{}
	if v, ok := health["ok"].(bool); ok && v {
		st.Info["status"] = "ok"
	}

	var stats struct {
		Requests int `json:"requests"`
		Totals   struct {
			Requests        int      `json:"requests"`
			Errors          int      `json:"errors"`
			CostUSD         float64  `json:"costUsd"`
			InputTokens     float64  `json:"inputTokens"`
			OutputTokens    float64  `json:"outputTokens"`
			ReasoningTokens float64  `json:"reasoningTokens"`
			CacheReadTokens float64  `json:"cacheReadTokens"`
			CacheHitRate    *float64 `json:"cacheHitRate"`
		} `json:"totals"`
		Latency struct {
			TTFTP50      *float64 `json:"ttftP50"`
			TTFTP95      *float64 `json:"ttftP95"`
			LatencyP50   *float64 `json:"latencyP50"`
			LatencyP95   *float64 `json:"latencyP95"`
			TokPerSecP50 *float64 `json:"tokPerSecP50"`
		} `json:"latency"`
		Finish struct {
			Truncated      int      `json:"truncated"`
			TruncationRate *float64 `json:"truncationRate"`
		} `json:"finish"`
		Models []struct {
			Model    string  `json:"model"`
			Provider string  `json:"provider"`
			Requests int     `json:"requests"`
			CostUSD  float64 `json:"costUsd"`
			Errors   int     `json:"errors"`
		} `json:"models"`
	}
	if err := s.getJSON(base+"/v1/stats", t, &stats); err != nil {
		// Health said we're up; stats is best-effort but worth surfacing.
		return out, fmt.Errorf("/v1/stats: %w", err)
	}
	g := func(name string, v *float64) {
		if v != nil {
			out = append(out, Sample{TS: now, Name: name, Value: *v})
		}
	}
	g("ob_requests_total", ptr(float64(stats.Totals.Requests)))
	g("ob_errors_total", ptr(float64(stats.Totals.Errors)))
	g("ob_cost_usd_total", ptr(stats.Totals.CostUSD))
	g("ob_input_tokens_total", ptr(stats.Totals.InputTokens))
	g("ob_output_tokens_total", ptr(stats.Totals.OutputTokens))
	g("ob_reasoning_tokens_total", ptr(stats.Totals.ReasoningTokens))
	g("ob_cache_read_tokens_total", ptr(stats.Totals.CacheReadTokens))
	g("ob_cache_hit_rate", stats.Totals.CacheHitRate)
	g("ob_ttft_p50_ms", stats.Latency.TTFTP50)
	g("ob_ttft_p95_ms", stats.Latency.TTFTP95)
	g("ob_latency_p50_ms", stats.Latency.LatencyP50)
	g("ob_latency_p95_ms", stats.Latency.LatencyP95)
	g("ob_tok_per_sec_p50", stats.Latency.TokPerSecP50)
	g("ob_truncated_total", ptr(float64(stats.Finish.Truncated)))
	g("ob_truncation_rate", stats.Finish.TruncationRate)
	for _, m := range stats.Models {
		lbl := map[string]string{"model": m.Model, "provider": m.Provider}
		out = append(out,
			Sample{TS: now, Name: "ob_model_requests_total", Labels: lbl, Value: float64(m.Requests)},
			Sample{TS: now, Name: "ob_model_cost_usd_total", Labels: lbl, Value: m.CostUSD},
			Sample{TS: now, Name: "ob_model_errors_total", Labels: lbl, Value: float64(m.Errors)})
	}

	var spend struct {
		Providers []struct {
			Provider  string  `json:"provider"`
			SpentUSD  float64 `json:"spent_usd"`
			Exhausted bool    `json:"exhausted"`
		} `json:"providers"`
		Models []struct {
			Model    string  `json:"model"`
			SpentUSD float64 `json:"spent_usd"`
		} `json:"models"`
	}
	if err := s.getJSON(base+"/v1/spend", t, &spend); err == nil {
		total := 0.0
		for _, p := range spend.Providers {
			out = append(out, Sample{TS: now, Name: "ob_provider_spend_usd",
				Labels: map[string]string{"provider": p.Provider}, Value: p.SpentUSD})
			total += p.SpentUSD
			if p.Exhausted {
				out = append(out, Sample{TS: now, Name: "ob_provider_budget_exhausted",
					Labels: map[string]string{"provider": p.Provider}, Value: 1})
			}
		}
		out = append(out, Sample{TS: now, Name: "ob_spend_usd_all_time", Value: total})
		for _, m := range spend.Models {
			out = append(out, Sample{TS: now, Name: "ob_model_spend_usd_all_time",
				Labels: map[string]string{"model": m.Model}, Value: m.SpentUSD})
		}
	}
	return out, nil
}

func ptr(f float64) *float64 { return &f }

// ---------- http plumbing ----------

func (s *Scheduler) apiKey(t Target) string {
	if t.APIKeyEnv == "" {
		return ""
	}
	return os.Getenv(t.APIKeyEnv)
}

func (s *Scheduler) fetch(rawurl string, t Target, accept string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, rawurl, nil)
	if err != nil {
		return nil, err
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if k := s.apiKey(t); k != "" {
		req.Header.Set("Authorization", "Bearer "+k)
	}
	client := s.client
	if t.InsecureTLS {
		client = &http.Client{
			Timeout: s.client.Timeout,
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
				TLSHandshakeTimeout: 5 * time.Second,
				TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			},
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var rdr io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		rdr = gz
	}
	body, err := io.ReadAll(io.LimitReader(rdr, 32<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d: %s", rawurl, resp.StatusCode, truncate(string(body), 200))
	}
	return body, nil
}

func (s *Scheduler) getJSON(rawurl string, t Target, v any) error {
	body, err := s.fetch(rawurl, t, "application/json")
	if err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}

// ---------- ssh tunnels ----------

type tunnel struct {
	cmd    *exec.Cmd
	stderr *bytes.Buffer
	port   int
}

// ensureTunnel keeps one `ssh -N -L local:remoteHost:remotePort alias` alive
// per (alias, remoteHost, remotePort) and returns "127.0.0.1:local".
func (s *Scheduler) ensureTunnel(alias, remoteHost string, remotePort int) (string, error) {
	key := fmt.Sprintf("%s|%s|%d", alias, remoteHost, remotePort)
	scx, ok := s.tunnels[key]
	if ok && scx.cmd.Process != nil && scx.cmd.ProcessState == nil {
		return fmt.Sprintf("127.0.0.1:%d", scx.port), nil
	}
	port, err := freePort()
	if err != nil {
		return "", err
	}
	args := []string{
		"-N", "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
		"-o", "ServerAliveInterval=30", "-o", "StrictHostKeyChecking=accept-new",
		"-i", s.cfg.SSHKey,
		"-L", fmt.Sprintf("127.0.0.1:%d:%s:%d", port, remoteHost, remotePort),
		alias,
	}
	cmd := exec.Command("ssh", args...)
	errBuf := &bytes.Buffer{}
	cmd.Stderr = errBuf
	if err := cmd.Start(); err != nil {
		return "", err
	}
	tn := &tunnel{cmd: cmd, stderr: errBuf, port: port}
	s.tunnels[key] = tn
	go func() { _ = cmd.Wait() }()
	// Give the forward a moment to bind.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			c.Close()
			return fmt.Sprintf("127.0.0.1:%d", port), nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	tn.close()
	delete(s.tunnels, key)
	return "", fmt.Errorf("forward never bound: %s", truncate(errBuf.String(), 200))
}

func (tn *tunnel) close() {
	if tn.cmd.Process != nil {
		_ = tn.cmd.Process.Kill()
	}
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func hostPortFromURL(raw string) (string, int, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", 0, err
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		return u.Host, defaultPort(u.Scheme), nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}

func defaultPort(scheme string) int {
	if scheme == "https" {
		return 443
	}
	return 80
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
