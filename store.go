package main

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// Sample is one observation of one series at one instant.
type Sample struct {
	TS     time.Time         `json:"ts"`
	Name   string            `json:"name"` // metric name, target prefix stripped where useful
	Labels map[string]string `json:"labels,omitempty"`
	Value  float64           `json:"value"`
}

// TargetState is the last scrape result for a target.
type TargetState struct {
	Name       string            `json:"name"`
	Kind       TargetKind        `json:"kind"`
	URL        string            `json:"url"`
	Labels     map[string]string `json:"labels,omitempty"`
	Up         bool              `json:"up"`
	Error      string            `json:"error,omitempty"`
	ScrapedAt  time.Time         `json:"scraped_at"`
	DurationMs int64             `json:"duration_ms"`
	Series     int               `json:"series"`
	// Info holds non-numeric facts from the last poll (model names, versions).
	Info map[string]string `json:"info,omitempty"`
}

// Store keeps a bounded ring of samples per (target, series key) and the last
// state per target. Everything is in memory: retention hours, not history.
type Store struct {
	mu         sync.RWMutex
	retention  time.Duration
	series     map[string][]Sample // key: target|name|labels-canonical
	states     map[string]*TargetState
	introspect map[float64]int // bucket -> count of scrapes slower than bucket seconds
}

func NewStore(retention Duration) *Store {
	return &Store{
		retention: retention.Duration(),
		series:    map[string][]Sample{},
		states:    map[string]*TargetState{},
	}
}

func seriesKey(target, name string, labels map[string]string) string {
	if len(labels) == 0 {
		return target + "|" + name + "|"
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(target)
	b.WriteByte('|')
	b.WriteString(name)
	b.WriteByte('|')
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
		b.WriteByte(',')
	}
	return b.String()
}

// AddSeries replaces the whole set for one scrape of one target. Counter-style
// raw values are stored as-is; rate math happens at read time.
func (s *Store) AddSeries(target string, samples []Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addSeriesLocked(target, samples, false)
}

// LoadHistorical replays persisted samples at startup. Unlike a live scrape
// it must NOT drop series that the replay hasn't reached yet — those are
// still alive and will be re-reported by the first scrape.
func (s *Store) LoadHistorical(target string, samples []Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addSeriesLocked(target, samples, true)
}

func (s *Store) addSeriesLocked(target string, samples []Sample, loading bool) {
	cutoff := time.Now().Add(-s.retention)
	seen := map[string]bool{}
	for _, sm := range samples {
		key := seriesKey(target, sm.Name, sm.Labels)
		seen[key] = true
		arr := s.series[key]
		// Gauges are instantaneous observations: re-reporting the same value
		// every scrape adds no information and would grow unbounded over a
		// long retention window. Counters (_total/_count/_sum/quantiles) are
		// always kept — their per-second rate needs the full staircase.
		if len(arr) > 0 && !isCounter(sm.Name) && arr[len(arr)-1].Value == sm.Value {
			// Refresh the timestamp: the series is still alive, it just
			// hasn't moved. The dashboard shows "last seen", not "last change".
			arr[len(arr)-1].TS = sm.TS
			arr[len(arr)-1].Labels = sm.Labels
			continue
		}
		// Append if newer than the tail; drop expired head.
		if len(arr) > 0 && !sm.TS.After(arr[len(arr)-1].TS) {
			continue
		}
		arr = append(arr, sm)
		for len(arr) > 0 && arr[0].TS.Before(cutoff) {
			arr = arr[1:]
		}
		if len(arr) > 20000 { // hard cap per series
			arr = arr[len(arr)-20000:]
		}
		s.series[key] = arr
	}
	// Drop series that vanished from the last scrape (engine restarted with a
	// different label set) so stale lines don't haunt the dashboard. During a
	// startup replay this would delete everything not yet replayed, so skip.
	if !loading {
		prefix := target + "|"
		for k := range s.series {
			if strings.HasPrefix(k, prefix) && !seen[k] {
				delete(s.series, k)
			}
		}
	}
}

func (s *Store) SetState(st *TargetState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[st.Name] = st
}

func (s *Store) States() []*TargetState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*TargetState, 0, len(s.states))
	for _, st := range s.states {
		cp := *st
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// SeriesView is one series flattened for JSON.
type SeriesView struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	TS     []int64           `json:"ts"`
	Values []float64         `json:"values"`
	Last   float64           `json:"last"`
	LastTS int64             `json:"last_ts"`
	PerSec float64           `json:"per_sec,omitempty"` // counter rate over the window
}

// Snapshot returns every series for a target (or all targets when target==""),
// oldest→newest, plus a per-second rate for monotonically increasing counters.
func (s *Store) Snapshot(target string) map[string][]SeriesView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string][]SeriesView{}
	prefix := ""
	if target != "" {
		prefix = target + "|"
	}
	for key, arr := range s.series {
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		if len(arr) == 0 {
			continue
		}
		parts := strings.SplitN(key, "|", 3)
		tgt, name := parts[0], parts[1]
		view := SeriesView{Name: name, Last: arr[len(arr)-1].Value, LastTS: arr[len(arr)-1].TS.UnixMilli()}
		if len(arr[len(arr)-1].Labels) > 0 {
			view.Labels = arr[len(arr)-1].Labels
		}
		view.TS = make([]int64, len(arr))
		view.Values = make([]float64, len(arr))
		for i, sm := range arr {
			view.TS[i] = sm.TS.UnixMilli()
			view.Values[i] = sm.Value
		}
		// Rate across the window when it looks like a counter.
		if len(arr) >= 2 {
			dt := arr[len(arr)-1].TS.Sub(arr[0].TS).Seconds()
			dv := arr[len(arr)-1].Value - arr[0].Value
			if dt > 0 && dv >= 0 && isCounter(name) {
				view.PerSec = dv / dt
			}
		}
		out[tgt] = append(out[tgt], view)
	}
	for tgt := range out {
		sort.Slice(out[tgt], func(i, j int) bool {
			a, b := out[tgt][i], out[tgt][j]
			if a.Name != b.Name {
				return a.Name < b.Name
			}
			return seriesKey("", a.Name, a.Labels) < seriesKey("", b.Name, b.Labels)
		})
	}
	return out
}

// isCounter decides whether a per-second rate is meaningful for a name.
func isCounter(name string) bool {
	return strings.HasSuffix(name, "_total") || strings.HasSuffix(name, "_count") ||
		strings.HasSuffix(name, "_sum")
}
