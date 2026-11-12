package main

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// HandleMetrics re-exports everything llmwatch scraped, in Prometheus text
// format, so a real Prometheus (or promtool) could scrape us later.
func HandleMetrics(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		states := store.States()
		snap := store.Snapshot("")

		// up gauge per target first — the single most important series.
		fmt.Fprintln(w, "# HELP llmwatch_target_up Last scrape success per target.")
		fmt.Fprintln(w, "# TYPE llmwatch_target_up gauge")
		for _, st := range states {
			up := 0.0
			if st.Up {
				up = 1.0
			}
			labels := mergeLabels(map[string]string{"target": st.Name}, st.Labels)
			fmt.Fprintf(w, "llmwatch_target_up%s %g\n", renderLabels(labels), up)
		}
		fmt.Fprintln(w, "# HELP llmwatch_scrape_duration_seconds Scrape wall time.")
		fmt.Fprintln(w, "# TYPE llmwatch_scrape_duration_seconds gauge")
		for _, st := range states {
			labels := map[string]string{"target": st.Name}
			fmt.Fprintf(w, "llmwatch_scrape_duration_seconds%s %g\n",
				renderLabels(mergeLabels(labels, st.Labels)), float64(st.DurationMs)/1000.0)
		}

		// Everything else, grouped by target for readable HELP lines.
		targets := make([]string, 0, len(snap))
		for t := range snap {
			targets = append(targets, t)
		}
		sort.Strings(targets)
		emittedHelp := map[string]bool{}
		for _, tgt := range targets {
			for _, sv := range snap[tgt] {
				base := sanitize(sv.Name)
				if !emittedHelp[base] {
					fmt.Fprintf(w, "# TYPE %s gauge\n", base)
					emittedHelp[base] = true
				}
				labels := mergeLabels(map[string]string{"target": tgt}, sv.Labels)
				fmt.Fprintf(w, "%s%s %g %d\n", base, renderLabels(labels), sv.Last, sv.LastTS)
			}
		}
	}
}

func sanitize(name string) string {
	repl := func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == ':':
			return r
		}
		return '_'
	}
	return strings.Map(repl, name)
}

func mergeLabels(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		if _, ok := out[k]; !ok {
			out[k] = v
		}
	}
	return out
}

func renderLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", k, labels[k])
	}
	b.WriteByte('}')
	return b.String()
}
