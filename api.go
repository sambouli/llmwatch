package main

import (
	"encoding/json"
	"net/http"
)

// HandleState serves the dashboard's single JSON feed: last scrape state per
// target plus every series' history.
func (s *Scheduler) HandleState(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		resp := struct {
			Targets []*TargetState          `json:"targets"`
			Series  map[string][]SeriesView `json:"series"`
		}{Targets: store.States(), Series: store.Snapshot("")}
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(resp)
	}
}
