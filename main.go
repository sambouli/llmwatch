// Package main implements llmwatch: a tiny pull-based monitor for the LLM
// stack (vLLM/sglang on the Spark, Ollama, OpenBrouter). It scrapes Prometheus
// endpoints where they exist, polls JSON admin APIs where they don't, keeps a
// short in-memory ring of samples, and serves a self-contained dashboard plus
// its own /metrics re-export over HTTP.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("llmwatch: config: %v", err)
	}

	store := NewStore(cfg.Retention)
	scheduler := NewScheduler(cfg, store)
	scheduler.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(dashboardBytes)
	})
	mux.HandleFunc("/api/state", scheduler.HandleState(store))
	mux.HandleFunc("/metrics", HandleMetrics(store))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("llmwatch listening on %s (%d targets, retention %s)", cfg.Listen, len(cfg.Targets), cfg.Retention)
	if err := srv.ListenAndServe(); err != nil {
		log.Println("llmwatch:", err)
		os.Exit(1)
	}
}
