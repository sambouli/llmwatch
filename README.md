# llmwatch

Tiny pull-based monitor for the personal LLM stack: vLLM/sglang engines on the
DGX Spark ("sparky"), Ollama, and [OpenBrouter](https://github.com/sambouli/openbrouter).
One Go binary, no database, no dependencies to run: it scrapes Prometheus
`/metrics` where they exist, polls JSON admin APIs where they don't, keeps a
24 h in-memory ring of samples, and serves a self-contained dashboard plus its
own Prometheus re-export.

## What it monitors

| Target | Source | How |
|---|---|---|
| vLLM / sglang engines | `GET /metrics` (Prometheus text) | direct, or over an auto-managed `ssh -L` tunnel when the port is only bound on the box (sparky's vLLM publishes nothing) |
| Ollama | `GET /api/tags`, `GET /api/ps` | ollama has no `/metrics`; we export installed/loaded models, VRAM, context length |
| OpenBrouter | `GET /health`, `GET /v1/stats`, `GET /v1/spend` | ob exposes no Prometheus surface; its JSON stats are re-exported as gauges (requests, errors, cost, TTFT/latency percentiles, tok/s, cache hit rate, per-model and per-provider spend, exhausted budgets) |

Adding another engine is one YAML stanza — see `config.example.yaml`.

## Endpoints served

- `/` — dashboard (dark single page, sparklines, live filter, 5 s refresh)
- `/api/state` — the dashboard's JSON feed
- `/metrics` — everything scraped, Prometheus text format (`llmwatch_target_up`, scrape durations, all series) so a real Prometheus could scrape *us* later
- `/healthz` — liveness

## Run

```sh
go build -o llmwatch .
export OB_API_KEY=***      # bearer key for the ob admin API
./llmwatch -config config.yaml
# → http://localhost:8799
```

`config.yaml` is local-only (gitignored); start from `config.example.yaml`.
Secrets are never read from the YAML — the config names an env var
(`api_key_env`) and the value comes from the environment.

As a user service (this host):

```sh
install -m 600 /dev/null llmwatch.env   # then add OB_API_KEY=***
systemctl --user enable --now llmwatch.service   # unit in deploy/
```

## Design notes

- **In-memory only.** Retention-bounded ring per series (default 24 h), hard
  cap per series; counters are stored raw and rates computed at read time.
  Restarting loses history — that is the deal for "tiny".
- **SSH tunnels are managed internally**: one `ssh -N -L …` per
  (host, remote port), started lazily, respawned if it dies. No sshfs, no
  hand-opened tunnels.
- **Series that vanish** between scrapes are dropped, so a restarted engine
  with a new label set doesn't leave ghosts on the dashboard.
- Histograms collapse to `_sum`/`_count` (enough for averages); full buckets
  would blow the memory budget for a toy.

## Tests

```sh
go test ./...
```
