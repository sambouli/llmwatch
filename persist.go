package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Disk persistence for the sample ring. The in-memory Store stays the read
// path (dashboard latency); the journal is a write-behind copy so a restart —
// crash, upgrade, or `systemctl restart` — replays history instead of wiping
// it. One SQLite file, single writer goroutine, WAL mode.
//
// Compaction mirrors the memory policy: unchanged gauge points collapse into
// one row whose ts advances (still an honest "last seen"); counters keep every
// staircase point because rate math needs them.

type persistJob struct {
	target  string
	samples []Sample
}

// Persister owns the samples.journal database and its writer goroutine.
type Persister struct {
	db      *sql.DB
	jobs    chan persistJob
	wg      sync.WaitGroup
	closed  chan struct{}
	closeOk sync.Once
}

const schema = `
CREATE TABLE IF NOT EXISTS samples (
  id       INTEGER PRIMARY KEY,
  target   TEXT NOT NULL,
  name     TEXT NOT NULL,
  labels   TEXT NOT NULL DEFAULT '',
  ts_ms    INTEGER NOT NULL,
  value    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_samples_series ON samples(target, name, labels, ts_ms);
CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples(ts_ms);
`

// OpenPersister opens (creating if needed) the journal and starts the writer.
func OpenPersister(path string, retention time.Duration) (*Persister, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// modernc driver serializes anyway; one writer keeps fsync cheap.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	p := &Persister{db: db, jobs: make(chan persistJob, 256), closed: make(chan struct{})}
	p.wg.Add(1)
	go p.writer(retention)
	return p, nil
}

// Enqueue copies the scrape result to the writer goroutine. Non-blocking by
// design: if the journal falls behind we drop journal entries (memory still
// has them) rather than slow scrapes.
func (p *Persister) Enqueue(target string, samples []Sample) {
	cp := make([]Sample, len(samples))
	copy(cp, samples)
	select {
	case p.jobs <- persistJob{target: target, samples: cp}:
	case <-p.closed:
	default:
		log.Printf("persist: journal queue full, dropping %d samples for %s", len(cp), target)
	}
}

func (p *Persister) writer(retention time.Duration) {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case job := <-p.jobs:
			if err := p.writeBatch(job); err != nil {
				log.Printf("persist: %v", err)
			}
		case <-ticker.C:
			if _, err := p.db.Exec("DELETE FROM samples WHERE ts_ms < ?",
				time.Now().Add(-retention).UnixMilli()); err != nil {
				log.Printf("persist: prune: %v", err)
			}
		case <-p.closed:
			// drain what's queued, then exit
			for {
				select {
				case job := <-p.jobs:
					if err := p.writeBatch(job); err != nil {
						log.Printf("persist: %v", err)
					}
				default:
					return
				}
			}
		}
	}
}

// Close stops the writer (draining the queue) and closes the database. Safe
// to call twice.
func (p *Persister) Close() {
	p.closeOk.Do(func() { close(p.closed) })
	p.wg.Wait()
	p.db.Close()
}

// writeBatch appends samples with tail-compaction for gauges (see header).
func (p *Persister) writeBatch(job persistJob) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	type tailRow struct {
		id    int64
		value float64
	}
	tails := map[string]tailRow{}
	getTail := func(name, labels string) (int64, float64, error) {
		key := name + "|" + labels
		if v, ok := tails[key]; ok {
			return v.id, v.value, nil
		}
		var id int64
		var val float64
		err := tx.QueryRow(`SELECT id, value FROM samples
			WHERE target=? AND name=? AND labels=? ORDER BY ts_ms DESC LIMIT 1`,
			job.target, name, labels).Scan(&id, &val)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, nil
		}
		if err != nil {
			return 0, 0, err
		}
		tails[key] = tailRow{id, val}
		return id, val, nil
	}

	ins, err := tx.Prepare(`INSERT INTO samples (target, name, labels, ts_ms, value) VALUES (?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer ins.Close()
	upd, err := tx.Prepare(`UPDATE samples SET ts_ms=? WHERE id=?`)
	if err != nil {
		return err
	}
	defer upd.Close()

	for _, sm := range job.samples {
		labels := canonicalLabels(sm.Labels)
		if !isCounter(sm.Name) {
			id, last, err := getTail(sm.Name, labels)
			if err != nil {
				return err
			}
			if id != 0 && last == sm.Value {
				if _, err := upd.Exec(sm.TS.UnixMilli(), id); err != nil {
					return err
				}
				continue
			}
		}
		if _, err := ins.Exec(job.target, sm.Name, labels, sm.TS.UnixMilli(), sm.Value); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Load warms the in-memory store from the journal (samples within retention,
// oldest first). Called once at startup before scrapers start.
func (p *Persister) Load(store *Store, retention time.Duration) (int, error) {
	cutoff := time.Now().Add(-retention).UnixMilli()
	rows, err := p.db.Query(`SELECT target, name, labels, ts_ms, value FROM samples
		WHERE ts_ms >= ? ORDER BY target, name, labels, ts_ms`, cutoff)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	n := 0
	var cur []Sample
	var curTarget string
	flush := func() {
		if len(cur) > 0 {
			store.LoadHistorical(curTarget, cur)
			n += len(cur)
		}
		cur = nil
	}
	for rows.Next() {
		var target, name, labels string
		var tsMs int64
		var value float64
		if err := rows.Scan(&target, &name, &labels, &tsMs, &value); err != nil {
			return n, err
		}
		if target != curTarget {
			flush()
			curTarget = target
		}
		cur = append(cur, Sample{TS: time.UnixMilli(tsMs), Name: name, Labels: parseLabels(labels), Value: value})
	}
	flush()
	if err := rows.Err(); err != nil {
		return n, err
	}
	return n, nil
}

// canonicalLabels renders labels in the same sorted k=v order that
// seriesKey uses, so replayed rows land in the same series as live ones.
func canonicalLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+labels[k])
	}
	return strings.Join(parts, ",")
}

func parseLabels(s string) map[string]string {
	if s == "" {
		return nil
	}
	out := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[k] = v
		}
	}
	return out
}

var _ = fmt.Sprintf
