package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/redis/rueidis"
)

// verify checks every acknowledged event is stored once by external ID with
// exactly one run, and that nothing was dead-lettered.
func verify(ctx context.Context, db *sql.DB, rc rueidis.Client, phase string, acked []string) (map[string]any, error) {
	like := phase + "-%"
	report := map[string]any{"events_acked": len(acked)}

	var rows, distinctIDs, distinctInternal int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*), count(DISTINCT event_id), count(DISTINCT internal_id) FROM events WHERE event_name = $1 AND event_id LIKE $2`,
		eventName, like,
	).Scan(&rows, &distinctIDs, &distinctInternal); err != nil {
		return nil, fmt.Errorf("counting events: %w", err)
	}
	report["event_rows"] = rows
	report["distinct_event_ids"] = distinctIDs
	report["distinct_internal_ids"] = distinctInternal

	stored := map[string]bool{}
	q, err := db.QueryContext(ctx, `SELECT DISTINCT event_id FROM events WHERE event_name = $1 AND event_id LIKE $2`, eventName, like)
	if err != nil {
		return nil, fmt.Errorf("listing events: %w", err)
	}
	for q.Next() {
		var id string
		if err := q.Scan(&id); err != nil {
			return nil, err
		}
		stored[id] = true
	}
	_ = q.Close()
	missing := 0
	for _, id := range acked {
		if !stored[id] {
			missing++
		}
	}
	report["acked_but_missing"] = missing

	var runs, withoutRun, duplicateRuns int
	if err := db.QueryRowContext(ctx, `
		WITH e AS (SELECT DISTINCT event_id, internal_id FROM events WHERE event_name = $1 AND event_id LIKE $2),
		     r AS (SELECT e.event_id, count(fr.run_id) AS n FROM e LEFT JOIN function_runs fr ON fr.event_id = e.internal_id GROUP BY e.event_id)
		SELECT coalesce(sum(n), 0), count(*) FILTER (WHERE n = 0), count(*) FILTER (WHERE n > 1) FROM r`,
		eventName, like,
	).Scan(&runs, &withoutRun, &duplicateRuns); err != nil {
		return nil, fmt.Errorf("counting runs: %w", err)
	}
	report["function_runs"] = runs
	report["events_without_run"] = withoutRun
	report["events_with_duplicate_runs"] = duplicateRuns

	dlq, err := rc.Do(ctx, rc.B().Xlen().Key(streamKey+":dlq").Build()).AsInt64()
	if err != nil {
		return nil, fmt.Errorf("reading dead-letter stream: %w", err)
	}
	report["dead_lettered"] = dlq

	report["ok"] = missing == 0 && distinctIDs == len(acked) && withoutRun == 0 && duplicateRuns == 0 && dlq == 0
	return report, nil
}
