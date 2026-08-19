# SOLUTION

## What was broken, and why

**1. Recordings never marked processed (silent failure)**  
Recording work ran in a background goroutine using the HTTP request context. The handler returns 200 immediately, which cancels that context. `MarkRecordingProcessed` then failed, and the error was swallowed (`// TODO: handle`), so nothing appeared in logs.

**Fix:** Use `context.Background()` in the goroutine and log failures.

**2. In-flight work lost on deploy**  
On SIGTERM, `main` only called `srv.Shutdown()` (stop accepting HTTP). Background recording goroutines were not waited on, so the process could exit while work was still running.

**Fix:** Track in-flight recordings with a `sync.WaitGroup` and call `svc.Shutdown()` after HTTP shutdown to drain them before exit.

**3. Duplicate events and inflated call counts**  
Deduplication used `EventExists` followed by a separate `INSERT`. Under concurrent deliveries of the same `event_id`, both requests could pass the check and both insert — doubling rows in `events` and incrementing `account_stats` and the in-memory cache twice. The schema had an index on `event_id` but no unique constraint, so Postgres did not reject duplicates. Existing tests only covered sequential retries.

**Fix:** Add `UNIQUE (event_id)` on `events` (migration `002_unique_event_id.sql`). Change `InsertEvent` to `INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING event_id` and only run upsert/stats/cache/recording when a row was actually inserted. Keep `EventExists` as a fast path for obvious sequential duplicates.

## Deduplication strategy

**Chosen approach: Postgres unique constraint + `ON CONFLICT DO NOTHING`.**

The durable record of a delivery is already the `events` row. Making `event_id` unique and using an atomic insert means concurrent duplicates are resolved in one database operation — no check-then-act race.

**Why not Redis?** Redis `SET NX` would work for a fast “seen before?” check, but adds a second system that must stay consistent with Postgres. If Redis evicts a key or restarts empty, duplicates could slip through unless Postgres still enforces uniqueness. For this service’s scale, Postgres alone is simpler and sufficient.

**Why not `EventExists` alone?** It is useful as an early exit for sequential retries (avoids a write) but is not safe under concurrency by itself.

## At 10,000 webhooks/second

- **Redis as a first-line dedup cache** — most traffic is retries; filtering duplicates in memory before hitting Postgres would reduce write load.
- **Keep Postgres `UNIQUE` on `event_id`** as the source of truth regardless of cache.
- **Wrap ingest side effects in a transaction** so event, call, and stats stay consistent.
- **Move recording processing to a queue** (SQS, Redis streams, etc.) instead of unbounded goroutines per request; workers scale independently and survive deploys cleanly.
- **Batch stats updates** or derive aggregates from `events` periodically if per-row increments become a bottleneck.
- **Add a mutex to `stats.Cache.Record`** (today `Get` is locked but `Record` is not) and warm the cache from `account_stats` on startup so the stats endpoint is correct after deploy.
