# lakeforge

A backfill-safe lakehouse ingestion pipeline on real Delta Lake tables
(`delta-rs` / the `deltalake` Python package — no Spark, no JVM, no Docker):
idempotent order-independent merge, online schema evolution with zero
rewrite, and reconciliation that catches more than a row-count check — all
proven against a real local Delta table, including a test that spawns
genuine concurrent OS processes to prove the optimistic-concurrency retry
logic actually does something.

```
historical/live event batches ──▶ idempotent_merge (MERGE INTO,
                                    strictly-newer-wins predicate)
                                           │
                              Delta table (_delta_log + Parquet,
                              real ACID commits, real conflict detection)
                                           │
                         reconcile() ──▶ row count + amount sum per partition
```

## Why this project

The research behind this portfolio's Data Engineer projects kept pointing
at the same thing: idempotent/replayable pipelines and schema contracts are
now expected table-stakes for this role, and the real differentiator is
whether a candidate can reason about what actually goes wrong in a backfill
— double-counted replays, stale-data-wins-over-fresh-data races, a schema
migration that needs a maintenance window. This project builds and tests
each of those failure modes directly rather than just wiring up a "MERGE
INTO" and calling it done.

## Quickstart

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

make test        # 17 tests, pure local Delta tables, no external services
make demo         # end-to-end: ingest, evolve schema, backfill, reconcile
```

## What's actually tested, concretely

- **Idempotent merge**: replaying an identical batch is a true no-op; a
  batch with an OLDER `updated_at` arriving AFTER a newer one has already
  landed is correctly ignored (the core backfill-safety property); a batch
  mixing brand-new keys and existing keys does the right thing for each.
- **Schema evolution**: adding a column is proven metadata-only (the data
  file set is compared before/after and asserted identical); an old-shaped
  backfill batch that predates a schema change still merges cleanly into
  the evolved table.
- **Backfill + reconciliation**: `backfill_range` processes a date range
  with per-step version bookkeeping; `reconcile` checks row count AND
  `sum(amount)` per partition specifically because row count alone can't
  catch a corruption that preserves count while scrambling values.
- **Real concurrent writers**: `tests/test_concurrency.py` spawns separate
  OS processes that race to commit to the same table at the same instant,
  proving delta-rs's optimistic concurrency conflicts are real (a companion
  test with retries disabled reproduces an actual `CommitFailedError`) and
  that `idempotent_merge`'s retry loop absorbs them with zero data loss.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design
rationale, and [docs/BACKFILL_RUNBOOK.md](docs/BACKFILL_RUNBOOK.md) for the
operational procedure this project is built to support.

## Project layout

```
lakeforge/
  schema.py            canonical orders schema
  tableops.py           create_table, idempotent_merge (+ conflict retry)
  schema_evolution.py    add_nullable_column, current_columns
  backfill.py            backfill_range, reconcile
  synthetic.py           synthetic order-event batch generator
tests/
  test_idempotent_merge.py
  test_schema_evolution.py
  test_backfill.py
  test_concurrency.py    real multi-process concurrent-writer tests
scripts/demo.py          end-to-end runnable demo
docs/ARCHITECTURE.md      design rationale
docs/BACKFILL_RUNBOOK.md  operational backfill procedure
```

## What's deliberately out of scope (v1)

See docs/ARCHITECTURE.md — no query engine wired in as a first-class
reader, no catalog service, no scheduler/orchestration layer. All are
consumption- or deployment-layer concerns orthogonal to the pipeline
correctness problems (idempotency, schema evolution, concurrency) this
project is actually about.
