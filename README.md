# lakeforge

A backfill-safe lakehouse ingestion pipeline, written in Python, running on
real [Delta Lake](https://delta.io/) tables through
[`delta-rs`](https://github.com/delta-io/delta-rs) (the `deltalake` Python
package). No Spark, no JVM, no Docker, no cluster — just a local directory
on disk that holds a real Delta table: a folder of Parquet data files plus a
`_delta_log/` of JSON commit records, read and written directly by a Rust
engine wrapped in a Python API. Despite the minimal setup, every guarantee
this project claims is a *real* one: real ACID transactions, real
[optimistic concurrency control](#how-it-works) against the on-disk log, and
a real `MERGE INTO` — not a mock, not an in-memory stand-in.

The pipeline ingests "order event" batches (one row per order, carrying its
latest known state) into a table, can replay historical batches to fix past
data (a **backfill** — re-processing data for a time period that's already
been ingested, usually because the source was wrong or a job failed), can
add new columns to that table while it's live, and can run a reconciliation
check to prove a backfill didn't quietly corrupt anything. All four of those
behaviors are exercised by 17 automated tests against a real local Delta
table — no fakes anywhere in the test suite.

## Why this exists

Ask anyone who has run a production lakehouse pipeline where things actually
go wrong, and the basic "read data, write data" path is rarely the answer.
The pain shows up in two specific places:

1. **Backfills.** The moment you need to re-run yesterday's job, or replay
   three months of corrected data from an upstream system, you've created
   an opportunity for two classic disasters: **double-counting** (the same
   batch gets applied twice because a job retried, or because you re-ran it
   by hand, and now every order shows up twice) and **stale-data-wins**
   (batches from a backfill don't arrive in the order the data actually
   happened in — that's normal, backfills are inherently out of order — and
   a batch of old, already-superseded data arrives *after* fresher data has
   already landed, overwriting the good values with garbage).
2. **Schema changes.** Adding a column to a live table traditionally means
   either a maintenance window (stop writes, migrate, resume) or living with
   the risk of a half-migrated table where some files have the new column
   and some don't, and nobody's quite sure which reader will see what.

Both problems are "solved" in principle by reaching for `MERGE INTO` and an
`ALTER TABLE ADD COLUMN`, and plenty of teams do exactly that and ship it.
This project exists to go one level deeper: to prove, with tests that
actually exercise the dangerous scenarios (an out-of-order replay, a bad
backfill, a schema change made mid-flight, two processes racing to write at
once) rather than just the happy path, that the specific implementation here
handles all of it correctly — and to explain, in plain language, *why* it
holds up.

## How it works

### Ingesting a new batch

A "batch" is a table of order rows — order ID, a status, an amount, the
date the order happened, and an `updated_at` timestamp saying when this
version of the row became true. Ingesting a batch means calling
`idempotent_merge(table_path, batch)`, which issues one Delta `MERGE INTO`
transaction:

- If an incoming row's order ID **isn't in the table yet**, it's inserted.
- If it **is already in the table**, the incoming row only overwrites the
  stored one when its `updated_at` is **strictly greater than** the stored
  row's `updated_at`. Equal or older timestamps change nothing.

That single predicate — strictly-newer-wins — is the entire idempotency
mechanism, and it's worth seeing why it quietly solves both backfill
disasters at once, with no special-case code for either:

- **Replaying a batch that's already been applied** is a true no-op: every
  row in the replay has the exact same `updated_at` as what's already
  stored, so `strictly greater than` is false for all of them, and nothing
  changes. Ingest the same batch five times, get the same table.
- **Replaying an old batch after a newer one has already landed** — the
  defining failure mode of backfills, since they process historical data
  that can arrive in any order relative to what else is happening — also
  changes nothing, for the same reason: the predicate compares the
  *version* (`updated_at`) each row carries, not the order in which batches
  happen to arrive. A batch from January that shows up in March, after a
  June correction has already been applied, loses to that June data
  automatically, because June's `updated_at` is later. There's no "oops,
  arrival order mattered" bug to write, because arrival order was never part
  of the logic.

One `MERGE` is a single real Delta **transaction**: it reads the table at
its current version, computes inserts/updates, and atomically commits the
next version's log entry — either the whole merge lands, or none of it
does.

### Making a schema change

Adding a column (say, a new `discount_pct` field) is done with
`add_nullable_column`, which calls Delta's `ALTER TABLE ADD COLUMN` under
the hood. This is a **metadata-only** operation: it writes one new entry to
the `_delta_log` recording that the schema now has an extra nullable column,
and touches *zero* existing Parquet data files. Every file written before
the change simply gets read back with `NULL` in that column for the rows it
contains; every file written after the change can populate it. This project
proves that claim rather than asserting it — the test literally lists every
data file in the table before the schema change and after, and asserts the
two sets are byte-for-byte identical.

Why this matters operationally: because no file is rewritten, there is no
window where the migration is "60% done." A table with a thousand files and
a schema change either has the new column in its schema or it doesn't —
there's no intermediate state where some readers see the column and others
don't depending on timing, because nothing about the existing data ever
moves.

### What a backfill looks like end to end

`backfill_range(table_path, batches_by_date)` takes a dictionary of
`{date: batch}` and, for each date (processed in date order purely for
readable audit logs — correctness doesn't depend on the order, as the
idempotent-merge section above explains), runs one `idempotent_merge` and
records the table's version number before and after. Those version numbers
are your audit trail: if something goes wrong later, you know exactly which
Delta commits belonged to this backfill, and you can use Delta's time-travel
(`DeltaTable(path).restore(version_before)`) to roll back to the pre-backfill
state if the backfill data itself turns out to have been wrong.

### Catching a bad backfill before anyone trusts it: reconciliation

`reconcile(table_path, expected_by_date)` compares the table's actual state,
per date partition, against numbers you already know should be true (from
the source system or an export manifest) — and it checks **two** things per
partition, not one:

- **Row count.**
- **`sum(amount)`** — the total dollar amount across every row in that
  partition.

Row count alone is a weak check: it only tells you the *number* of rows is
right, not that they're the *right* rows with the *right* values. A bug
that swapped two rows' amounts, or corrupted a value in place while leaving
the row count completely untouched, would sail straight through a
count-only reconciliation and nobody would ever know. Checking the sum of
`amount` as well means a value-level corruption that doesn't happen to
leave the sum unchanged (which, for any specific scrambling bug, is
overwhelmingly the likely case) gets caught immediately. It's not a
cryptographic guarantee — two compensating errors could theoretically cancel
out — but it closes the most common and realistic gap that a bare count
check leaves wide open, for the cost of one more aggregate.

## Architecture at a glance

```
lakeforge/
  schema.py            The canonical "orders" table schema: order_id (key),
                        updated_at (version), status, amount, event_date
                        (partition column).
  tableops.py           create_table (initializes an empty Delta table) and
                        idempotent_merge — the strictly-newer-wins MERGE,
                        plus the retry loop described below.
  schema_evolution.py   add_nullable_column (the metadata-only ALTER TABLE)
                        and current_columns (reads the live schema back).
  backfill.py           backfill_range (per-date merge + version bookkeeping)
                        and reconcile (row-count + amount-sum checking).
  synthetic.py          Generates realistic order batches for tests/demo —
                        no production data anywhere in this repo.
tests/
  test_idempotent_merge.py   Core merge-predicate correctness.
  test_schema_evolution.py   Metadata-only column add, proven via file diff.
  test_backfill.py           The operational layer: backfill_range + reconcile.
  test_concurrency.py        Real multi-process concurrent-writer races.
scripts/demo.py          A runnable, narrated end-to-end walkthrough.
docs/ARCHITECTURE.md     Full design rationale and trade-off writeup.
docs/BACKFILL_RUNBOOK.md Operational runbook for running a real backfill.
```

Each module has exactly one job, and `backfill.py` is deliberately a thin
orchestration layer on top of `tableops.idempotent_merge` rather than a
second place where merge logic could drift out of sync with the core
correctness guarantee.

## Proving concurrent writers are actually safe

`delta-rs` gives every writer **optimistic concurrency control**: a writer
reads the table at version N, stages its change, and tries to commit it as
version N+1. If two different writers both try to become version N+1 at the
same time, the storage layer lets exactly one of them win; the other gets a
`CommitFailedError` and has to reload the table (now at N+1) and retry its
change against that newer state. `idempotent_merge`'s retry loop does
exactly this: on `CommitFailedError` it re-opens the table fresh and retries
the merge, backing off with a bit of random jitter each time so that two
writers who collided once don't just collide again on their very next
simultaneous retry.

The standout test in this project (`tests/test_concurrency.py`) exists to
prove that loop actually works — and proves it the hard way. It doesn't use
threads, and it doesn't simulate anything: it uses Python's
`multiprocessing` module to spawn **six genuinely separate operating-system
processes**, each with its own Python interpreter and its own memory space.
All six are handed a `multiprocessing.Barrier`, a synchronization point that
blocks every process until all of them have reached it — so instead of
starting at slightly different times, all six are released to start writing
to the *same* Delta table at essentially the same instant. Each process
writes 20 of its own unique rows. With the retry loop enabled
(`max_retries=10`), the test asserts **zero errors across all six
processes** and that **every single one of the 120 rows** — not just the
right total count, but every individual worker's specific keys — ends up in
the table.

That "zero errors" result only means something if real conflicts actually
happened during the race. A test that reports zero errors because there was
never anything to conflict over in the first place would be worthless —
it would look identical to a test that proves nothing. So a **second,
companion test** exists specifically to rule that out: it runs the exact
same six-processes-racing setup, but with retries effectively switched off
(`max_retries=1`, no second attempt). Under that configuration, the test
asserts that a genuine `CommitFailedError` actually occurs — a real
exception, from a real lost commit race between real OS processes, not a
theoretical possibility. Seeing that failure under a tweaked configuration
is what makes the first test's "zero errors, zero data loss" result
trustworthy: it confirms there really was something to survive, and the
retry logic is what absorbed it, not that the race never materialized in
the first place.

Together, the two tests say something neither one could say alone: real
conflicts happen under real concurrency (proven by the second test), and the
production code path handles every single one of them without losing or
duplicating a row (proven by the first).

## Try it yourself

This is a plain Python project — no cluster, no external services, nothing
to spin up beforehand.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

make test   # runs all 17 tests against real local Delta tables
make demo   # runs scripts/demo.py: ingest, evolve schema, backfill, reconcile
```

`make test` runs `pytest` over everything in `tests/`; each test creates its
own throwaway Delta table under pytest's `tmp_path`, so nothing persists
between runs and nothing in this repo needs cleaning up afterward.
`make demo` walks through the whole lifecycle narrated with print
statements — ingest two days of live data, evolve the schema, backfill a
corrected day, reconcile it, and re-run that same backfill a second time to
show it changes nothing — and leaves the resulting table on disk for you to
poke at with `DeltaTable(path).to_pyarrow_table()` afterward.

**On the testing itself, honestly reported:** all 17 tests passed on the
first or near-first run. The only two things that needed fixing along the
way were trivial mistakes in the *test code itself* — a wrong method name,
and a wrong assumption about a library internal — not bugs in the pipeline
logic being tested. That isn't a boast about perfect code; it's a reflection
of how small and well-specified each correctness property is once you write
it down as "strictly greater than" or "same file set before and after."

## What this deliberately doesn't do (yet)

- **No query engine wired in as a first-class reader.** Reading the table
  back for verification uses `DeltaTable.to_pyarrow_table()` directly, which
  is enough for this project's own tests and demo. A real deployment would
  typically put Spark, Trino, or DuckDB on top to actually query the data,
  but that's a consumption-layer concern, separate from the pipeline
  correctness problems (idempotency, schema evolution, concurrency) this
  project is about.
- **No catalog service** (Hive Metastore, AWS Glue, Unity Catalog, etc.) —
  the table is addressed by a plain filesystem path throughout. Swapping in
  a catalog is a path-resolution detail that doesn't change any of the
  merge, schema-evolution, or concurrency logic this project tests.
- **No orchestration or scheduling layer** (Airflow, Dagster, cron, etc.) —
  `backfill_range` is the primitive a real scheduler would call; building
  the scheduler itself is out of scope here.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the fuller design
write-up and trade-off reasoning, and
[docs/BACKFILL_RUNBOOK.md](docs/BACKFILL_RUNBOOK.md) for the step-by-step
operational procedure for running a real backfill against this pipeline.

## Project layout

```
lakeforge/
  schema.py              canonical orders schema
  tableops.py             create_table, idempotent_merge (+ conflict retry)
  schema_evolution.py      add_nullable_column, current_columns
  backfill.py              backfill_range, reconcile
  synthetic.py             synthetic order-event batch generator
tests/
  test_idempotent_merge.py
  test_schema_evolution.py
  test_backfill.py
  test_concurrency.py      real multi-process concurrent-writer tests
scripts/demo.py            end-to-end runnable demo
docs/ARCHITECTURE.md       design rationale
docs/BACKFILL_RUNBOOK.md   operational backfill procedure
```

## License

MIT — see [LICENSE](LICENSE).

Repo: [github.com/Kuwar-Vishnudatt/lakeforge](https://github.com/Kuwar-Vishnudatt/lakeforge)
