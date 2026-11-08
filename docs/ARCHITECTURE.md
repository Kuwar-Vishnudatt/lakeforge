# Architecture

## What problem this solves

A lakehouse pipeline fails in a specific, well-known way: a backfill or a
replay goes wrong. The 2026 Data Engineer hiring research pointed at this
directly — idempotent/replayable pipelines and schema contracts are now
expected, not differentiating, and the places candidates actually get
caught out are backfill correctness and schema evolution, not basic
ingestion. This project is a from-scratch, tested answer to: can you
replay a day of data twice without duplicating it, can you replay it
out of order without a stale value clobbering a newer one, can you evolve
the schema without an outage, and does any of this survive two jobs
genuinely writing to the table at the same time?

## Why Delta Lake via delta-rs, not Spark + a hosted catalog

Docker wasn't available in this environment, and a "real" Iceberg/Spark
lakehouse setup typically wants a JVM, a cluster (even single-node), and a
catalog service. `delta-rs` (the `deltalake` Python package used here) is a
Rust implementation of the Delta Lake protocol with no JVM and no cluster —
it reads and writes the actual Delta transaction log format
(`_delta_log/*.json`) and Parquet data files directly against local
storage, with real ACID transactions, real optimistic concurrency control,
and a real `MERGE` operation. This is the same engineering trade pgcdc made
for Postgres (a from-scratch WAL-protocol client instead of accepting a
Debezium+Kafka+ZooKeeper stack) and dqstream made for Kafka (Homebrew's
KRaft-mode broker instead of Docker): prefer real infrastructure over a
mock, but choose the lightest real implementation of that infrastructure
that still exercises the actual protocol and actual failure modes.

## The three properties that matter, and how each is achieved

### 1. Idempotent, order-independent merge (`lakeforge/tableops.py`)

`idempotent_merge` is a `MERGE INTO ... WHEN MATCHED AND source.updated_at >
target.updated_at THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`. The
strict-greater-than predicate is the entire mechanism: a row can only move
forward in version, never backward, which makes both of the dangerous
backfill scenarios safe automatically rather than requiring special-case
logic:

- **Replaying an already-applied batch** changes nothing (every row fails
  the `>` predicate against itself).
- **Replaying an OLD batch after a NEWER one has already landed** — the
  defining danger of backfills run out of real-world order — also changes
  nothing, because the predicate compares *versions*, not *arrival order*.

### 2. Metadata-only schema evolution (`lakeforge/schema_evolution.py`)

Adding a nullable column via `DeltaTable.alter.add_columns` is a pure
transaction-log metadata change: no existing Parquet file is touched,
proven directly in `test_add_column_is_metadata_only_no_rewrite` by
comparing the table's file set before and after. This matters operationally
because it means schema evolution never has an outage window — there's no
"the migration is 60% done and some files have the new column and some
don't" state to worry about; every file either predates the column (and
reads back as NULL for it) or postdates it.

### 3. Real concurrent-writer safety (`lakeforge/tableops.py`,
proven in `tests/test_concurrency.py`)

This is the one place a mocked backend would have been actively misleading.
`delta-rs` uses optimistic concurrency control: every writer reads the
table at some version N, stages its change, and tries to atomically create
version N+1's log entry; if two writers race, exactly one wins and the
loser gets `CommitFailedError`. `idempotent_merge`'s retry loop reloads the
table and retries on exactly that exception, with jittered exponential
backoff.

Proving this needed genuine concurrency, not a simulated one — so
`tests/test_concurrency.py` spawns real separate OS processes (via
`multiprocessing.Process`, not threads) that all wait on a `multiprocessing.
Barrier` and then hit the same table path at nearly the same instant.
Two tests make the claim honest rather than merely plausible:

- `test_concurrent_writers_with_retries_lose_no_data`: 6 processes × 20 rows
  each, writing concurrently with the real retry logic (max_retries=10) —
  asserts zero errors and every single row from every worker present.
- `test_concurrent_writers_without_retries_can_genuinely_conflict`: the same
  scenario with retries effectively disabled (max_retries=1) — asserts that
  across repeated attempts, a real `CommitFailedError` actually occurs. This
  test exists specifically so the first test's "zero errors" result means
  "the retry logic absorbed real conflicts," not "there was never anything
  to retry in the first place" — a passing test suite where the hard case
  never actually triggers is a well-known way to ship false confidence.
  Both held up across repeated manual runs in this environment; a
  concurrency-dependent test is maintained as an honestly-labeled
  best-effort reproduction, not a guaranteed-every-run assertion, which is
  why it's isolated from the main correctness test.

## Reconciliation: row count is not enough

`lakeforge.backfill.reconcile` checks both row count and the sum of
`amount` per partition. A row-count-only check can't distinguish "every row
is correct" from "the right number of rows exist but some have swapped or
corrupted values" —
`test_reconcile_catches_amount_sum_mismatch_even_with_correct_row_count`
exists specifically to keep this honest: it constructs a scenario where row
count is correct but the aggregate is deliberately wrong, and confirms
`reconcile` still catches it.

## Testing strategy

- `tests/test_idempotent_merge.py`: the core merge-predicate correctness —
  insert, update, stale-update-ignored, equal-version-ignored (strictly
  greater than, not greater-or-equal — two batches that happen to share a
  timestamp must not let one arbitrarily "win" over the other), and mixed
  insert+update in one batch.
- `tests/test_schema_evolution.py`: metadata-only add-column (file set
  provably unchanged), old rows reading back as NULL, a new batch
  populating the new column, and the trickier backward-compatibility case —
  an old-shaped backfill batch (predating the new column) still merging
  cleanly against the evolved schema.
- `tests/test_backfill.py`: the operational layer — `backfill_range`'s
  per-date version bookkeeping, `reconcile` catching both row-count and
  value-level corruption, idempotent re-running of a whole backfill, and a
  backfill for one date not disturbing another date's data.
- `tests/test_concurrency.py`: real multi-process concurrent writers, as
  described above — the one test class in this project that exercises an
  actual OS-level race condition rather than single-process logic.

## What's deliberately out of scope (v1)

- **A query engine (Spark/Trino/DuckDB) wired in as a first-class reader.**
  `DeltaTable.to_pyarrow_table()` is sufficient for this project's own
  verification needs; a real deployment would typically add a query engine
  on top, but that's a consumption-layer concern orthogonal to the
  pipeline-correctness problems this project is about.
- **A catalog service (Hive Metastore, AWS Glue, Unity Catalog).** The
  table is addressed by filesystem path throughout; swapping in a catalog
  is a path-resolution concern that doesn't change any of the merge,
  schema-evolution, or concurrency logic this project actually tests.
- **Automatic retry/orchestration scheduling** (Airflow, Dagster, etc.) —
  `backfill_range` is the orchestration primitive a real scheduler would
  call; building the scheduler itself is out of scope.
