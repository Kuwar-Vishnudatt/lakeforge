# Backfill Runbook

This is the operational procedure for re-processing historical data into the
orders table — a corrected upstream export, recovering from an outage, or
replaying a date range after a bug fix. It assumes the mechanics in
`lakeforge.tableops.idempotent_merge` and `lakeforge.backfill`; read
[ARCHITECTURE.md](ARCHITECTURE.md) first if you haven't, since this runbook
leans on the guarantees explained there rather than re-deriving them.

## Before you start

1. **Confirm the source data's `updated_at` values are correct and not just
   "now."** The idempotent-merge safety property depends entirely on
   `updated_at` genuinely representing when the row was last true upstream —
   if a backfill source stamps every row with the time the backfill *job*
   ran rather than the time the row was actually last updated, it will
   incorrectly win against legitimately newer data already in the table.
   This is the single most common way to defeat the safety this pipeline is
   built around; check it first.
2. **Know the expected row count and amount sum per date you're backfilling**,
   from the source system or export manifest, before you run anything. You
   need this for step 4 — reconciliation only catches a bad backfill if you
   gave it something real to compare against.
3. **Confirm you're backfilling into the current schema, or know you're
   not.** If the backfill source predates a schema change (see
   `test_backfill_batch_without_new_column_still_merges_cleanly`), that's
   fine and handled — the new column is simply left untouched for whatever
   rows this batch touches. But if the source SHOULD have the new column and
   doesn't, that's a data gap to notice, not something the merge will flag
   for you.

## Procedure

1. **Stage the batches.** Build one `pyarrow.Table` per `event_date` you're
   backfilling, matching the orders schema (or a subset of its columns —
   see `lakeforge.schema.ORDERS_SCHEMA`).

2. **Run the backfill.**
   ```python
   from lakeforge.backfill import backfill_range
   steps = backfill_range(table_path, batches_by_date)
   for s in steps:
       print(s.event_date, s.version_before, "->", s.version_after, s.merge_result)
   ```
   Keep the printed `version_before`/`version_after` pairs — they're your
   audit trail and the basis for a rollback (see below) if reconciliation
   fails.

3. **Reconcile immediately, before considering the backfill done.**
   ```python
   from lakeforge.backfill import reconcile
   mismatches = reconcile(table_path, expected_by_date)
   if mismatches:
       raise SystemExit("\n".join(str(m) for m in mismatches))
   ```
   A clean reconciliation checks BOTH row count and the sum of `amount` per
   date — row count alone can't catch a corruption that preserves count
   while corrupting values (see
   `test_reconcile_catches_amount_sum_mismatch_even_with_correct_row_count`).

4. **If reconciliation fails, don't re-run blindly.** Re-running the exact
   same backfill batch is always safe (idempotent by construction — see
   `test_replaying_a_backfill_date_twice_is_safe`), so a transient failure
   (a crashed job, a network blip mid-backfill) is fine to simply retry as-is.
   But a reconciliation mismatch usually means the SOURCE data or the
   expected-counts manifest was wrong, not that the merge logic
   misbehaved — check those before re-running with different data, since
   idempotent_merge has no way to "undo" a bad row once a later legitimate
   update supersedes it (by design: it would then look like just another
   stale write to ignore).

5. **If you genuinely need to roll back** (the backfill data itself was
   wrong and nothing has written a newer version since), use Delta's time
   travel directly rather than trying to "merge away" bad data:
   ```python
   from deltalake import DeltaTable
   dt = DeltaTable(table_path)
   dt.restore(version_before)  # the version you recorded in step 2
   ```
   `restore` is itself just another commit (it doesn't delete history), so
   it's safe to run even if other writers are active — it will conflict and
   retry exactly like any other commit if something else is writing at the
   same moment.

## Running a backfill concurrently with live ingestion

This is supported and tested (`tests/test_concurrency.py`,
`test_backfill_for_past_date_does_not_affect_other_dates`): a backfill job
for a past date and a live ingestion job for today's date can run against
the same table at the same time. Two things make this safe:

- **Different keys almost always mean no logical conflict** — a backfill for
  `2026-01-01` and live ingestion for `2026-01-15` touch disjoint rows, so
  there's nothing to reconcile between them beyond Delta's own commit
  mechanics.
- **Delta's optimistic concurrency control handles the commit race.** Both
  jobs may try to commit at the same table version; the loser gets
  `CommitFailedError`, reloads the table, and retries automatically
  (`idempotent_merge`'s retry loop) — no manual coordination needed. See
  ARCHITECTURE.md for why this is real, not merely "should work in theory."
