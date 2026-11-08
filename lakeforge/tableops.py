"""Core table operations: creation and idempotent, order-independent merge.

The central correctness property this module exists to provide: ingesting
the same batch twice, or ingesting batches out of their real-world order
(the normal case during a backfill), must never corrupt the table — a
later-arriving but logically-older update must never clobber a logically-
newer value that's already there, and re-ingesting an unchanged batch must
be a true no-op.
"""

import random
import time

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, TableNotFoundError

from lakeforge.schema import KEY_COL, ORDERS_SCHEMA, PARTITION_COL, VERSION_COL


def table_exists(path: str) -> bool:
    try:
        DeltaTable(path)
        return True
    except TableNotFoundError:
        return False


def create_table(path: str, schema: pa.Schema = ORDERS_SCHEMA, partition_by: str = PARTITION_COL) -> None:
    empty = pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)
    write_deltalake(path, empty, mode="error", partition_by=[partition_by])


def ensure_table(path: str, schema: pa.Schema = ORDERS_SCHEMA, partition_by: str = PARTITION_COL) -> None:
    if not table_exists(path):
        create_table(path, schema, partition_by)


def idempotent_merge(
    path: str,
    batch: pa.Table,
    key_col: str = KEY_COL,
    version_col: str = VERSION_COL,
    max_retries: int = 8,
) -> dict:
    """Upsert `batch` into the Delta table at `path`.

    For each key in `batch`:
      - not present in the table yet -> inserted.
      - present, and batch's version_col is STRICTLY GREATER than the
        stored row's -> updated to the incoming values.
      - present, and batch's version_col is less than or equal to the
        stored row's -> left untouched. This is what makes replaying an
        already-applied (or stale/out-of-order) batch safe: it can only
        ever move a row forward in version, never backward, and replaying
        an identical batch changes nothing at all.

    Retries on CommitFailedError: delta-rs uses optimistic concurrency
    control, so two writers racing to commit the same next table version
    will see exactly one succeed; the other must reload the table at its
    new version and retry the merge against it, which is exactly what the
    retry loop here does by re-constructing DeltaTable(path) on every
    attempt rather than reusing a stale handle.
    """
    if batch.num_rows == 0:
        return {"num_source_rows": 0, "num_target_rows_inserted": 0, "num_target_rows_updated": 0}

    cols = batch.schema.names
    updates = {c: f"source.{c}" for c in cols}

    last_err = None
    for attempt in range(max_retries):
        try:
            dt = DeltaTable(path)
            result = (
                dt.merge(
                    source=batch,
                    predicate=f"target.{key_col} = source.{key_col}",
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update(
                    updates=updates,
                    predicate=f"source.{version_col} > target.{version_col}",
                )
                .when_not_matched_insert(updates=updates)
                .execute()
            )
            return result
        except CommitFailedError as e:
            last_err = e
            if attempt == max_retries - 1:
                raise
            # Jittered exponential backoff: without jitter, two writers that
            # collided once would likely collide again on their very next
            # simultaneous retry.
            backoff = min(2**attempt, 8) * 0.05
            time.sleep(backoff * (0.5 + random.random()))
    raise last_err  # pragma: no cover - unreachable, loop always returns or raises
