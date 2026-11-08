"""Real concurrent-writer tests using separate OS processes (not threads —
delta-rs's commit path releases the GIL during I/O, so even threads would
genuinely race at the OS level, but separate processes make the "two
independent ingestion jobs hit the same table at once" scenario undeniable
rather than an implementation detail of the Python interpreter).

delta-rs uses optimistic concurrency control: every writer reads the
table's current version, builds its commit, and atomically creates the
next version's log file — if two writers race to create the same version
number, exactly one wins and the other must see CommitFailedError, reload
the table, and retry. This module proves both halves of that: that the
conflict genuinely happens under real concurrency (not just in theory),
and that lakeforge.tableops.idempotent_merge's retry loop actually handles
it with zero data loss.
"""

import multiprocessing as mp
from datetime import date, datetime

from deltalake import DeltaTable

from lakeforge.synthetic import make_batch, order_ids
from lakeforge.tableops import create_table, idempotent_merge


def _writer_worker(path: str, prefix: str, n: int, event_date: date, start_barrier, max_retries: int, error_queue):
    """Runs in a separate process. Waits at the barrier so every worker
    process starts committing at nearly the same moment, maximizing the
    chance of a real version-commit collision."""
    batch = make_batch(order_ids(prefix, n), event_date, datetime(2026, 1, 1, 10, 0, 0), amount_fn=lambda _: 1.0)
    start_barrier.wait()
    try:
        idempotent_merge(path, batch, max_retries=max_retries)
    except Exception as e:  # noqa: BLE001 - reporting back to the parent process
        error_queue.put(f"{prefix}: {type(e).__name__}: {e}")


def run_concurrent_writers(table_path: str, num_workers: int, rows_per_worker: int, max_retries: int) -> list[str]:
    event_date = date(2026, 1, 1)
    barrier = mp.Barrier(num_workers)
    error_queue = mp.Queue()
    procs = [
        mp.Process(
            target=_writer_worker,
            args=(table_path, f"w{i}", rows_per_worker, event_date, barrier, max_retries, error_queue),
        )
        for i in range(num_workers)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=60)

    errors = []
    while not error_queue.empty():
        errors.append(error_queue.get())
    return errors


def test_concurrent_writers_with_retries_lose_no_data(tmp_path):
    table_path = str(tmp_path / "orders")
    create_table(table_path)

    num_workers = 6
    rows_per_worker = 20
    errors = run_concurrent_writers(table_path, num_workers, rows_per_worker, max_retries=10)

    assert errors == [], f"expected the retry loop to absorb every commit conflict, got: {errors}"

    df = DeltaTable(table_path).to_pyarrow_table().to_pandas()
    assert len(df) == num_workers * rows_per_worker, (
        f"expected {num_workers * rows_per_worker} rows from {num_workers} concurrent writers, got {len(df)} "
        "— this would indicate silent data loss from an unhandled commit conflict"
    )
    # Every worker's keys must all be present — not just the right total
    # count (which a bug that dropped one worker's writes while duplicating
    # another's could still satisfy by coincidence).
    for i in range(num_workers):
        prefix = f"w{i}"
        count = df["order_id"].str.startswith(f"{prefix}-").sum()
        assert count == rows_per_worker, f"worker {prefix} expected {rows_per_worker} rows, found {count}"


def test_concurrent_writers_without_retries_can_genuinely_conflict(tmp_path):
    """Proves the retry logic is load-bearing rather than decorative: with
    max_retries effectively disabled (1 attempt, no retry), real concurrent
    writers against the same table DO occasionally fail with a genuine
    CommitFailedError. This test only asserts that AT LEAST ONE run across
    several repetitions reproduces a real conflict — a flaky-by-nature
    assertion on a race condition, which is exactly why it's isolated here
    rather than relied on in the "no data loss" test above.
    """
    saw_a_conflict = False
    for attempt in range(5):
        table_path = str(tmp_path / f"orders_no_retry_{attempt}")
        create_table(table_path)
        errors = run_concurrent_writers(table_path, num_workers=8, rows_per_worker=15, max_retries=1)
        if errors:
            saw_a_conflict = True
            assert any("CommitFailedError" in e for e in errors), errors
            break

    assert saw_a_conflict, (
        "expected at least one of 5 attempts with max_retries=1 to reproduce a real "
        "CommitFailedError under genuine concurrent writers — if this never happens, "
        "either the concurrency isn't real or delta-rs's conflict detection isn't "
        "being exercised at all, and the retry logic in idempotent_merge would be "
        "untested dead code"
    )
