"""Backfill orchestration and reconciliation.

A backfill here means re-ingesting one or more historical event_date
partitions' worth of data — e.g. a corrected export from an upstream system,
or recovering from a day the pipeline was down. The two things that make a
backfill dangerous in practice: (1) double-counting if replay isn't
idempotent, and (2) silently clobbering a newer value with a stale one if
replay isn't order-independent. Both are handled by `tableops.idempotent_merge`;
this module adds the operational layer around it — running a backfill as a
sequence of per-date merges with before/after version bookkeeping, and
reconciling the result against an expected shape.
"""

from dataclasses import dataclass
from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable

from lakeforge.schema import PARTITION_COL
from lakeforge.tableops import idempotent_merge


@dataclass
class BackfillStepResult:
    event_date: date
    version_before: int
    version_after: int
    merge_result: dict


def backfill_range(path: str, batches_by_date: dict[date, pa.Table]) -> list[BackfillStepResult]:
    """Ingest each date's batch in order, one Delta commit per date.

    Processing in date order isn't required for correctness (idempotent_merge
    is order-independent by construction), but it keeps the version history
    readable and makes a partial-failure midway through a backfill easy to
    reason about: everything up to and including the last logged step
    definitely landed.
    """
    results = []
    for event_date in sorted(batches_by_date):
        batch = batches_by_date[event_date]
        version_before = DeltaTable(path).version()
        merge_result = idempotent_merge(path, batch)
        version_after = DeltaTable(path).version()
        results.append(
            BackfillStepResult(
                event_date=event_date,
                version_before=version_before,
                version_after=version_after,
                merge_result=merge_result,
            )
        )
    return results


@dataclass
class ReconciliationMismatch:
    event_date: date
    field: str
    expected: object
    actual: object

    def __str__(self) -> str:
        return f"{self.event_date}: {self.field} expected {self.expected!r}, got {self.actual!r}"


def reconcile(path: str, expected_by_date: dict[date, dict]) -> list[ReconciliationMismatch]:
    """Compare the table's current state against expected per-partition
    aggregates. Each value in expected_by_date is a dict with any of:
    "row_count" (int) and "amount_sum" (float, compared with a small
    tolerance for floating-point accumulation). Row count alone can't catch
    every corruption class — a bug that matches the right NUMBER of rows but
    the WRONG keys (or clobbers values while leaving row count unchanged)
    would sail through a count-only check, which is why amount_sum is
    checked too.
    """
    table = DeltaTable(path).to_pyarrow_table()
    mismatches: list[ReconciliationMismatch] = []

    for event_date, expected in expected_by_date.items():
        mask = pc.equal(table[PARTITION_COL], pa.scalar(event_date, type=pa.date32()))
        subset = table.filter(mask)

        if "row_count" in expected:
            actual_count = subset.num_rows
            if actual_count != expected["row_count"]:
                mismatches.append(
                    ReconciliationMismatch(event_date, "row_count", expected["row_count"], actual_count)
                )

        if "amount_sum" in expected:
            actual_sum = pc.sum(subset["amount"]).as_py() or 0.0
            if abs(actual_sum - expected["amount_sum"]) > 1e-6:
                mismatches.append(
                    ReconciliationMismatch(event_date, "amount_sum", expected["amount_sum"], actual_sum)
                )

    return mismatches
