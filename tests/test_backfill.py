from datetime import date, datetime

import pytest
from deltalake import DeltaTable

from lakeforge.backfill import backfill_range, reconcile
from lakeforge.synthetic import make_batch, order_ids
from lakeforge.tableops import create_table, idempotent_merge


@pytest.fixture
def table_path(tmp_path):
    path = str(tmp_path / "orders")
    create_table(path)
    return path


def test_backfill_range_ingests_each_date_and_reconciles_clean(table_path):
    d1, d2, d3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
    batches = {
        d1: make_batch(order_ids("o1", 5), d1, datetime(2026, 1, 1, 9, 0, 0), amount_fn=lambda _: 10.0),
        d2: make_batch(order_ids("o2", 7), d2, datetime(2026, 1, 2, 9, 0, 0), amount_fn=lambda _: 20.0),
        d3: make_batch(order_ids("o3", 3), d3, datetime(2026, 1, 3, 9, 0, 0), amount_fn=lambda _: 30.0),
    }
    steps = backfill_range(table_path, batches)
    assert [s.event_date for s in steps] == [d1, d2, d3]
    assert all(s.version_after > s.version_before for s in steps)

    mismatches = reconcile(
        table_path,
        {
            d1: {"row_count": 5, "amount_sum": 50.0},
            d2: {"row_count": 7, "amount_sum": 140.0},
            d3: {"row_count": 3, "amount_sum": 90.0},
        },
    )
    assert mismatches == []


def test_reconcile_catches_row_count_mismatch(table_path):
    d1 = date(2026, 1, 1)
    idempotent_merge(table_path, make_batch(order_ids("o", 5), d1, datetime(2026, 1, 1, 9, 0, 0)))
    mismatches = reconcile(table_path, {d1: {"row_count": 6}})
    assert len(mismatches) == 1
    assert mismatches[0].field == "row_count"
    assert mismatches[0].expected == 6
    assert mismatches[0].actual == 5


def test_reconcile_catches_amount_sum_mismatch_even_with_correct_row_count(table_path):
    """A corruption that swaps two rows' amounts, or double-applies one
    row's amount while dropping another, could still leave row COUNT
    correct — this is exactly the corruption class row-count-only
    reconciliation would miss."""
    d1 = date(2026, 1, 1)
    idempotent_merge(
        table_path,
        make_batch(order_ids("o", 3), d1, datetime(2026, 1, 1, 9, 0, 0), amount_fn=lambda _: 100.0),
    )
    mismatches = reconcile(table_path, {d1: {"row_count": 3, "amount_sum": 999.0}})
    assert len(mismatches) == 1
    assert mismatches[0].field == "amount_sum"


def test_replaying_a_backfill_date_twice_is_safe(table_path):
    """The whole point of backfill-safety: re-running a backfill for a date
    that already succeeded (operator re-runs a job, a retry fires twice,
    etc.) must reconcile clean, not double the data."""
    d1 = date(2026, 1, 1)
    batch = make_batch(order_ids("o", 4), d1, datetime(2026, 1, 1, 9, 0, 0), amount_fn=lambda _: 25.0)
    backfill_range(table_path, {d1: batch})
    backfill_range(table_path, {d1: batch})  # re-run

    mismatches = reconcile(table_path, {d1: {"row_count": 4, "amount_sum": 100.0}})
    assert mismatches == []


def test_backfill_for_past_date_does_not_affect_other_dates(table_path):
    d1, d2 = date(2026, 1, 1), date(2026, 1, 2)
    idempotent_merge(table_path, make_batch(order_ids("live", 5), d2, datetime(2026, 1, 2, 9, 0, 0), amount_fn=lambda _: 50.0))

    backfill_range(table_path, {d1: make_batch(order_ids("hist", 10), d1, datetime(2026, 1, 1, 9, 0, 0), amount_fn=lambda _: 5.0)})

    mismatches = reconcile(table_path, {d1: {"row_count": 10}, d2: {"row_count": 5, "amount_sum": 250.0}})
    assert mismatches == []
