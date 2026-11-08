from datetime import date, datetime

import pytest
from deltalake import DeltaTable

from lakeforge.schema import ORDERS_SCHEMA
from lakeforge.synthetic import make_batch, order_ids
from lakeforge.tableops import create_table, idempotent_merge


@pytest.fixture
def table_path(tmp_path):
    path = str(tmp_path / "orders")
    create_table(path)
    return path


def read_df(path):
    return DeltaTable(path).to_pyarrow_table().to_pandas()


def test_initial_insert(table_path):
    batch = make_batch(order_ids("o", 5), date(2026, 1, 1), datetime(2026, 1, 1, 12, 0, 0), status="placed")
    idempotent_merge(table_path, batch)
    df = read_df(table_path)
    assert len(df) == 5
    assert set(df["status"]) == {"placed"}


def test_replaying_identical_batch_is_a_no_op(table_path):
    batch = make_batch(order_ids("o", 5), date(2026, 1, 1), datetime(2026, 1, 1, 12, 0, 0), status="placed")
    idempotent_merge(table_path, batch)
    df_before = read_df(table_path).sort_values("order_id").reset_index(drop=True)

    idempotent_merge(table_path, batch)  # exact replay
    df_after = read_df(table_path).sort_values("order_id").reset_index(drop=True)

    assert len(df_after) == 5, "replaying an identical batch must not duplicate rows"
    assert df_before.equals(df_after), "replaying an identical batch must not change any values"


def test_newer_update_overwrites_older_value(table_path):
    ids = order_ids("o", 3)
    t1 = datetime(2026, 1, 1, 10, 0, 0)
    t2 = datetime(2026, 1, 1, 14, 0, 0)
    idempotent_merge(table_path, make_batch(ids, date(2026, 1, 1), t1, status="placed"))
    idempotent_merge(table_path, make_batch(ids, date(2026, 1, 1), t2, status="shipped"))

    df = read_df(table_path)
    assert set(df["status"]) == {"shipped"}
    assert set(df["updated_at"].astype(str).str[:19]) == {"2026-01-01 14:00:00"}


def test_stale_out_of_order_update_is_ignored(table_path):
    """The defining backfill-safety property: replaying an OLD batch AFTER
    a NEWER one has already landed must not regress the row, even though
    the old batch is being applied later in wall-clock time."""
    ids = order_ids("o", 3)
    t_old = datetime(2026, 1, 1, 10, 0, 0)
    t_new = datetime(2026, 1, 1, 14, 0, 0)

    idempotent_merge(table_path, make_batch(ids, date(2026, 1, 1), t_new, status="shipped"))
    idempotent_merge(table_path, make_batch(ids, date(2026, 1, 1), t_old, status="placed"))  # late/stale replay

    df = read_df(table_path)
    assert set(df["status"]) == {"shipped"}, "a stale backfill batch must never overwrite a newer value"


def test_insert_and_update_in_the_same_batch(table_path):
    existing = order_ids("o", 3)
    idempotent_merge(table_path, make_batch(existing, date(2026, 1, 1), datetime(2026, 1, 1, 10, 0, 0), status="placed"))

    mixed_ids = existing + order_ids("o", 2, start=100)  # 3 existing keys + 2 brand-new keys
    idempotent_merge(table_path, make_batch(mixed_ids, date(2026, 1, 1), datetime(2026, 1, 1, 11, 0, 0), status="shipped"))

    df = read_df(table_path)
    assert len(df) == 5
    assert set(df["status"]) == {"shipped"}


def test_equal_version_does_not_update(table_path):
    """Strictly-greater, not greater-or-equal: two batches that happen to
    carry the exact same updated_at must not be treated as one superseding
    the other (there's no basis to prefer either), so the second is a no-op
    against the first."""
    ids = order_ids("o", 2)
    t = datetime(2026, 1, 1, 10, 0, 0)
    idempotent_merge(table_path, make_batch(ids, date(2026, 1, 1), t, status="placed"))
    idempotent_merge(table_path, make_batch(ids, date(2026, 1, 1), t, status="shipped"))

    df = read_df(table_path)
    assert set(df["status"]) == {"placed"}
