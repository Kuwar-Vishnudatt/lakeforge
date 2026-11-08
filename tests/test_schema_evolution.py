from datetime import date, datetime

import pytest
from deltalake import DeltaTable

from lakeforge.schema_evolution import add_nullable_column, current_columns
from lakeforge.synthetic import make_batch, order_ids
from lakeforge.tableops import create_table, idempotent_merge


@pytest.fixture
def table_path(tmp_path):
    path = str(tmp_path / "orders")
    create_table(path)
    return path


def test_add_column_is_metadata_only_no_rewrite(table_path):
    ids = order_ids("o", 10)
    idempotent_merge(table_path, make_batch(ids, date(2026, 1, 1), datetime(2026, 1, 1, 10, 0, 0)))
    version_before = DeltaTable(table_path).version()
    files_before = set(DeltaTable(table_path).file_uris())

    add_nullable_column(table_path, "discount_pct", "double")

    version_after = DeltaTable(table_path).version()
    files_after = set(DeltaTable(table_path).file_uris())

    assert version_after == version_before + 1
    assert files_after == files_before, "adding a column must not rewrite any existing data files"
    assert "discount_pct" in current_columns(table_path)


def test_old_rows_read_back_as_null_for_new_column(table_path):
    ids = order_ids("o", 5)
    idempotent_merge(table_path, make_batch(ids, date(2026, 1, 1), datetime(2026, 1, 1, 10, 0, 0)))
    add_nullable_column(table_path, "discount_pct", "double")

    df = DeltaTable(table_path).to_pyarrow_table().to_pandas()
    assert df["discount_pct"].isna().all()


def test_new_batch_can_populate_new_column_after_evolution(table_path):
    ids = order_ids("o", 3)
    idempotent_merge(table_path, make_batch(ids, date(2026, 1, 1), datetime(2026, 1, 1, 10, 0, 0)))
    add_nullable_column(table_path, "discount_pct", "double")

    import pyarrow as pa

    from lakeforge.schema import ORDERS_SCHEMA

    new_schema = ORDERS_SCHEMA.append(pa.field("discount_pct", pa.float64()))
    new_ids = order_ids("o", 2, start=100)
    batch = pa.table(
        {
            "order_id": new_ids,
            "updated_at": [datetime(2026, 1, 1, 11, 0, 0)] * 2,
            "status": ["placed"] * 2,
            "amount": [10.0, 20.0],
            "event_date": [date(2026, 1, 1)] * 2,
            "discount_pct": [0.1, 0.2],
        },
        schema=new_schema,
    )
    idempotent_merge(table_path, batch)

    df = DeltaTable(table_path).to_pyarrow_table().to_pandas()
    new_rows = df[df["order_id"].isin(new_ids)]
    assert sorted(new_rows["discount_pct"].tolist()) == [0.1, 0.2]


def test_backfill_batch_without_new_column_still_merges_cleanly(table_path):
    """A historical/backfill source often predates a schema change and
    simply doesn't have the new column at all. Ingesting that old-shaped
    batch after the table has already evolved must not error, and must
    leave the new column NULL for whatever it touches (inserts) or
    untouched (updates) — it should never need to "know about" a column
    that didn't exist when it was produced."""
    ids = order_ids("o", 3)
    idempotent_merge(table_path, make_batch(ids, date(2026, 1, 1), datetime(2026, 1, 1, 10, 0, 0), status="placed"))
    add_nullable_column(table_path, "discount_pct", "double")

    # Old-shaped backfill batch: no discount_pct column at all, newer
    # updated_at so it legitimately supersedes the existing rows.
    old_shaped_batch = make_batch(ids, date(2026, 1, 1), datetime(2026, 1, 1, 12, 0, 0), status="shipped")
    idempotent_merge(table_path, old_shaped_batch)

    df = DeltaTable(table_path).to_pyarrow_table().to_pandas()
    updated = df[df["order_id"].isin(ids)]
    assert set(updated["status"]) == {"shipped"}
    assert updated["discount_pct"].isna().all()
