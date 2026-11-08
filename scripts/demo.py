#!/usr/bin/env python3
"""End-to-end demo: live ingestion, online schema evolution, a backfill of a
historical date with corrected data, and reconciliation — all against a real
local Delta table (no cluster, no cloud storage, just the filesystem)."""

import shutil
import tempfile
from datetime import date, datetime

from deltalake import DeltaTable

from lakeforge.backfill import backfill_range, reconcile
from lakeforge.schema_evolution import add_nullable_column, current_columns
from lakeforge.synthetic import make_batch, order_ids
from lakeforge.tableops import create_table, idempotent_merge


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    workdir = tempfile.mkdtemp(prefix="lakeforge-demo-")
    table_path = f"{workdir}/orders"
    print(f"table path: {table_path}")

    section("1. Create table, ingest 2 days of 'live' traffic")
    create_table(table_path)
    d1, d2 = date(2026, 1, 1), date(2026, 1, 2)
    idempotent_merge(table_path, make_batch(order_ids("live1", 20), d1, datetime(2026, 1, 1, 9, 0, 0), amount_fn=lambda _: 10.0))
    idempotent_merge(table_path, make_batch(order_ids("live2", 15), d2, datetime(2026, 1, 2, 9, 0, 0), amount_fn=lambda _: 10.0))
    print(f"version={DeltaTable(table_path).version()} rows={DeltaTable(table_path).to_pyarrow_table().num_rows}")

    section("2. Online schema evolution: add 'discount_pct' (metadata-only, no rewrite)")
    files_before = set(DeltaTable(table_path).file_uris())
    add_nullable_column(table_path, "discount_pct", "double")
    files_after = set(DeltaTable(table_path).file_uris())
    print(f"columns now: {sorted(current_columns(table_path))}")
    print(f"data files unchanged by the schema change: {files_before == files_after}")

    section("3. New live batch populates the new column")
    import pyarrow as pa

    from lakeforge.schema import ORDERS_SCHEMA

    new_schema = ORDERS_SCHEMA.append(pa.field("discount_pct", pa.float64()))
    live3_ids = order_ids("live3", 5)
    batch = pa.table(
        {
            "order_id": live3_ids,
            "updated_at": [datetime(2026, 1, 2, 10, 0, 0)] * 5,
            "status": ["placed"] * 5,
            "amount": [10.0] * 5,
            "event_date": [d2] * 5,
            "discount_pct": [0.1] * 5,
        },
        schema=new_schema,
    )
    idempotent_merge(table_path, batch)

    section("4. Backfill day 1 with a corrected export (old-shaped, no discount_pct column)")
    corrected_d1 = make_batch(order_ids("live1", 20), d1, datetime(2026, 1, 1, 15, 0, 0), status="shipped", amount_fn=lambda _: 12.5)
    steps = backfill_range(table_path, {d1: corrected_d1})
    for s in steps:
        print(f"  backfilled {s.event_date}: version {s.version_before} -> {s.version_after}")

    section("5. Reconcile")
    mismatches = reconcile(
        table_path,
        {
            d1: {"row_count": 20, "amount_sum": 20 * 12.5},
            d2: {"row_count": 20, "amount_sum": 15 * 10.0 + 5 * 10.0},
        },
    )
    if mismatches:
        print("MISMATCHES FOUND:")
        for m in mismatches:
            print(f"  {m}")
    else:
        print("reconciliation clean: every partition matches expected row count and amount sum")

    section("6. Re-run the SAME backfill again (idempotency check)")
    before = DeltaTable(table_path).to_pyarrow_table().to_pandas().sort_values("order_id").reset_index(drop=True)
    backfill_range(table_path, {d1: corrected_d1})
    after = DeltaTable(table_path).to_pyarrow_table().to_pandas().sort_values("order_id").reset_index(drop=True)
    print(f"table unchanged after re-running the identical backfill: {before.equals(after)}")

    print(f"\ndemo complete. table left at {table_path} for inspection; `rm -rf {workdir}` to clean up.")


if __name__ == "__main__":
    main()
