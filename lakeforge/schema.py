"""The canonical orders schema used throughout this project's tests and demo.

A real lakehouse pipeline's schema lives wherever the business domain lives;
this one is deliberately simple (an order-events table) so the engineering
effort stays on the pipeline mechanics — idempotent merge, schema evolution,
backfill reconciliation, concurrent-writer safety — rather than on domain
modeling.
"""

import pyarrow as pa

KEY_COL = "order_id"
VERSION_COL = "updated_at"
PARTITION_COL = "event_date"

ORDERS_SCHEMA = pa.schema(
    [
        (KEY_COL, pa.string()),
        (VERSION_COL, pa.timestamp("us")),
        ("status", pa.string()),
        ("amount", pa.float64()),
        (PARTITION_COL, pa.date32()),
    ]
)


def empty_table(schema: pa.Schema = ORDERS_SCHEMA) -> pa.Table:
    return pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)
