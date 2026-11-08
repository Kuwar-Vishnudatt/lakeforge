"""Synthetic order-event batches for tests and the demo."""

import random
from datetime import date, datetime, timedelta

import pyarrow as pa

from lakeforge.schema import ORDERS_SCHEMA

STATUSES = ["placed", "shipped", "delivered", "cancelled"]


def make_batch(
    order_ids: list[str],
    event_date: date,
    updated_at: datetime,
    status: str = "placed",
    amount_fn=None,
    schema: pa.Schema = ORDERS_SCHEMA,
) -> pa.Table:
    """Build a batch where every row shares the same event_date/updated_at/
    status, except amount which can vary per row via amount_fn(order_id)."""
    n = len(order_ids)
    amounts = [amount_fn(oid) if amount_fn else round(random.uniform(5, 500), 2) for oid in order_ids]
    data = {
        "order_id": order_ids,
        "updated_at": [updated_at] * n,
        "status": [status] * n,
        "amount": amounts,
        "event_date": [event_date] * n,
    }
    return pa.table(data, schema=schema)


def order_ids(prefix: str, n: int, start: int = 0) -> list[str]:
    return [f"{prefix}-{i}" for i in range(start, start + n)]


def daterange(start: date, days: int) -> list[date]:
    return [start + timedelta(days=i) for i in range(days)]
