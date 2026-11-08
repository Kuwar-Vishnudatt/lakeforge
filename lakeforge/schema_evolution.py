"""Online schema evolution: add a nullable column to a live table without
rewriting any existing data files.

Delta Lake's ALTER TABLE ADD COLUMN is a metadata-only operation — it
appends a new field to the table's schema in the transaction log and every
existing Parquet file is simply read back with that column as NULL for rows
written before the column existed. This is the property that makes schema
evolution safe to run against a table that's actively being written to: no
rewrite means no window where readers see a half-migrated table.
"""

from deltalake import DeltaTable
from deltalake.schema import Field, PrimitiveType


def add_nullable_column(path: str, name: str, primitive_type: str) -> None:
    """Add a new nullable column. primitive_type is a Delta primitive type
    name such as "double", "string", "boolean", "long"."""
    dt = DeltaTable(path)
    dt.alter.add_columns(Field(name, PrimitiveType(primitive_type), nullable=True))


def current_columns(path: str) -> set[str]:
    dt = DeltaTable(path)
    return {f.name for f in dt.schema().fields}
