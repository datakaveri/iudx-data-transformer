"""
JSON → Parquet conversion.

Schema handling
---------------
* Scalar fields (str, int, float, bool, None) are kept as-is.
* Nested dicts are JSON-serialised to a string column so that the Parquet
  schema stays flat and compatible with downstream tools (Spark, Athena, etc.)
* Lists are likewise JSON-serialised to string.
* pyarrow infers column types from the data; schema drift between batches is
  handled by casting to a common schema when possible.

The result is returned as raw bytes (Snappy-compressed Parquet) so the caller
can stream it directly to the object store without touching the filesystem.
"""

import io
import json
import logging
import re
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


_ALPHANUMERIC_RE = re.compile(r"[^a-zA-Z0-9]")


def _to_alphanumeric(value: Any) -> str:
    return _ALPHANUMERIC_RE.sub("", str(value))


def _normalise_value(value: Any) -> Any:
    """Flatten nested structures to JSON strings for consistent Parquet schema."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _normalise_record(record: dict) -> dict:
    return {k: _normalise_value(v) for k, v in record.items()}


def _coerce_mixed_columns(df: pd.DataFrame) -> None:
    """In-place: cast mixed-type object columns to alphanumeric strings."""
    for col in df.columns:
        if df[col].dtype == object:
            non_null = df[col].dropna()
            if non_null.apply(lambda x: not isinstance(x, str)).any():
                df[col] = df[col].where(df[col].isna(), df[col].map(_to_alphanumeric))


def _align_table(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Align a batch table to an established schema.

    Columns missing from the batch are added as nulls; type mismatches are
    cast to the target type, falling back to string on failure.
    """
    arrays = []
    for field in schema:
        if field.name in table.schema.names:
            col = table.column(field.name)
            if col.type != field.type:
                try:
                    col = col.cast(field.type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    col = col.cast(pa.string())
            arrays.append(col)
        else:
            arrays.append(pa.array([None] * len(table), type=field.type))
    return pa.table(dict(zip(schema.names, arrays)))


def json_records_batches_to_parquet(batches: Iterable[list[dict]]) -> bytes:
    """Stream batches of JSON records into a single Snappy-compressed Parquet file.

    Only one batch is materialised in memory at a time, so this handles
    arbitrarily large datasets without OOM risk.

    The schema is fixed from the first non-empty batch; subsequent batches are
    aligned to it (missing columns become nulls, type mismatches are cast).
    """
    buf = io.BytesIO()
    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None
    total_rows = 0

    for batch in batches:
        if not batch:
            continue

        normalised = [_normalise_record(r) for r in batch]
        df = pd.DataFrame(normalised)
        _coerce_mixed_columns(df)
        table = pa.Table.from_pandas(df, preserve_index=False)

        if writer is None:
            schema = table.schema
            writer = pq.ParquetWriter(
                buf, schema,
                compression="snappy",
                coerce_timestamps="ms",
                allow_truncated_timestamps=True,
            )
        else:
            table = _align_table(table, schema)

        writer.write_table(table)
        total_rows += len(table)

    if writer is None:
        raise ValueError("Cannot create Parquet file from an empty record set.")

    writer.close()
    buf.seek(0)
    data = buf.read()
    logger.debug("Wrote %d rows to Parquet (%d bytes)", total_rows, len(data))
    return data
