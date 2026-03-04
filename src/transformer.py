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
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def _normalise_value(value: Any) -> Any:
    """Flatten nested structures to JSON strings for consistent Parquet schema."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _normalise_record(record: dict) -> dict:
    return {k: _normalise_value(v) for k, v in record.items()}


def json_records_to_parquet(records: list[dict]) -> bytes:
    """
    Convert a list of JSON records to Snappy-compressed Parquet bytes.

    Parameters
    ----------
    records : list of dicts (each dict is one ES _source document)

    Returns
    -------
    bytes – raw Parquet file content
    """
    if not records:
        raise ValueError("Cannot create Parquet file from an empty record set.")

    normalised = [_normalise_record(r) for r in records]

    df = pd.DataFrame(normalised)

    # Coerce object columns that look like numeric to their proper types
    for col in df.select_dtypes(include="object").columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")

    table = pa.Table.from_pandas(df, preserve_index=False)

    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression="snappy",
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )
    buf.seek(0)
    data = buf.read()

    logger.debug(
        "Converted %d records to Parquet (%d bytes, %d columns)",
        len(records), len(data), len(table.schema),
    )
    return data
