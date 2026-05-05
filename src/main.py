"""
IUDX Data Transformer – main entry point.

Runs a scheduled job every N minutes (default 15) that:
  1. Lists all Elasticsearch indices (each represents a dataset)
  2. For each index, fetches documents that arrived since the last run
     using search_after-based incremental pagination
  3. Converts new documents to Parquet (Snappy-compressed)
  4. Uploads the Parquet file to the configured object store bucket
     under a folder named after the dataset id
  5. Updates the per-index checkpoint so the next run skips these docs

No duplicate uploads: if an index has no new documents the run for that
index is skipped entirely.

Config is loaded from the path specified by the env var CONFIG_PATH
(default: /app/config.yaml).
"""

import logging
import os
import sys
import time

import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from checkpoint import CheckpointManager
from es_client import ESClient
from redis_client import RedisClient
from storage_client import StorageClient
from transformer import json_records_batches_to_parquet

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("transformer.main")

# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config.yaml")


def load_config(path: str) -> dict:
    with open(path, "r") as fh:
        cfg = yaml.safe_load(fh)
    logger.info("Configuration loaded from %s", path)
    return cfg


# ---------------------------------------------------------------------------
# Core job
# ---------------------------------------------------------------------------

def run_transformation(
    es: ESClient,
    storage: StorageClient,
    checkpoints: CheckpointManager,
    redis: RedisClient,
) -> None:
    """Single execution of the ETL cycle across all ES indices."""
    logger.info("=== Transformation run started ===")

    try:
        indices = es.get_dataset_indices()
    except Exception as exc:
        logger.error("Cannot list ES indices – aborting run: %s", exc)
        return

    pushed, skipped, failed = 0, 0, 0

    for index in indices:
        try:
            _process_index(index, es, storage, checkpoints, redis)
            pushed += 1
        except _NoNewData:
            skipped += 1
        except Exception as exc:
            logger.error("Error processing index '%s': %s", index, exc, exc_info=True)
            failed += 1

    logger.info(
        "=== Run complete – pushed=%d  skipped=%d  failed=%d ===",
        pushed, skipped, failed,
    )


class _NoNewData(Exception):
    """Raised when an index has no new documents since the last checkpoint."""


def _process_index(
    index: str,
    es: ESClient,
    storage: StorageClient,
    checkpoints: CheckpointManager,
    redis: RedisClient,
) -> None:
    search_after = checkpoints.get_search_after(index)
    prev_count = checkpoints.get_doc_count(index)

    # ---- fast-path: check whether doc count has grown -------------------
    current_count = es.get_doc_count(index)
    if checkpoints.has_index(index) and current_count <= prev_count:
        logger.info("Index '%s': no new documents (count=%d) – skipping.", index, current_count)
        raise _NoNewData

    # ---- incremental fetch + transform (streaming, one batch at a time) -
    # last_sort and total_docs are updated as a side-effect of _batch_iter
    # so the checkpoint can be advanced after a successful upload.
    last_sort = search_after
    total_docs = 0

    def _batch_iter():
        nonlocal last_sort, total_docs
        for batch, sort in es.iter_new_documents(index, search_after=search_after):
            last_sort = sort
            total_docs += len(batch)
            yield batch

    try:
        parquet_bytes = json_records_batches_to_parquet(_batch_iter())
    except ValueError:
        logger.info("Index '%s': search_after returned 0 docs – skipping.", index)
        raise _NoNewData

    logger.info("Index '%s': %d new document(s) to push.", index, total_docs)

    # ---- upload ----------------------------------------------------------
    # NOTE: checkpoint is updated only after a successful upload.
    #       If the upload fails, the next run will re-fetch and retry.
    storage.upload_parquet(dataset_id=index, data=parquet_bytes)

    # ---- notify Redis readiness queue ------------------------------------
    # Strip infrastructure prefix (e.g. 'iudx__') to get the bare dataset UUID,
    # matching the same extraction used by es_client.update_catalogue_last_updated.
    databank_id = index.split("__", 1)[-1]
    redis.push_readiness_message(databank_id=databank_id)
    redis.push_zip_message(databank_id=databank_id)

    # ---- update catalogue lastUpdated ------------------------------------
    es.update_catalogue_last_updated(index)

    # ---- update checkpoint -----------------------------------------------
    checkpoints.update(
        index=index,
        search_after=last_sort,
        doc_count=current_count,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    cfg = load_config(CONFIG_PATH)

    es = ESClient(cfg["elasticsearch"])
    storage = StorageClient(cfg["object_store"])
    redis = RedisClient(cfg["redis"])
    resume = bool(cfg["transformer"].get("resume_checkpoint", True))
    checkpoints = CheckpointManager(cfg["transformer"]["checkpoint_file"], resume=resume)

    # Ensure destination bucket exists before scheduling
    storage.ensure_bucket()

    interval_minutes: int = int(cfg["transformer"].get("schedule_minutes", 15))
    logger.info("Scheduler configured – interval=%d minutes.", interval_minutes)

    # Run once immediately on startup, then on schedule
    run_transformation(es, storage, checkpoints, redis)

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        func=run_transformation,
        trigger=IntervalTrigger(minutes=interval_minutes),
        kwargs={"es": es, "storage": storage, "checkpoints": checkpoints, "redis": redis},
        id="data_transformer",
        name="ES → Parquet → Object Store",
        max_instances=1,          # prevent overlapping runs
        coalesce=True,            # skip missed runs instead of stacking them
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
    finally:
        es.close()
        redis.close()


if __name__ == "__main__":
    main()
