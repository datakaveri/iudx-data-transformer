# IUDX Data Transformer

Incrementally exports JSON documents from Elasticsearch to Parquet files in an S3-compatible object store (MinIO). Runs on a configurable schedule (default: every 15 minutes). Only new documents since the last run are exported — no duplicates.

---

## Table of Contents

- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [How Duplicate Prevention Works](#how-duplicate-prevention-works)
- [Configuration Reference](#configuration-reference)
- [Deployment](#deployment)
- [Observing Logs](#observing-logs)
- [Operations Runbook](#operations-runbook)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        iudx-net (Docker network)                │
│                                                                 │
│  ┌─────────────────┐        ┌──────────────────────────────┐   │
│  │  Elasticsearch  │        │     MinIO (S3-compatible)    │   │
│  │  :9200          │        │     :9000 (API)              │   │
│  │                 │        │     :9001 (Web console)      │   │
│  │  index-A  ──────┼──┐     └──────────────┬───────────────┘   │
│  │  index-B  ──────┼──┤                    │                   │
│  │  index-C  ──────┼──┤                    │                   │
│  └─────────────────┘  │                    │                   │
│                        │  ┌─────────────┐  │                   │
│                        └──► Transformer ├──┘                   │
│                           │  (cron job) │                      │
│                           └──────┬──────┘                      │
│                                  │                              │
└──────────────────────────────────┼──────────────────────────────┘
                                   │ /data/checkpoints/state.json
                              (named volume)
```

### Data flow per scheduled run

```
For each Elasticsearch index (= dataset id):
  │
  ├─ 1. GET /_count  →  compare with stored count
  │       └─ count unchanged? → SKIP (no upload)
  │
  ├─ 2. search_after query from last checkpoint
  │       └─ 0 results? → SKIP (no upload)
  │
  ├─ 3. Convert JSON records → Snappy-compressed Parquet
  │
  ├─ 4. Upload to  s3://<bucket>/<index-name>/<timestamp>_<uid>.parquet
  │
  └─ 5. Persist new checkpoint (search_after vector + doc count)
```

### Object store layout

```
iudx-data/                                     ← bucket
  ├── dataset-abc/
  │   ├── 2026-03-04T09-00-01_a1b2c3d4.parquet
  │   └── 2026-03-04T09-15-02_e5f6g7h8.parquet
  └── dataset-xyz/
      └── 2026-03-04T09-00-05_i9j0k1l2.parquet
```

Each file contains only the documents that arrived between two consecutive runs.

---

## Project Structure

```
iudx-data-transformer/
│
├── docker-compose.infra.yml    Infrastructure: Elasticsearch + MinIO
│                               Owns the shared Docker network (iudx-net)
│                               and named data volumes.
│
├── docker-compose.cronjob.yml  Application: Transformer cron job
│                               Joins iudx-net as an external network.
│                               Mounts config.yaml and a checkpoints volume.
│
├── config.yaml                 All runtime configuration (URLs, credentials,
│                               schedule). Mounted read-only into the container.
│
├── Dockerfile                  Multi-stage Python 3.11 image for the transformer.
│
├── requirements.txt            Python dependencies.
│
└── src/
    ├── main.py                 Entry point. Loads config, wires components,
    │                           runs the job immediately on startup then on
    │                           the configured interval via APScheduler.
    │
    ├── es_client.py            Elasticsearch wrapper.
    │                           - Lists all user-facing indices
    │                           - Fetches documents incrementally using
    │                             search_after pagination
    │
    ├── storage_client.py       MinIO / S3 wrapper (boto3).
    │                           - Auto-creates the bucket if missing
    │                           - Uploads Parquet bytes under
    │                             <dataset_id>/<timestamp>_<uid>.parquet
    │
    ├── transformer.py          Converts a list of JSON dicts to
    │                           Snappy-compressed Parquet bytes using
    │                           pandas + pyarrow. Nested objects/arrays
    │                           are JSON-serialised to string columns.
    │
    └── checkpoint.py           Persists per-index state to a JSON file.
                                State includes: search_after vector,
                                doc count, and last successful push time.
                                Uses atomic file replacement to prevent
                                corruption on crash.
```

---

## How Duplicate Prevention Works

Two independent guards run on every cycle per index:

**Guard 1 – Document count check (fast path)**
The transformer stores the total document count of each index after every successful push. At the start of each run it queries `GET /<index>/_count`. If the count has not grown, the index is skipped immediately — no documents are fetched.

**Guard 2 – `search_after` pagination (precise boundary)**
Documents are sorted by `[sort_field ASC, _id ASC]`. After a successful push, the sort-key vector of the last document is saved as the checkpoint. On the next run, passing this vector as `search_after` instructs Elasticsearch to return only documents that sort *after* the checkpoint. This is ES-native and produces zero overlap.

**Safe retry on upload failure**
The checkpoint is updated *only after* a successful upload. If MinIO is temporarily unreachable, the next scheduled run re-fetches the same documents and retries the upload automatically.

---

## Configuration Reference

All settings live in `config.yaml`. The file is mounted into the container at `/app/config.yaml` (read-only).

### Elasticsearch

```yaml
elasticsearch:
  host: elasticsearch     # Hostname or IP of the ES node
  port: 9200              # ES HTTP port
  scheme: http            # http or https
  username: elastic       # ES username
  password: changeme      # ES password

  sort_field: "observationDateTime"
  # Name of the timestamp field used to order documents for incremental fetch.
  # Must be present in all documents of every index.
  # Set to "" (empty string) to fall back to _id-based ordering.
  # Tip: for IUDX data this is typically "observationDateTime".

  batch_size: 10000
  # Number of documents fetched per Elasticsearch page.
  # Lower this if the transformer runs out of memory on large documents.
  # Raise it to reduce the number of round-trips for large datasets.
```

### Object Store (MinIO / S3)

```yaml
object_store:
  endpoint_url: http://minio:9000
  # Full URL of the S3-compatible endpoint.
  # For AWS S3: https://s3.amazonaws.com
  # For MinIO: http://<host>:9000

  access_key: minioadmin  # S3 access key / MinIO root user
  secret_key: minioadmin  # S3 secret key / MinIO root password
  bucket: iudx-data       # Destination bucket (auto-created if missing)
  region: us-east-1       # AWS region; ignored by MinIO but required by boto3
```

### Transformer Behaviour

```yaml
transformer:
  schedule_minutes: 15
  # How often the job runs. The first run happens immediately on container
  # startup; subsequent runs follow this interval.

  checkpoint_file: /data/checkpoints/state.json
  # Path inside the container where checkpoint state is stored.
  # This path is backed by a Docker named volume so it survives restarts.
  # Do not change unless you also update the volume mount in
  # docker-compose.cronjob.yml.
```

### Connecting to external / existing Elasticsearch or S3

To point the transformer at infrastructure you already operate, edit `config.yaml` before starting the cronjob stack:

```yaml
# Example: AWS OpenSearch + AWS S3
elasticsearch:
  host: my-domain.us-east-1.es.amazonaws.com
  port: 443
  scheme: https
  username: admin
  password: <your-password>

object_store:
  endpoint_url: https://s3.amazonaws.com
  access_key: AKIA...
  secret_key: <your-secret>
  bucket: my-iudx-bucket
  region: ap-south-1
```

Then start **only** the cronjob stack (skip `docker-compose.infra.yml`):

```bash
docker compose -f docker-compose.cronjob.yml build
docker compose -f docker-compose.cronjob.yml up -d
```

---

## Deployment

### Prerequisites

- Docker Engine >= 20.10
- Docker Compose v2 (`docker compose` command, not `docker-compose`)

### Step 1 — Clone and configure

```bash
git clone <repo-url>
cd iudx-data-transformer

# Edit credentials and endpoints before starting
vim config.yaml
```

### Step 2 — Start infrastructure (Elasticsearch + MinIO)

```bash
docker compose -f docker-compose.infra.yml up -d
```

Wait for both services to become healthy:

```bash
docker compose -f docker-compose.infra.yml ps
# Both services should show "(healthy)" status
```

### Step 3 — Build and start the transformer

```bash
# Build the transformer image
docker compose -f docker-compose.cronjob.yml build

# Start the cron job
docker compose -f docker-compose.cronjob.yml up -d
```

### Step 4 — Verify

```bash
# Check the transformer started correctly
docker logs iudx-transformer --tail 30

# Browse uploaded Parquet files in MinIO
# Open http://localhost:9001 in your browser
# Login: minioadmin / minioadmin
```

### Stopping services

```bash
# Stop only the transformer (keeps infrastructure running)
docker compose -f docker-compose.cronjob.yml down

# Stop everything (data volumes are preserved)
docker compose -f docker-compose.infra.yml down
docker compose -f docker-compose.cronjob.yml down

# Stop everything AND delete all data volumes (destructive)
docker compose -f docker-compose.infra.yml down -v
docker compose -f docker-compose.cronjob.yml down -v
```

### Updating the transformer after a code change

```bash
docker compose -f docker-compose.cronjob.yml build
docker compose -f docker-compose.cronjob.yml up -d --force-recreate
```

Checkpoints are stored in a named volume and are preserved across image rebuilds.

---

## Observing Logs

### Stream live logs

```bash
docker logs -f iudx-transformer
```

### Show the last N lines

```bash
docker logs --tail 100 iudx-transformer
```

### Filter by log level

```bash
# Show only errors
docker logs -f iudx-transformer 2>&1 | grep ERROR

# Show run summaries
docker logs -f iudx-transformer 2>&1 | grep "Run complete"

# Show upload events
docker logs -f iudx-transformer 2>&1 | grep "Uploaded"
```

### Typical log output

```
2026-03-04T09:00:00 INFO     transformer.main – Configuration loaded from /app/config.yaml
2026-03-04T09:00:00 INFO     transformer.main – Scheduler configured – interval=15 minutes.
2026-03-04T09:00:00 INFO     transformer.main – === Transformation run started ===
2026-03-04T09:00:01 INFO     transformer.es_client – Found 4 dataset indices.
2026-03-04T09:00:01 INFO     transformer.main – Index 'dataset-abc': 320 new document(s) to push.
2026-03-04T09:00:02 INFO     transformer.storage – Uploaded 45231 bytes → s3://iudx-data/dataset-abc/2026-03-04T09-00-02_a1b2c3d4.parquet
2026-03-04T09:00:02 INFO     transformer.main – Index 'dataset-xyz': no new documents (count=1500) – skipping.
2026-03-04T09:00:02 INFO     transformer.main – === Run complete – pushed=1  skipped=3  failed=0 ===
```

### Inspect the checkpoint state

```bash
# Print current checkpoint state
docker run --rm \
  -v iudx-data-transformer_transformer-checkpoints:/data \
  alpine cat /data/checkpoints/state.json
```

Example output:

```json
{
  "dataset-abc": {
    "search_after": ["2026-03-04T08:59:55.000Z", "doc_id_8821"],
    "doc_count": 4820,
    "last_push": "2026-03-04T09:00:02.341Z"
  },
  "dataset-xyz": {
    "search_after": ["2026-03-04T08:44:10.000Z", "doc_id_1500"],
    "doc_count": 1500,
    "last_push": "2026-03-04T08:45:01.112Z"
  }
}
```

---

## Operations Runbook

### Reset the checkpoint for one index (force full re-export)

```bash
# 1. Stop the transformer
docker compose -f docker-compose.cronjob.yml down

# 2. Edit the checkpoint file — remove the entry for the target index
docker run --rm -it \
  -v iudx-data-transformer_transformer-checkpoints:/data \
  alpine vi /data/checkpoints/state.json

# 3. Restart — the transformer will re-export all documents for that index
docker compose -f docker-compose.cronjob.yml up -d
```

### Reset all checkpoints (full re-export of everything)

```bash
docker compose -f docker-compose.cronjob.yml down
docker volume rm iudx-data-transformer_transformer-checkpoints
docker compose -f docker-compose.cronjob.yml up -d
```

### Change the schedule without rebuilding the image

Edit `config.yaml`, then restart the container (the config file is bind-mounted, so a rebuild is not needed):

```bash
vim config.yaml   # change transformer.schedule_minutes
docker compose -f docker-compose.cronjob.yml restart transformer
```

### Rotate credentials

Edit the relevant section in `config.yaml` and restart:

```bash
vim config.yaml
docker compose -f docker-compose.cronjob.yml restart transformer
```

If you also change MinIO or Elasticsearch passwords, update both `config.yaml` and the corresponding environment variable in `docker-compose.infra.yml`, then recreate the infra stack.
