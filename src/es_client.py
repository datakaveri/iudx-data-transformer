"""
Elasticsearch client wrapper.

New-document detection strategy
---------------------------------
Documents are fetched using ES `search_after` pagination sorted by `_seq_no`.

`_seq_no` is an internal Elasticsearch counter that is assigned at indexing
time and increments monotonically per primary shard.  For single-shard indices
(number_of_shards=1, as configured in docker-compose.infra.yml) this gives a
globally ordered insertion sequence.

Sorting by `_seq_no` means the checkpoint tracks *when a document was indexed*,
not the value of any content field such as `observationDateTime`.  This
correctly captures documents that arrive out-of-order or carry backdated
timestamps — a common pattern in IoT and batch-ingestion pipelines.

Previous approach (sort_field + _id) missed documents whose content timestamp
fell before the last checkpoint even though they were newly inserted.

If an index has no checkpoint (first run) all documents are returned.
If the index has not grown since the last run, an empty list is returned.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from elasticsearch import Elasticsearch, NotFoundError

logger = logging.getLogger(__name__)

# System indices to exclude from processing
_SYSTEM_INDEX_PREFIXES = (".", "kibana", "ilm-history", "logstash")


class ESClient:
    def __init__(self, config: dict) -> None:
        """
        Expected config keys:
          host, port, scheme (http/https), username, password,
          batch_size   (optional, default 10000)
          index_prefix (optional, default "" = all user indices)
        """
        self._batch_size: int = int(config.get("batch_size", 10_000))
        self._index_prefix: str = config.get("index_prefix", "")
        self._catalogue_index: str = config.get("catalogue_index", "catalogue")

        hosts = [{
            "host": config["host"],
            "port": int(config["port"]),
            "scheme": config.get("scheme", "http"),
        }]

        self._client = Elasticsearch(
            hosts=hosts,
            http_auth=(config["username"], config["password"]),
            timeout=30,
            max_retries=3,
            retry_on_timeout=True,
        )
        logger.info("ES client initialised → %s:%s", config["host"], config["port"])

    # ------------------------------------------------------------------
    # Index discovery
    # ------------------------------------------------------------------

    def get_dataset_indices(self) -> list[str]:
        """Return user-facing indices, filtered by index_prefix when configured."""
        try:
            all_indices = list(self._client.indices.get_alias().keys())
        except Exception as exc:
            logger.error("Failed to list ES indices: %s", exc)
            raise

        result = [
            idx for idx in all_indices
            if not any(idx.startswith(p) for p in _SYSTEM_INDEX_PREFIXES)
            and idx.startswith(self._index_prefix)
        ]

        if self._index_prefix:
            logger.info("Found %d dataset indices with prefix '%s'.",
                        len(result), self._index_prefix)
        else:
            logger.info("Found %d dataset indices.", len(result))

        return sorted(result)

    def get_doc_count(self, index: str) -> int:
        try:
            return self._client.count(index=index)["count"]
        except NotFoundError:
            return 0

    # ------------------------------------------------------------------
    # Incremental fetch
    # ------------------------------------------------------------------

    def fetch_new_documents(
        self,
        index: str,
        search_after: Optional[list] = None,
    ) -> tuple[list[dict], Optional[list]]:
        """
        Fetch all documents after *search_after* using paginated search_after.

        Returns
        -------
        (records, last_sort_value)
          records          – list of _source dicts (empty if nothing new)
          last_sort_value  – sort key of the last returned document, or the
                             original *search_after* if nothing was returned
        """
        sort = self._build_sort()
        body: dict[str, Any] = {
            "size": self._batch_size,
            "sort": sort,
            "query": {"match_all": {}},
            "track_total_hits": False,
        }

        if search_after:
            body["search_after"] = search_after

        all_docs: list[dict] = []
        current_sort = search_after

        while True:
            try:
                response = self._client.search(index=index, body=body)
            except Exception as exc:
                logger.error("ES search failed on index '%s': %s", index, exc)
                raise

            hits = response["hits"]["hits"]
            if not hits:
                break

            all_docs.extend(hit["_source"] for hit in hits)
            current_sort = hits[-1]["sort"]
            body["search_after"] = current_sort

            if len(hits) < self._batch_size:
                break  # last page

        logger.debug(
            "fetch_new_documents('%s'): returned %d new docs", index, len(all_docs)
        )
        return all_docs, current_sort

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_sort(self) -> list[dict]:
        # Sort by _seq_no (insertion order) so that search_after tracks
        # when documents were indexed, not their content timestamps.
        return [{"_seq_no": "asc"}]

    # ------------------------------------------------------------------
    # Catalogue
    # ------------------------------------------------------------------

    def update_catalogue_last_updated(self, index_id: str) -> None:
        """Set lastUpdated to now on the catalogue document whose _id matches index_id.

        Strips any infrastructure prefix (e.g. 'iudx__') before the lookup so
        that the catalogue document id remains the raw dataset UUID regardless
        of how the ES index is named.
        """
        catalogue_id = index_id.split("__", 1)[-1]
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")
        try:
            self._client.update(
                index=self._catalogue_index,
                id=catalogue_id,
                body={"doc": {"lastUpdated": now}},
            )
            logger.info(
                "Catalogue lastUpdated → index='%s' catalogue_id='%s' ts=%s",
                index_id, catalogue_id, now,
            )
        except NotFoundError:
            logger.debug(
                "No catalogue document for '%s' – skipping lastUpdated update.",
                catalogue_id,
            )
        except Exception as exc:
            logger.warning(
                "Failed to update catalogue lastUpdated for '%s': %s", catalogue_id, exc
            )

    def close(self) -> None:
        self._client.close()
