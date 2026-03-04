"""
Elasticsearch client wrapper.

New-document detection strategy
---------------------------------
Documents are fetched using ES `search_after` pagination with a stable sort:
  1. [sort_field ASC, _id ASC]  – when a timestamp/sort field is configured
  2. [_id ASC]                  – fallback (works for string _id ordering)

The caller stores the last returned sort-key vector as the checkpoint.
On the next run, passing that vector as `search_after` returns only documents
inserted *after* the checkpoint, giving us an incremental / delta fetch with
no duplicates.

If an index has no checkpoint (first run) all documents are returned.
If the index has not grown since the last run, an empty list is returned.
"""

import logging
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
          sort_field (optional), batch_size (optional, default 10000)
        """
        self._sort_field: Optional[str] = config.get("sort_field")
        self._batch_size: int = int(config.get("batch_size", 10_000))

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
        """Return all user-facing indices (excludes system indices)."""
        try:
            all_indices = list(self._client.indices.get_alias().keys())
        except Exception as exc:
            logger.error("Failed to list ES indices: %s", exc)
            raise

        result = [
            idx for idx in all_indices
            if not any(idx.startswith(prefix) for prefix in _SYSTEM_INDEX_PREFIXES)
        ]
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
        sort: list[dict] = []
        if self._sort_field:
            sort.append({
                self._sort_field: {
                    "order": "asc",
                    "unmapped_type": "date",   # graceful fallback for missing field
                }
            })
        sort.append({"_id": "asc"})  # stable tiebreaker
        return sort

    def close(self) -> None:
        self._client.close()
