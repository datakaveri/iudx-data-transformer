"""
Checkpoint manager for tracking last-pushed state per Elasticsearch index.
Persists state to a JSON file so it survives container restarts.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


class CheckpointManager:
    """
    Stores per-index checkpoint data:
      - search_after: last sort values used in ES search_after pagination
      - doc_count: number of documents pushed so far
      - last_push: ISO timestamp of the last successful push
    """

    def __init__(self, checkpoint_file: str, resume: bool = True) -> None:
        self.checkpoint_file = checkpoint_file
        self._state: dict = self._load() if resume else {}
        if not resume:
            logger.info("resume_checkpoint=false – ignoring existing checkpoint file, starting fresh.")
        # Always persist on startup so the file exists from the first run onward.
        self._save()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r") as fh:
                    data = json.load(fh)
                logger.info("Loaded checkpoints from %s", self.checkpoint_file)
                return data
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Could not read checkpoint file (%s); starting fresh.", exc)
        return {}

    def _save(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.checkpoint_file)), exist_ok=True)
        tmp = self.checkpoint_file + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(self._state, fh, indent=2)
        os.replace(tmp, self.checkpoint_file)  # atomic write

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_search_after(self, index: str) -> Optional[list]:
        """Return the stored search_after value for *index*, or None."""
        return self._state.get(index, {}).get("search_after")

    def get_doc_count(self, index: str) -> int:
        """Return the number of documents pushed so far for *index*."""
        return self._state.get(index, {}).get("doc_count", 0)

    def update(self, index: str, search_after: Any, doc_count: int) -> None:
        """Persist new checkpoint after a successful push."""
        self._state[index] = {
            "search_after": search_after,
            "doc_count": doc_count,
            "last_push": datetime.now(timezone.utc).isoformat(),
        }
        self._save()
        logger.debug("Checkpoint updated for index '%s': search_after=%s, doc_count=%d",
                     index, search_after, doc_count)

    def has_index(self, index: str) -> bool:
        return index in self._state
