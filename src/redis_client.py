import json
import logging
import uuid
from datetime import datetime, timezone

import redis

logger = logging.getLogger("transformer.redis")


class RedisClient:
    def __init__(self, cfg: dict) -> None:
        self._queue = cfg["readiness_queue_name"]
        self._client = redis.Redis(
            host=cfg["host"],
            port=int(cfg["port"]),
            db=int(cfg.get("db", 0)),
            username=cfg.get("username") or None,
            password=cfg.get("password") or None,
            decode_responses=True,
        )
        logger.info(
            "Redis client initialised – host=%s port=%s db=%s queue=%s",
            cfg["host"], cfg["port"], cfg.get("db", 0), self._queue,
        )

    def push_readiness_message(self, databank_id: str) -> None:
        message = {
            "jobId": str(uuid.uuid4()),
            "type": "report",
            "databankId": databank_id,
            "options": {},
            "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        }
        self._client.rpush(self._queue, json.dumps(message))
        logger.info(
            "Pushed readiness message to '%s' for databankId='%s' (jobId=%s)",
            self._queue, databank_id, message["jobId"],
        )

    def close(self) -> None:
        self._client.close()
