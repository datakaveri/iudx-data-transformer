import json
import logging
import uuid
from datetime import datetime, timezone

import redis

logger = logging.getLogger("transformer.redis")


class RedisClient:
    def __init__(self, cfg: dict) -> None:
        prefix = cfg.get("key_prefix", "")
        self._queue = f"{prefix}{cfg['readiness_queue_name']}"
        self._zip_queue = f"{prefix}{cfg['zip_queue_name']}"
        self._client = redis.Redis(
            host=cfg["host"],
            port=int(cfg["port"]),
            username=cfg.get("username") or None,
            password=cfg.get("password") or None,
            decode_responses=True,
        )
        logger.info(
            "Redis client initialised – host=%s port=%s prefix=%r queue=%s zip_queue=%s",
            cfg["host"], cfg["port"], prefix, self._queue, self._zip_queue,
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

    def push_zip_message(self, databank_id: str) -> None:
        message = {
            "jobId": str(uuid.uuid4()),
            "type": "zip",
            "databankId": databank_id,
            "options": {},
            "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        }
        self._client.rpush(self._zip_queue, json.dumps(message))
        logger.info(
            "Pushed zip message to '%s' for databankId='%s' (jobId=%s)",
            self._zip_queue, databank_id, message["jobId"],
        )

    def close(self) -> None:
        self._client.close()
