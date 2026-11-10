import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import redis
from redis.cluster import RedisCluster, ClusterNode
from redis.sentinel import Sentinel

logger = logging.getLogger("transformer.redis")

DEFAULT_SENTINEL_PORT = 26379

# Errors that are expected while Sentinel promotes a new master. The
# master_for() client re-discovers the master on the next attempt.
_RETRYABLE_ERRORS = (
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    redis.exceptions.ReadOnlyError,
)
# 1 + 2 + 4 + 8 + 10 = 25s total – long enough to ride out a failover
# (down-after-milliseconds + promotion + old-master demotion).
_PUSH_ATTEMPTS = 6
_PUSH_BACKOFF_SECONDS = 1.0
_PUSH_BACKOFF_MAX_SECONDS = 10.0


def _env_bool(name: str):
    value = os.environ.get(name)
    if value is None or value == "":
        return None
    return value.strip().lower() in ("1", "true", "yes")


def _parse_sentinel_hosts(raw, default_host: str) -> list:
    """Parse "h1:26379,h2:26380" (or a YAML list) into [(host, port), ...]."""
    if isinstance(raw, (list, tuple)):
        entries = [str(e) for e in raw]
    else:
        entries = str(raw or "").split(",")

    nodes = []
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        host, sep, port = entry.rpartition(":")
        if not sep:
            host, port = entry, ""
        nodes.append((host, int(port) if port else DEFAULT_SENTINEL_PORT))

    return nodes or [(default_host, DEFAULT_SENTINEL_PORT)]


def _sentinel_settings(cfg: dict) -> dict:
    """Merge the redis.sentinel YAML block with REDIS_SENTINEL_* env overrides."""
    s = dict(cfg.get("sentinel") or {})

    enabled = _env_bool("REDIS_SENTINEL_ENABLED")
    if enabled is not None:
        s["enabled"] = enabled
    for key, env in (
        ("master_name", "REDIS_SENTINEL_MASTER_NAME"),
        ("hosts", "REDIS_SENTINEL_HOSTS"),
        ("username", "REDIS_SENTINEL_USERNAME"),
        ("password", "REDIS_SENTINEL_PASSWORD"),
    ):
        if os.environ.get(env):
            s[key] = os.environ[env]

    s["enabled"] = bool(s.get("enabled", False))
    s["master_name"] = s.get("master_name") or "mymaster"
    return s


class RedisClient:
    """Redis queue publisher. Mode precedence: sentinel → cluster → standalone."""

    def __init__(self, cfg: dict) -> None:
        prefix = cfg.get("key_prefix", "")
        self._queue = f"{prefix}{cfg['readiness_queue_name']}"
        self._zip_queue = f"{prefix}{cfg['zip_queue_name']}"

        host, port = cfg["host"], int(cfg["port"])
        kwargs = dict(
            username=cfg.get("username") or None,
            password=cfg.get("password") or None,
            decode_responses=True,
        )

        sentinel_cfg = _sentinel_settings(cfg)
        self._sentinel = None

        if sentinel_cfg["enabled"]:
            nodes = _parse_sentinel_hosts(sentinel_cfg.get("hosts"), host)
            master_name = sentinel_cfg["master_name"]
            sentinel_kwargs = {
                k: v
                for k, v in (
                    ("username", sentinel_cfg.get("username") or None),
                    ("password", sentinel_cfg.get("password") or None),
                )
                if v
            }
            logger.info(
                "Connecting to Redis Sentinel %s master=%s", nodes, master_name
            )
            self._sentinel = Sentinel(
                nodes,
                socket_timeout=5,
                sentinel_kwargs=sentinel_kwargs or None,
            )
            self._client = self._sentinel.master_for(
                master_name,
                socket_timeout=5,
                socket_keepalive=True,
                health_check_interval=30,
                **kwargs,
            )
            self._client.ping()
            mode = f"sentinel master={master_name} sentinels={nodes}"
        elif cfg.get("cluster_mode", False):
            self._client = RedisCluster(
                startup_nodes=[ClusterNode(host, port)],
                **kwargs,
            )
            mode = f"cluster host={host} port={port}"
        else:
            self._client = redis.Redis(host=host, port=port, **kwargs)
            mode = f"standalone host={host} port={port}"

        logger.info(
            "Redis client initialised – mode=%s prefix=%r queue=%s zip_queue=%s",
            mode, prefix, self._queue, self._zip_queue,
        )

    def _lpush(self, queue: str, payload: str) -> None:
        """LPUSH with retries so a Sentinel failover does not drop the message."""
        for attempt in range(1, _PUSH_ATTEMPTS + 1):
            try:
                if self._sentinel is not None:
                    # After a failover the old master keeps accepting writes until
                    # Sentinel demotes it, and those writes are discarded on resync.
                    # Ask Sentinel for the current master first: if it changed, the
                    # pool drops its idle connections and reconnects to the new one.
                    self._client.connection_pool.get_master_address()
                self._client.lpush(queue, payload)
                return
            except (*_RETRYABLE_ERRORS, redis.exceptions.ResponseError) as exc:
                # NOREPLICAS: an old master configured with min-replicas-to-write
                # rejects writes once its replica has been promoted away.
                retryable = isinstance(exc, _RETRYABLE_ERRORS) or str(exc).startswith(
                    "NOREPLICAS"
                )
                if not retryable or attempt == _PUSH_ATTEMPTS:
                    raise
                delay = min(_PUSH_BACKOFF_SECONDS * 2 ** (attempt - 1), _PUSH_BACKOFF_MAX_SECONDS)
                logger.warning(
                    "LPUSH to '%s' failed (attempt %d/%d): %s – retrying in %.1fs",
                    queue, attempt, _PUSH_ATTEMPTS, exc, delay,
                )
                time.sleep(delay)

    def _push(self, queue: str, job_type: str, label: str, databank_id: str) -> None:
        message = {
            "jobId": str(uuid.uuid4()),
            "type": job_type,
            "databankId": databank_id,
            "options": {},
            "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        }
        self._lpush(queue, json.dumps(message))
        logger.info(
            "Pushed %s message to '%s' for databankId='%s' (jobId=%s)",
            label, queue, databank_id, message["jobId"],
        )

    def push_readiness_message(self, databank_id: str) -> None:
        self._push(self._queue, "all", "readiness", databank_id)

    def push_zip_message(self, databank_id: str) -> None:
        self._push(self._zip_queue, "zip", "zip", databank_id)

    def close(self) -> None:
        self._client.close()
