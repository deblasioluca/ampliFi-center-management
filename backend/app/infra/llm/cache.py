"""Redis-backed LLM response cache (§05.12).

Wraps any LLMProvider and caches completions by prompt hash.
Identical prompts (same model, messages, temperature) return cached
results instantly, avoiding duplicate API calls and costs.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import TYPE_CHECKING

import structlog

from app.infra.llm.provider import Completion, _prompt_hash

if TYPE_CHECKING:
    from app.infra.llm.provider import LLMProvider, Message

logger = structlog.get_logger()

# Default TTL: 7 days
_DEFAULT_TTL = 7 * 24 * 3600


class CachedLLMProvider:
    """Wrapper that caches LLM completions in Redis."""

    def __init__(
        self,
        inner: LLMProvider,
        redis_url: str = "redis://localhost:6380/3",
        ttl: int = _DEFAULT_TTL,
        prefix: str = "llm:",
    ) -> None:
        self._inner = inner
        self._ttl = ttl
        self._prefix = prefix
        self._redis = None
        self._redis_url = redis_url

    def _get_redis(self):
        if self._redis is None:
            try:
                import redis

                self._redis = redis.from_url(self._redis_url, decode_responses=True)
                self._redis.ping()
            except Exception as exc:
                logger.warning("llm.cache.redis_unavailable", error=str(exc))
                self._redis = None
        return self._redis

    @property
    def name(self) -> str:
        return f"cached:{self._inner.name}"

    def complete(
        self,
        model: str,
        messages: list[Message],
        temperature: float = 0.0,
        max_tokens: int = 2000,
        metadata: dict | None = None,
    ) -> Completion:
        phash = _prompt_hash(model, messages, temperature)
        cache_key = f"{self._prefix}{phash}"

        r = self._get_redis()
        if r is not None:
            try:
                cached_raw = r.get(cache_key)
                if cached_raw:
                    data = json.loads(cached_raw)
                    logger.debug("llm.cache.hit", hash=phash[:12])
                    return Completion(
                        text=data["text"],
                        model=data.get("model", model),
                        tokens_in=data.get("tokens_in", 0),
                        tokens_out=data.get("tokens_out", 0),
                        cost_usd=0.0,
                        latency_ms=0,
                        cached=True,
                        prompt_hash=phash,
                        metadata=data.get("metadata", {}),
                    )
            except Exception as exc:
                logger.warning("llm.cache.read_error", error=str(exc))

        completion = self._inner.complete(model, messages, temperature, max_tokens, metadata)

        if r is not None and not completion.metadata.get("error"):
            try:
                payload = json.dumps(asdict(completion))
                r.setex(cache_key, self._ttl, payload)
                logger.debug("llm.cache.store", hash=phash[:12])
            except Exception as exc:
                logger.warning("llm.cache.write_error", error=str(exc))

        return completion

    def estimate_cost(self, completion: Completion) -> float:
        if completion.cached:
            return 0.0
        return self._inner.estimate_cost(completion)

    def invalidate(self, model: str, messages: list[Message], temperature: float = 0.0) -> bool:
        """Remove a specific cached entry."""
        phash = _prompt_hash(model, messages, temperature)
        r = self._get_redis()
        if r is not None:
            return bool(r.delete(f"{self._prefix}{phash}"))
        return False

    def flush_all(self) -> int:
        """Remove all LLM cache entries."""
        r = self._get_redis()
        if r is None:
            return 0
        keys = r.keys(f"{self._prefix}*")
        if keys:
            return r.delete(*keys)
        return 0

    def stats(self) -> dict:
        """Return cache statistics."""
        r = self._get_redis()
        if r is None:
            return {"available": False}
        keys = r.keys(f"{self._prefix}*")
        return {
            "available": True,
            "entries": len(keys),
            "prefix": self._prefix,
            "ttl_seconds": self._ttl,
        }
