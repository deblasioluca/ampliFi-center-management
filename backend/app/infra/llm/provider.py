"""LLM provider abstraction (§05.9).

Defines the LLMProvider protocol and concrete implementations for
Azure OpenAI and SAP BTP GenAI Hub.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Protocol

import structlog

logger = structlog.get_logger()


@dataclass
class Message:
    role: str  # system | user | assistant
    content: str


@dataclass
class Completion:
    text: str
    model: str
    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0
    cached: bool = False
    prompt_hash: str = ""
    metadata: dict = field(default_factory=dict)


class LLMProvider(Protocol):
    """Protocol for LLM providers (§05.9)."""

    @property
    def name(self) -> str: ...

    def complete(
        self,
        model: str,
        messages: list[Message],
        temperature: float = 0.0,
        max_tokens: int = 2000,
        metadata: dict | None = None,
    ) -> Completion: ...

    def estimate_cost(self, completion: Completion) -> float: ...


def _prompt_hash(model: str, messages: list[Message], temperature: float) -> str:
    """SHA-256 of (model, messages, temperature) for cache keying."""
    payload = json.dumps(
        {
            "model": model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": temperature,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


class AzureOpenAIProvider:
    """Azure OpenAI provider implementation."""

    def __init__(self, config: dict) -> None:
        self._config = config
        self._endpoint = config.get("endpoint", "")
        self._api_key = config.get("api_key", "")
        self._api_version = config.get("api_version", "2024-06-01")
        self._deployment = config.get("deployment", "")

    @property
    def name(self) -> str:
        return "azure"

    def complete(
        self,
        model: str,
        messages: list[Message],
        temperature: float = 0.0,
        max_tokens: int = 2000,
        metadata: dict | None = None,
    ) -> Completion:
        phash = _prompt_hash(model, messages, temperature)
        start = time.monotonic()

        try:
            import httpx

            url = (
                f"{self._endpoint}/openai/deployments/{self._deployment or model}/chat/completions"
            )
            headers = {
                "api-key": self._api_key,
                "Content-Type": "application/json",
            }
            body = {
                "messages": [{"role": m.role, "content": m.content} for m in messages],
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            resp = httpx.post(
                url,
                headers=headers,
                json=body,
                params={"api-version": self._api_version},
                timeout=120.0,
            )
            resp.raise_for_status()
            data = resp.json()

            text = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            tokens_in = usage.get("prompt_tokens", 0)
            tokens_out = usage.get("completion_tokens", 0)
            latency = int((time.monotonic() - start) * 1000)

            completion = Completion(
                text=text,
                model=model,
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                latency_ms=latency,
                prompt_hash=phash,
                metadata=metadata or {},
            )
            completion.cost_usd = self.estimate_cost(completion)
            return completion

        except Exception as e:
            logger.error("llm.azure.error", error=str(e), model=model)
            return Completion(
                text=f"[LLM Error: {e}]",
                model=model,
                prompt_hash=phash,
                metadata={"error": str(e)},
            )

    def estimate_cost(self, completion: Completion) -> float:
        # GPT-4o pricing (approximate)
        rate_in = 0.005 / 1000  # $5/1M input tokens
        rate_out = 0.015 / 1000  # $15/1M output tokens
        return completion.tokens_in * rate_in + completion.tokens_out * rate_out


class SapBtpProvider:
    """SAP BTP GenAI Hub provider implementation."""

    def __init__(self, config: dict) -> None:
        self._config = config
        self._base_url = config.get("base_url", "")
        self._client_id = config.get("client_id", "")
        self._client_secret = config.get("client_secret", "")
        self._token_url = config.get("token_url", "")

    @property
    def name(self) -> str:
        return "btp"

    def complete(
        self,
        model: str,
        messages: list[Message],
        temperature: float = 0.0,
        max_tokens: int = 2000,
        metadata: dict | None = None,
    ) -> Completion:
        phash = _prompt_hash(model, messages, temperature)
        start = time.monotonic()

        try:
            import httpx

            # Get OAuth token
            token_resp = httpx.post(
                self._token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
                timeout=30.0,
            )
            token_resp.raise_for_status()
            access_token = token_resp.json()["access_token"]

            # Call GenAI Hub
            url = f"{self._base_url}/api/v1/completions"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            body = {
                "model": model,
                "messages": [{"role": m.role, "content": m.content} for m in messages],
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            resp = httpx.post(url, headers=headers, json=body, timeout=120.0)
            resp.raise_for_status()
            data = resp.json()

            text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            usage = data.get("usage", {})
            latency = int((time.monotonic() - start) * 1000)

            completion = Completion(
                text=text,
                model=model,
                tokens_in=usage.get("prompt_tokens", 0),
                tokens_out=usage.get("completion_tokens", 0),
                latency_ms=latency,
                prompt_hash=phash,
                metadata=metadata or {},
            )
            completion.cost_usd = self.estimate_cost(completion)
            return completion

        except Exception as e:
            logger.error("llm.btp.error", error=str(e), model=model)
            return Completion(
                text=f"[LLM Error: {e}]",
                model=model,
                prompt_hash=phash,
                metadata={"error": str(e)},
            )

    def estimate_cost(self, completion: Completion) -> float:
        rate_in = 0.003 / 1000
        rate_out = 0.010 / 1000
        return completion.tokens_in * rate_in + completion.tokens_out * rate_out


class OpenAIProvider:
    """Standard OpenAI API provider."""

    def __init__(self, config: dict) -> None:
        self._config = config
        self._api_key = config.get("api_key", "")
        self._model = config.get("model", "gpt-4o")
        self._endpoint = config.get("endpoint", "https://api.openai.com/v1")

    @property
    def name(self) -> str:
        return "openai"

    def complete(
        self,
        model: str,
        messages: list[Message],
        temperature: float = 0.0,
        max_tokens: int = 2000,
        metadata: dict | None = None,
    ) -> Completion:
        phash = _prompt_hash(model, messages, temperature)
        start = time.monotonic()
        use_model = model or self._model

        try:
            import httpx

            url = f"{self._endpoint.rstrip('/')}/chat/completions"
            headers = {
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            }
            body = {
                "model": use_model,
                "messages": [{"role": m.role, "content": m.content} for m in messages],
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            resp = httpx.post(url, headers=headers, json=body, timeout=120.0)
            resp.raise_for_status()
            data = resp.json()

            text = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            latency = int((time.monotonic() - start) * 1000)

            completion = Completion(
                text=text,
                model=use_model,
                tokens_in=usage.get("prompt_tokens", 0),
                tokens_out=usage.get("completion_tokens", 0),
                latency_ms=latency,
                prompt_hash=phash,
                metadata=metadata or {},
            )
            completion.cost_usd = self.estimate_cost(completion)
            return completion

        except Exception as e:
            logger.error("llm.openai.error", error=str(e), model=use_model)
            return Completion(
                text=f"[LLM Error: {e}]",
                model=use_model,
                prompt_hash=phash,
                metadata={"error": str(e)},
            )

    def estimate_cost(self, completion: Completion) -> float:
        rate_in = 0.0025 / 1000
        rate_out = 0.010 / 1000
        return completion.tokens_in * rate_in + completion.tokens_out * rate_out


class AnthropicProvider:
    """Anthropic Claude API provider."""

    def __init__(self, config: dict) -> None:
        self._config = config
        self._api_key = config.get("api_key", "")
        self._model = config.get("model", "claude-sonnet-4-20250514")
        self._endpoint = config.get("endpoint", "https://api.anthropic.com")

    @property
    def name(self) -> str:
        return "anthropic"

    def complete(
        self,
        model: str,
        messages: list[Message],
        temperature: float = 0.0,
        max_tokens: int = 2000,
        metadata: dict | None = None,
    ) -> Completion:
        phash = _prompt_hash(model, messages, temperature)
        start = time.monotonic()
        use_model = model or self._model

        try:
            import httpx

            url = f"{self._endpoint.rstrip('/')}/v1/messages"
            headers = {
                "x-api-key": self._api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            }
            # Anthropic: system message is separate, not in messages array
            system_text = ""
            user_messages = []
            for m in messages:
                if m.role == "system":
                    system_text += m.content + "\n"
                else:
                    user_messages.append({"role": m.role, "content": m.content})
            if not user_messages:
                user_messages = [{"role": "user", "content": "Hello"}]

            body: dict = {
                "model": use_model,
                "messages": user_messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
            }
            if system_text.strip():
                body["system"] = system_text.strip()

            resp = httpx.post(url, headers=headers, json=body, timeout=120.0)
            resp.raise_for_status()
            data = resp.json()

            text = ""
            for block in data.get("content", []):
                if block.get("type") == "text":
                    text += block.get("text", "")
            usage = data.get("usage", {})
            latency = int((time.monotonic() - start) * 1000)

            completion = Completion(
                text=text,
                model=use_model,
                tokens_in=usage.get("input_tokens", 0),
                tokens_out=usage.get("output_tokens", 0),
                latency_ms=latency,
                prompt_hash=phash,
                metadata=metadata or {},
            )
            completion.cost_usd = self.estimate_cost(completion)
            return completion

        except Exception as e:
            logger.error("llm.anthropic.error", error=str(e), model=use_model)
            return Completion(
                text=f"[LLM Error: {e}]",
                model=use_model,
                prompt_hash=phash,
                metadata={"error": str(e)},
            )

    def estimate_cost(self, completion: Completion) -> float:
        rate_in = 0.003 / 1000
        rate_out = 0.015 / 1000
        return completion.tokens_in * rate_in + completion.tokens_out * rate_out


def get_active_provider_config(db: object) -> dict | None:
    """Look up the active LLM provider from app_config (llm_providers key).

    Returns a config dict ready for ``get_provider``, or *None* if nothing
    is configured.  ``db`` is a SQLAlchemy Session.
    """
    from sqlalchemy import select as sa_select

    from app.models.core import AppConfig

    cfg = db.execute(
        sa_select(AppConfig).where(AppConfig.key == "llm_providers")
    ).scalar_one_or_none()
    providers = (cfg.value if cfg else None) or []
    if isinstance(providers, dict):
        providers = [providers] if providers else []
    active = next((p for p in providers if p.get("is_active")), None)
    if not active and providers:
        active = providers[0]
    if not active:
        return None
    return {
        "provider": active.get("provider_type", "openai"),
        "api_key": active.get("api_key", ""),
        "model": active.get("model", ""),
        "endpoint": active.get("endpoint", ""),
        "deployment": active.get("model", ""),
    }


def get_provider(config: dict, cache: bool = True) -> LLMProvider:
    """Factory: create an LLM provider from config.

    If cache=True and redis_url is configured, wraps the provider with
    a Redis-backed response cache to avoid duplicate API calls.
    """
    provider_type = config.get("provider", "azure")
    if provider_type == "azure" or provider_type == "azure_openai":
        inner: LLMProvider = AzureOpenAIProvider(config)
    elif provider_type == "btp":
        inner = SapBtpProvider(config)
    elif provider_type == "openai":
        inner = OpenAIProvider(config)
    elif provider_type == "anthropic":
        inner = AnthropicProvider(config)
    else:
        raise ValueError(f"Unknown LLM provider: {provider_type}")

    if cache and config.get("cache_enabled", True):
        from app.infra.llm.cache import CachedLLMProvider

        redis_url = config.get("cache_redis_url", "redis://localhost:6380/3")
        ttl = config.get("cache_ttl", 7 * 24 * 3600)
        return CachedLLMProvider(inner, redis_url=redis_url, ttl=ttl)

    return inner
