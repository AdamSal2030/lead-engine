from __future__ import annotations
"""Provider-agnostic LLM client.

One entry point — `chat_json(system, user, claude_model=...)` — used by both the
article parser and the daily intelligence job. The active backend is chosen by
settings.LLM_PROVIDER:

  "claude"  → Anthropic Messages API (DEFAULT — unchanged behaviour)
  "ollama"  → Ollama native /api/chat with format=json
              base URL: settings.LLM_BASE_URL or http://localhost:11434
  "openai"  → any OpenAI-compatible /chat/completions endpoint. This covers
              Ollama's OpenAI mode AND hosted open-model providers (Groq,
              Together, OpenRouter, DeepInfra, vLLM, …). base URL must include
              the version path, e.g. https://api.groq.com/openai/v1

When an open-model provider errors or returns nothing, we fall back to Claude
(if ANTHROPIC_API_KEY is set) so pipeline yield never drops on an LLM hiccup.

The function returns the RAW text reply; callers parse JSON themselves (they
already strip markdown fences and validate fields).
"""
import asyncio
import logging
import httpx
from config import settings

log = logging.getLogger("llm")

_anthropic_client = None

# Lazily-built semaphore limiting concurrent open-model calls (CPU-box guardrail).
_open_sem: asyncio.Semaphore | None = None
_open_sem_n: int = -1


def _open_semaphore() -> asyncio.Semaphore | None:
    """Semaphore capping concurrent open-model calls, or None when unlimited."""
    global _open_sem, _open_sem_n
    n = int(getattr(settings, "LLM_MAX_CONCURRENCY", 0) or 0)
    if n <= 0:
        return None
    if _open_sem is None or _open_sem_n != n:
        _open_sem = asyncio.Semaphore(n)
        _open_sem_n = n
    return _open_sem


def _get_anthropic():
    global _anthropic_client
    if not settings.ANTHROPIC_API_KEY:
        return None
    if _anthropic_client is None:
        import anthropic
        _anthropic_client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
    return _anthropic_client


def active_provider() -> str:
    return (settings.LLM_PROVIDER or "claude").strip().lower()


async def _claude_chat(system: str, user: str, model: str, max_tokens: int,
                       timeout: int) -> str | None:
    client = _get_anthropic()
    if not client:
        return None
    kwargs = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": user}],
    }
    if system:
        kwargs["system"] = system
    msg = await client.messages.create(**kwargs)
    return msg.content[0].text.strip()


async def _ollama_chat(system: str, user: str, model: str, max_tokens: int,
                       timeout: int) -> str | None:
    base = (settings.LLM_BASE_URL or "http://localhost:11434").rstrip("/")
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "format": "json",                       # ask Ollama for strict JSON
        "options": {"num_predict": max_tokens, "temperature": 0},
    }
    headers = {}
    if settings.LLM_API_KEY:                     # some Ollama gateways require a key
        headers["Authorization"] = f"Bearer {settings.LLM_API_KEY}"
    async with httpx.AsyncClient(timeout=timeout) as cli:
        r = await cli.post(f"{base}/api/chat", json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    return (data.get("message", {}) or {}).get("content", "").strip() or None


async def _openai_chat(system: str, user: str, model: str, max_tokens: int,
                       timeout: int) -> str | None:
    base = (settings.LLM_BASE_URL or "").rstrip("/")
    if not base:
        log.warning("LLM_PROVIDER=openai but LLM_BASE_URL is empty")
        return None
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0,
        "response_format": {"type": "json_object"},  # JSON mode (widely supported)
    }
    headers = {"Content-Type": "application/json"}
    if settings.LLM_API_KEY:
        headers["Authorization"] = f"Bearer {settings.LLM_API_KEY}"
    async with httpx.AsyncClient(timeout=timeout) as cli:
        r = await cli.post(f"{base}/chat/completions", json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    choices = data.get("choices") or []
    if not choices:
        return None
    return (choices[0].get("message", {}) or {}).get("content", "").strip() or None


async def chat_json(system: str, user: str, *, claude_model: str,
                    max_tokens: int = 300, timeout: int = 60,
                    allow_claude_fallback: bool = True) -> str | None:
    """Return the model's raw text reply (caller parses JSON), or None on failure.

    `claude_model` is used only when the active provider is "claude"; open-model
    providers use settings.LLM_MODEL.
    """
    prov = active_provider()

    if prov == "claude":
        try:
            return await _claude_chat(system, user, claude_model, max_tokens, timeout)
        except Exception as e:
            log.debug(f"claude chat failed: {e}")
            return None

    # Open-model providers (ollama / openai-compatible)
    open_model = (settings.LLM_MODEL or "").strip()
    # Per-call timeout: explicit arg wins, else the configurable LLM_TIMEOUT
    # (lower it for a CPU box so slow calls bail to Claude fast).
    eff_timeout = timeout if timeout != 60 else int(getattr(settings, "LLM_TIMEOUT", 60) or 60)
    if not open_model:
        log.warning(f"LLM_PROVIDER={prov} but LLM_MODEL is empty — using Claude")
    else:
        sem = _open_semaphore()
        try:
            async def _do():
                if prov == "ollama":
                    return await _ollama_chat(system, user, open_model, max_tokens, eff_timeout)
                return await _openai_chat(system, user, open_model, max_tokens, eff_timeout)
            if sem is not None:
                async with sem:
                    out = await _do()
            else:
                out = await _do()
            if out:
                return out
            log.debug(f"LLM provider '{prov}' returned empty output")
        except Exception as e:
            log.warning(f"LLM provider '{prov}' failed ({e}); "
                        f"{'falling back to Claude' if allow_claude_fallback else 'no fallback'}")

    # Fallback so yield never drops when the open model errors
    if allow_claude_fallback and settings.ANTHROPIC_API_KEY:
        try:
            return await _claude_chat(system, user, claude_model, max_tokens, timeout)
        except Exception as e:
            log.debug(f"claude fallback failed: {e}")
    return None
