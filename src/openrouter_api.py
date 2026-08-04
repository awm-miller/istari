from __future__ import annotations

import json
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib import error, request


@dataclass(slots=True)
class OpenRouterChatClient:
    api_key: str
    base_url: str
    user_agent: str
    cache_dir: Path | None = None
    timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.cache_dir is not None:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def create_chat_completion(
        self,
        *,
        model: str,
        messages: list[dict[str, str]],
        response_format: dict[str, str] | None = None,
        reasoning: dict[str, str] | None = None,
        temperature: float = 0.0,
    ) -> dict[str, Any]:
        if not model.strip():
            raise ValueError("OpenRouter model is required.")
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": False,
            "provider": {"data_collection": "deny", "zdr": True},
        }
        if response_format:
            payload["response_format"] = response_format
        if reasoning:
            payload["reasoning"] = reasoning

        cache_path = self._cache_path(payload)
        if cache_path is not None and cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                cached = None
            if isinstance(cached, dict) and not _completion_error(cached):
                return cached

        response = self._post_json(payload)
        if cache_path is not None:
            cache_path.write_text(json.dumps(response, indent=2), encoding="utf-8")
        return response

    def _cache_path(self, payload: dict[str, Any]) -> Path | None:
        if self.cache_dir is None:
            return None
        cache_key = sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
        return self.cache_dir / f"{cache_key}.json"

    def _post_json(self, payload: dict[str, Any]) -> dict[str, Any]:
        req = request.Request(
            url=f"{self.base_url.rstrip('/')}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "User-Agent": self.user_agent,
                "HTTP-Referer": "https://projectistari.netlify.app",
                "X-Title": "Project Istari",
            },
        )
        delays = (0.0, 2.0, 5.0, 10.0, 20.0)
        for attempt, delay in enumerate(delays):
            if delay:
                time.sleep(delay)
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    decoded = json.loads(response.read().decode("utf-8"))
                    response_payload = decoded if isinstance(decoded, dict) else {}
                    response_error = _completion_error(response_payload)
                    if response_error and attempt < len(delays) - 1:
                        continue
                    if response_error:
                        raise RuntimeError(f"OpenRouter request failed: {response_error}")
                    return response_payload
            except error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                retryable = exc.code == 429 or exc.code >= 500
                if retryable and attempt < len(delays) - 1:
                    continue
                raise RuntimeError(f"OpenRouter request failed: HTTP {exc.code} {body[:500]}") from exc
            except error.URLError as exc:
                if attempt < len(delays) - 1:
                    continue
                raise RuntimeError(f"OpenRouter request failed: {exc}") from exc
        raise RuntimeError("OpenRouter request failed after retries.")


def _completion_error(response: dict[str, Any]) -> str:
    provider_error = response.get("error")
    if isinstance(provider_error, dict):
        return str(provider_error.get("message") or provider_error.get("code") or "provider error")
    if provider_error:
        return str(provider_error)
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        return "response did not include a completion choice"
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        return "response did not include completion content"
    return ""


def extract_chat_content(response: dict[str, Any]) -> str:
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("OpenRouter response did not include a completion choice.")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise ValueError("OpenRouter response did not include JSON content.")
    return content
