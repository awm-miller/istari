from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib import error, request

log = logging.getLogger("istari.gemini")


@dataclass(slots=True)
class GeminiClient:
    api_key: str
    cache_dir: Path
    base_url: str = "https://generativelanguage.googleapis.com/v1beta"
    timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        *,
        model: str,
        prompt: str,
        temperature: float = 0.2,
    ) -> dict[str, Any]:
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": temperature},
        }

        cache_key = sha256(
            json.dumps({"model": model, **payload}, sort_keys=True).encode()
        ).hexdigest()
        cache_path = self.cache_dir / f"{cache_key}.json"
        if cache_path.exists():
            log.debug("Gemini cache hit: %s", cache_key[:12])
            try:
                return json.loads(cache_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                # Corrupted cache file can happen after interrupted/concurrent runs.
                # Drop and refresh from live API instead of crashing the whole pipeline.
                log.warning("Gemini cache corrupted; deleting %s", cache_path.name)
                try:
                    cache_path.unlink(missing_ok=True)
                except Exception:
                    pass

        log.debug("Gemini API call: model=%s prompt_len=%d", model, len(prompt))
        url = f"{self.base_url}/models/{model}:generateContent?key={self.api_key}"
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url=url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        attempts = 3
        backoff_seconds = (1.0, 2.0, 4.0)
        last_error: RuntimeError | None = None
        for attempt in range(attempts):
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                    result = json.loads(resp.read().decode("utf-8"))
                    break
            except error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                last_error = RuntimeError(f"Gemini request failed: {exc.code} {body}")
                if exc.code in {429, 500, 502, 503, 504} and attempt < attempts - 1:
                    wait = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]
                    log.warning(
                        "Gemini transient error %s, retrying in %.1fs (attempt %d/%d)",
                        exc.code,
                        wait,
                        attempt + 1,
                        attempts,
                    )
                    time.sleep(wait)
                    continue
                raise last_error from exc
            except Exception as exc:
                last_error = RuntimeError(f"Gemini request failed: {exc}")
                if attempt < attempts - 1:
                    wait = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]
                    log.warning(
                        "Gemini request error, retrying in %.1fs (attempt %d/%d): %s",
                        wait,
                        attempt + 1,
                        attempts,
                        exc,
                    )
                    time.sleep(wait)
                    continue
                raise last_error from exc
        else:
            raise last_error or RuntimeError("Gemini request failed with unknown error")

        temp_path = cache_path.with_suffix(f"{cache_path.suffix}.tmp")
        temp_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        temp_path.replace(cache_path)
        return result


def extract_gemini_text(response: dict[str, Any]) -> str:
    for candidate in response.get("candidates", []):
        content = candidate.get("content", {})
        for part in content.get("parts", []):
            text = part.get("text")
            if text:
                return text
    return ""
