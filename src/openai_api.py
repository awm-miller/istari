from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib import error, request


@dataclass(slots=True)
class OpenAIResponsesClient:
    api_key: str
    base_url: str
    cache_dir: Path
    user_agent: str

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def create_response(
        self,
        *,
        model: str,
        input_text: str,
        tools: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": model,
            "input": input_text,
        }
        if tools:
            payload["tools"] = tools
        if metadata:
            payload["metadata"] = metadata

        cache_key = sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
        cache_path = self.cache_dir / f"{cache_key}.json"
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))

        response = self._post_json(f"{self.base_url}/responses", payload)
        cache_path.write_text(json.dumps(response, indent=2), encoding="utf-8")
        return response

    def _post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url=url,
            data=data,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "User-Agent": self.user_agent,
            },
        )
        try:
            with request.urlopen(req) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"OpenAI request failed: {exc.code} {body}") from exc


def extract_output_text(response: dict[str, Any]) -> str:
    if response.get("output_text"):
        return str(response["output_text"])

    parts: list[str] = []
    for item in response.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                parts.append(content.get("text", ""))
    return "\n".join(part for part in parts if part)


def extract_json_document(text: str) -> Any:
    text = text.strip()
    if not text:
        raise ValueError("Expected JSON output but received empty text.")

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    start_candidates = [idx for idx in (text.find("{"), text.find("[")) if idx >= 0]
    if not start_candidates:
        raise ValueError(f"Could not locate JSON payload in response: {text}")
    start = min(start_candidates)

    end_curly = text.rfind("}")
    end_square = text.rfind("]")
    end = max(end_curly, end_square)
    if end < start:
        raise ValueError(f"Could not locate JSON end in response: {text}")

    return json.loads(text[start : end + 1])
