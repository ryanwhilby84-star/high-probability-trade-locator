"""Financial Modeling Prep HTTP client — apikey never logged or echoed in errors."""

from __future__ import annotations

import random
import re
import time
from typing import Any
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import requests

from hptl.config import get_fmp_api_key
from hptl.data_sources.fmp_config import load_fmp_provider_config

FMP_ROOT = "https://financialmodelingprep.com"

_KEY_PARAM_RE = re.compile(r"(apikey=)[^&\s\"']+", re.I)
_KEY_JSON_RE = re.compile(r'("apiKey"\s*:\s*")[^"]+(")', re.I)


class FmpApiError(RuntimeError):
    """FMP request failed. Never carries the raw API key."""

    def __init__(
        self,
        message: str,
        *,
        path: str = "",
        status_code: int | None = None,
        note: str = "",
    ) -> None:
        super().__init__(redact_secrets(message))
        self.path = path
        self.status_code = status_code
        self.note = redact_secrets(note)


def redact_secrets(text: str) -> str:
    """Strip API keys from URLs, query strings, or JSON snippets."""
    if not text:
        return text
    out = _KEY_PARAM_RE.sub(r"\1***", text)
    out = _KEY_JSON_RE.sub(r"\1***\2", out)
    return out


def _safe_url(path: str, params: dict[str, str] | None = None) -> str:
    """Build request URL without storing key in a reusable string beyond the call."""
    q = dict(params or {})
    return f"{FMP_ROOT.rstrip('/')}/{path.lstrip('/')}?{urlencode(q)}"


def _redact_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        if "apikey" in qs:
            qs["apikey"] = ["***"]
        flat = []
        for k, vals in qs.items():
            for v in vals:
                flat.append((k, v))
        new_query = urlencode(flat)
        return urlunparse(parsed._replace(query=new_query))
    except Exception:
        return redact_secrets(url)


class FmpClient:
    """Minimal rate-limit-friendly FMP client for audit probes."""

    def __init__(
        self,
        *,
        timeout_seconds: int | None = None,
        max_retries: int | None = None,
        inter_delay_seconds: float | None = None,
    ) -> None:
        cfg = load_fmp_provider_config()
        self._timeout = timeout_seconds if timeout_seconds is not None else cfg.timeout_seconds
        self._max_retries = max_retries if max_retries is not None else cfg.max_retries
        self._inter_delay = inter_delay_seconds if inter_delay_seconds is not None else cfg.inter_request_delay_seconds
        self._last_request_at: float | None = None

    @property
    def configured(self) -> bool:
        return bool(get_fmp_api_key())

    def _throttle(self) -> None:
        if self._inter_delay <= 0 or self._last_request_at is None:
            return
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._inter_delay:
            time.sleep(self._inter_delay - elapsed)

    def get(self, path: str, **params: str) -> Any:
        """GET JSON from FMP. Raises ``FmpApiError`` on failure; never leaks the key."""
        key = get_fmp_api_key()
        if not key:
            raise FmpApiError("FMP_API_KEY not set", path=path)

        q = {k: str(v) for k, v in params.items() if v is not None}
        q["apikey"] = key
        url = _safe_url(path, q)
        safe_path = redact_secrets(path)

        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            self._throttle()
            self._last_request_at = time.monotonic()
            try:
                resp = requests.get(url, timeout=self._timeout)
            except requests.RequestException as exc:
                last_exc = exc
                if attempt + 1 < self._max_retries:
                    time.sleep(min(30.0, (2**attempt) + random.uniform(0, 0.5)))
                    continue
                raise FmpApiError(
                    f"FMP request failed ({type(exc).__name__})",
                    path=safe_path,
                ) from exc

            if resp.status_code == 429 or resp.status_code >= 500:
                last_exc = FmpApiError(
                    f"FMP HTTP {resp.status_code}",
                    path=safe_path,
                    status_code=resp.status_code,
                    note=redact_secrets((resp.text or "")[:300]),
                )
                if attempt + 1 < self._max_retries:
                    time.sleep(min(30.0, (2**attempt) + random.uniform(0.5, 1.5)))
                    continue
                raise last_exc

            if resp.status_code >= 400:
                raise FmpApiError(
                    f"FMP HTTP {resp.status_code}",
                    path=safe_path,
                    status_code=resp.status_code,
                    note=redact_secrets((resp.text or "")[:300]),
                )

            try:
                payload = resp.json()
            except ValueError as exc:
                raise FmpApiError(
                    "FMP non-JSON response",
                    path=safe_path,
                    note=redact_secrets((resp.text or "")[:200]),
                ) from exc

            if isinstance(payload, dict):
                err = payload.get("Error Message") or payload.get("error")
                if err:
                    raise FmpApiError(
                        "FMP error response",
                        path=safe_path,
                        note=redact_secrets(str(err)[:300]),
                    )

            return payload

        raise FmpApiError("FMP request exhausted retries", path=safe_path) from last_exc

    def probe_get(self, path: str, **params: str) -> dict[str, Any]:
        """Safe probe wrapper returning metadata without raw payload bulk."""
        started = time.monotonic()
        try:
            payload = self.get(path, **params)
            elapsed_ms = round((time.monotonic() - started) * 1000, 1)
            return {
                "ok": True,
                "elapsed_ms": elapsed_ms,
                "payload_type": type(payload).__name__,
                "payload": payload,
                "error": None,
            }
        except FmpApiError as exc:
            elapsed_ms = round((time.monotonic() - started) * 1000, 1)
            return {
                "ok": False,
                "elapsed_ms": elapsed_ms,
                "payload_type": None,
                "payload": None,
                "error": str(exc),
                "status_code": exc.status_code,
                "note": exc.note,
            }
