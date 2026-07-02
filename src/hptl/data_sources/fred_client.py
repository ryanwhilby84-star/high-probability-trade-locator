"""FRED Web Services client for audit-only macro blueprint probes.

Separate from ``hptl.macro.fred_client`` (rates cache layer). Never logs API keys.
"""

from __future__ import annotations

import random
import re
import time
from typing import Any
from urllib.parse import urlencode

import requests

from hptl.config import get_fred_api_key, get_settings

FRED_API_ROOT = "https://api.stlouisfed.org/fred/"

_KEY_PARAM_RE = re.compile(r"(api_key=)[^&\s\"']+", re.I)


class FredApiError(RuntimeError):
    """FRED request failed without exposing the API key."""

    def __init__(
        self,
        message: str,
        *,
        endpoint: str = "",
        series_id: str = "",
        status_code: int | None = None,
        note: str = "",
    ) -> None:
        super().__init__(redact_secrets(message))
        self.endpoint = endpoint
        self.series_id = series_id
        self.status_code = status_code
        self.note = redact_secrets(note)


def redact_secrets(text: str) -> str:
    if not text:
        return text
    return _KEY_PARAM_RE.sub(r"\1***", text)


class FredAuditClient:
    """Minimal FRED API client for macro blueprint audits."""

    def __init__(
        self,
        *,
        timeout_seconds: int | None = None,
        max_retries: int = 3,
        inter_delay_seconds: float = 0.25,
    ) -> None:
        settings = get_settings()
        self._timeout = timeout_seconds if timeout_seconds is not None else settings.request_timeout_seconds
        self._max_retries = max(1, max_retries)
        self._inter_delay = max(0.0, inter_delay_seconds)
        self._last_request_at: float | None = None

    @property
    def configured(self) -> bool:
        return bool(get_fred_api_key())

    def _throttle(self) -> None:
        if self._inter_delay <= 0 or self._last_request_at is None:
            return
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._inter_delay:
            time.sleep(self._inter_delay - elapsed)

    def _get(self, endpoint: str, **params: str) -> dict[str, Any]:
        key = get_fred_api_key()
        if not key:
            raise FredApiError("FRED_API_KEY not set", endpoint=endpoint)

        q = {k: str(v) for k, v in params.items() if v is not None}
        q["api_key"] = key
        q.setdefault("file_type", "json")
        url = f"{FRED_API_ROOT}{endpoint}?{urlencode(q)}"

        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            self._throttle()
            self._last_request_at = time.monotonic()
            try:
                resp = requests.get(url, timeout=self._timeout)
            except requests.RequestException as exc:
                last_exc = exc
                if attempt + 1 < self._max_retries:
                    time.sleep(min(20.0, (2**attempt) + random.uniform(0, 0.4)))
                    continue
                raise FredApiError(
                    f"FRED request failed ({type(exc).__name__})",
                    endpoint=endpoint,
                    series_id=str(params.get("series_id") or ""),
                ) from exc

            if resp.status_code in (429, 500, 502, 503, 504):
                last_exc = FredApiError(
                    f"FRED HTTP {resp.status_code}",
                    endpoint=endpoint,
                    series_id=str(params.get("series_id") or ""),
                    status_code=resp.status_code,
                    note=redact_secrets((resp.text or "")[:300]),
                )
                if attempt + 1 < self._max_retries:
                    time.sleep(min(20.0, (2**attempt) + random.uniform(0.5, 1.0)))
                    continue
                raise last_exc

            if resp.status_code >= 400:
                raise FredApiError(
                    f"FRED HTTP {resp.status_code}",
                    endpoint=endpoint,
                    series_id=str(params.get("series_id") or ""),
                    status_code=resp.status_code,
                    note=redact_secrets((resp.text or "")[:300]),
                )

            try:
                payload = resp.json()
            except ValueError as exc:
                raise FredApiError(
                    "FRED non-JSON response",
                    endpoint=endpoint,
                    series_id=str(params.get("series_id") or ""),
                    note=redact_secrets((resp.text or "")[:200]),
                ) from exc

            if not isinstance(payload, dict):
                raise FredApiError(
                    "FRED unexpected payload type",
                    endpoint=endpoint,
                    series_id=str(params.get("series_id") or ""),
                )
            if payload.get("error_code"):
                raise FredApiError(
                    f"FRED API error {payload.get('error_code')}",
                    endpoint=endpoint,
                    series_id=str(params.get("series_id") or ""),
                    note=redact_secrets(str(payload.get("error_message") or "")[:300]),
                )
            return payload

        raise FredApiError(
            "FRED request exhausted retries",
            endpoint=endpoint,
            series_id=str(params.get("series_id") or ""),
        ) from last_exc

    def fetch_series_metadata(self, series_id: str) -> dict[str, Any]:
        payload = self._get("series", series_id=series_id)
        rows = payload.get("seriess") or []
        if not rows or not isinstance(rows[0], dict):
            raise FredApiError(
                f"No metadata for series {series_id}",
                endpoint="series",
                series_id=series_id,
            )
        return rows[0]

    def fetch_observations(
        self,
        series_id: str,
        *,
        sort_order: str = "desc",
        limit: int | None = None,
        observation_start: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {"series_id": series_id, "sort_order": sort_order}
        if limit is not None:
            params["limit"] = str(limit)
        if observation_start:
            params["observation_start"] = observation_start
        payload = self._get("series/observations", **params)
        obs = payload.get("observations")
        if not isinstance(obs, list):
            raise FredApiError(
                "Missing observations array",
                endpoint="series/observations",
                series_id=series_id,
            )
        return [o for o in obs if isinstance(o, dict)]

    def probe_series(self, series_id: str, *, tail_limit: int = 12) -> dict[str, Any]:
        """Fetch metadata + observation summary for audit reporting."""
        started = time.monotonic()
        try:
            meta = self.fetch_series_metadata(series_id)
            tail = self.fetch_observations(series_id, sort_order="desc", limit=tail_limit)
            # Full depth: ascending fetch with high limit (FRED default cap 100k)
            all_obs = self.fetch_observations(series_id, sort_order="asc", limit=100000)
            elapsed_ms = round((time.monotonic() - started) * 1000, 1)
            return {
                "ok": True,
                "elapsed_ms": elapsed_ms,
                "metadata": meta,
                "tail_observations": tail,
                "all_observations": all_obs,
                "error": None,
            }
        except FredApiError as exc:
            elapsed_ms = round((time.monotonic() - started) * 1000, 1)
            return {
                "ok": False,
                "elapsed_ms": elapsed_ms,
                "metadata": None,
                "tail_observations": [],
                "all_observations": [],
                "error": str(exc),
                "status_code": exc.status_code,
                "note": exc.note,
            }
