"""OANDA v20 REST API — account instruments (read-only)."""

from __future__ import annotations

from typing import Any

import requests

from hptl.config import get_oanda_account_id, get_oanda_api_host, get_oanda_api_key, get_settings

OANDA_PRACTICE_HOST = "https://api-fxpractice.oanda.com"
OANDA_LIVE_HOST = "https://api-fxtrade.oanda.com"


class OandaApiError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, body: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def _session() -> requests.Session:
    key = get_oanda_api_key()
    if not key:
        raise OandaApiError(
            "OANDA_API_KEY not set — add your personal access token to .env (see .env.example)."
        )
    s = requests.Session()
    s.headers.update(
        {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }
    )
    return s


def api_get(path: str, *, params: dict[str, str] | None = None) -> dict[str, Any]:
    host = get_oanda_api_host().rstrip("/")
    url = f"{host}{path}"
    timeout = get_settings().request_timeout_seconds
    try:
        r = _session().get(url, params=params, timeout=timeout)
    except requests.RequestException as exc:
        raise OandaApiError(f"OANDA request failed: {type(exc).__name__}: {exc}") from exc
    if r.status_code >= 400:
        raise OandaApiError(
            f"OANDA HTTP {r.status_code} for {path}",
            status_code=r.status_code,
            body=(r.text or "")[:2000],
        )
    try:
        return r.json()
    except ValueError as exc:
        raise OandaApiError(f"OANDA response not JSON for {path}") from exc


def list_accounts() -> list[dict[str, Any]]:
    doc = api_get("/v3/accounts")
    accounts = doc.get("accounts") or []
    if not isinstance(accounts, list):
        raise OandaApiError("Unexpected OANDA accounts payload")
    return accounts


def resolve_account_id() -> str:
    explicit = get_oanda_account_id()
    if explicit:
        return explicit
    accounts = list_accounts()
    if not accounts:
        raise OandaApiError("No OANDA accounts returned for this API token.")
    aid = str(accounts[0].get("id") or "").strip()
    if not aid:
        raise OandaApiError("First OANDA account has no id.")
    return aid


def fetch_account_instruments(account_id: str | None = None) -> list[dict[str, Any]]:
    """Return tradeable instruments for the account (OANDA v20 ``/instruments``)."""
    aid = account_id or resolve_account_id()
    doc = api_get(f"/v3/accounts/{aid}/instruments")
    instruments = doc.get("instruments") or []
    if not isinstance(instruments, list):
        raise OandaApiError("Unexpected OANDA instruments payload")
    return instruments


def instrument_names_set(instruments: list[dict[str, Any]]) -> set[str]:
    return {str(i.get("name") or "").strip() for i in instruments if i.get("name")}


def instruments_by_name(instruments: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in instruments:
        name = str(row.get("name") or "").strip()
        if name:
            out[name] = row
    return out
