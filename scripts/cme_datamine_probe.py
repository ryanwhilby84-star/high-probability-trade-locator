"""CME DataMine entitlement/source probe for Institutional Edge long-history research.

This script is intentionally safe to add before credentials exist. It does not
contain secrets and it does not touch production price data.

When CME_DATAMINE_API_ID and CME_DATAMINE_API_PASSWORD are present, it:
  1. obtains an OAuth token;
  2. lists entitled DataMine files;
  3. identifies EOD datasets relevant to the first long-history validation tranche;
  4. prints dataset codes and date/file metadata for acquisition planning.

Credentials stay in environment variables and must never be committed.
"""
from __future__ import annotations

import json
import os
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen

AUTH_URL = "https://auth.cmegroup.com/as/token.oauth2"
LIST_URL = "https://datamine.new.cmegroup.com/api/list_entitlements_files"

# First tranche: old CME FX futures used in the Bernd validation set.
TARGET_TERMS = {
    "Japanese Yen / 6J": ("Japanese Yen", "JPY", "6J"),
    "Canadian Dollar / 6C": ("Canadian Dollar", "CAD", "6C"),
    "Swiss Franc / 6S": ("Swiss Franc", "CHF", "6S"),
}


def request_json(url: str, *, data: bytes | None = None, headers: dict | None = None) -> dict:
    req = Request(url, data=data, headers=headers or {})
    with urlopen(req, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def get_token(api_id: str, password: str) -> str:
    import base64

    auth = base64.b64encode(f"{api_id}:{password}".encode()).decode()
    payload = urlencode({"grant_type": "client_credentials"}).encode()
    result = request_json(
        AUTH_URL,
        data=payload,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "InstitutionalEdge-Research/1.0",
        },
    )
    token = result.get("access_token")
    if not token:
        raise RuntimeError(f"CME token response did not contain access_token: {result}")
    return token


def list_entitlements(token: str) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        query = urlencode({"category_code": "EOD", "limit": 1000, "offset": offset})
        result = request_json(
            f"{LIST_URL}?{query}",
            headers={
                "Authorization": f"Bearer {token}",
                "User-Agent": "InstitutionalEdge-Research/1.0",
            },
        )
        batch = result.get("data") or []
        rows.extend(batch)
        paging = result.get("paging") or {}
        if not paging.get("next") or len(batch) == 0:
            break
        offset += 1000
    return rows


def main() -> int:
    api_id = os.getenv("CME_DATAMINE_API_ID", "").strip()
    password = os.getenv("CME_DATAMINE_API_PASSWORD", "").strip()

    print("=" * 88)
    print("INSTITUTIONAL EDGE — CME DATAMINE LONG-HISTORY ENTITLEMENT PROBE")
    print("=" * 88)
    print("Research only. Production price store is not modified.\n")

    if not api_id or not password:
        print("STATUS: READY — credentials not supplied yet.")
        print("Expected environment variables:")
        print("  CME_DATAMINE_API_ID")
        print("  CME_DATAMINE_API_PASSWORD")
        print("\nNothing else is required until CME access/entitlements are available.")
        return 0

    print("Authenticating with CME DataMine...")
    token = get_token(api_id, password)
    print("Authentication: OK")
    print("Listing entitled EOD datasets...")
    rows = list_entitlements(token)
    print(f"EOD entitlement rows: {len(rows):,}\n")

    haystack = []
    for row in rows:
        text = " ".join(str(row.get(k, "")) for k in ("dataset_code", "dataset_name", "period_date"))
        haystack.append((text.lower(), row))

    found_any = False
    for instrument, terms in TARGET_TERMS.items():
        print("-" * 88)
        print(instrument)
        matches = []
        for text, row in haystack:
            if any(term.lower() in text for term in terms):
                matches.append(row)
        if not matches:
            print("  No matching entitled EOD dataset found.")
            continue
        found_any = True
        for row in matches[:20]:
            files = row.get("files") or []
            print(
                f"  dataset={row.get('dataset_code')} | name={row.get('dataset_name')} | "
                f"date={row.get('period_date')} | files={len(files)}"
            )
            for f in files[:3]:
                print(f"    {f.get('file_id')} | {f.get('file_name')}")

    print("\n" + "=" * 88)
    if found_any:
        print("RESULT: CME access detected. Dataset codes above can feed the automated downloader next.")
    else:
        print("RESULT: API works, but the first-tranche EOD datasets are not currently entitled.")
    print("=" * 88)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
