"""Priority board + per-instrument debug for the full expanded universe."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.context.attention_engine import (
    PRIORITY_DEVELOPING,
    PRIORITY_HIGH,
    PRIORITY_LABELS,
    PRIORITY_LOW,
    PRIORITY_WATCHLIST,
)
from hptl.context.macro_only_context import build_macro_only_attention
from hptl.markets.instrument_registry import (
    all_instrument_ids,
    canonical_priority_group,
    get_instrument,
    load_registry,
)

PRIORITY_DEBUG_PATH = Path("data/priority_debug_latest.json")
PUBLIC_PRIORITY_DEBUG_PATH = Path("web-dashboard/public/data/priority_debug_latest.json")

BOARD_SCORE_FLOOR = 14.0
AUDIT_TOP_N = 30

# Lower number = higher preference on the priority board.
ELIG_COT_MACRO = 0
ELIG_COT_ONLY = 1
ELIG_PROXY_MACRO = 2
ELIG_PROXY_ONLY = 3
ELIG_MACRO_ONLY = 4
ELIG_NONE = 9

ELIG_LABELS = {
    ELIG_COT_MACRO: "direct_cot_and_macro",
    ELIG_COT_ONLY: "direct_cot_only",
    ELIG_PROXY_MACRO: "proxy_cot_and_macro",
    ELIG_PROXY_ONLY: "proxy_cot_only",
    ELIG_MACRO_ONLY: "macro_only",
    ELIG_NONE: "not_eligible",
}

ALERT_ANOMALY_WEIGHTS = {
    "flow_extreme": 28,
    "transition": 26,
    "exhaustion": 24,
    "deterioration": 22,
    "tension": 18,
    "opportunity": 20,
    "macro": 16,
    "flow": 14,
    "participation": 20,
    "proxy": 4,
    "data": 2,
}


def _cot_resolved(rec: dict[str, Any]) -> bool:
    bias = str(rec.get("cot_bias") or "").strip().upper()
    if not bias or bias == "N/A":
        return False
    if "no mapped raw COT" in str(rec.get("missing_reason") or ""):
        return False
    return True


def _has_macro_transmission(rec: dict[str, Any]) -> bool:
    tx = rec.get("macro_transmission")
    if not isinstance(tx, dict):
        tx = (rec.get("institutional_context") or {}).get("macro_transmission")
    return isinstance(tx, dict) and tx.get("available") is True


def _macro_generic_only(rec: dict[str, Any] | None) -> bool:
    if not rec:
        return True
    tx = rec.get("macro_transmission") or (rec.get("institutional_context") or {}).get("macro_transmission") or {}
    return bool(tx.get("generic_rates_only"))


def _synthesize_macro_attention(rec: dict[str, Any]) -> dict[str, Any] | None:
    spec = get_instrument(str(rec.get("market") or ""))
    if not spec:
        return None
    tx = rec.get("macro_transmission") or (rec.get("institutional_context") or {}).get("macro_transmission")
    if not isinstance(tx, dict) or not tx.get("available"):
        return None
    macro_signal = str(rec.get("macro_regime") or tx.get("rates_snapshot", {}).get("macro_signal") or "neutral")
    if macro_signal == "N/A":
        macro_signal = "neutral"
    return build_macro_only_attention(
        market=spec.id,
        spec=spec,
        macro_transmission=tx,
        macro_signal=macro_signal,
    )


def resolve_attention_for_record(
    rec: dict[str, Any],
    week_by_market: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    inst = rec.get("institutional_context") or {}
    att = inst.get("attention")
    if att:
        return att, "institutional_context"
    syn = _synthesize_macro_attention(rec)
    if syn:
        return syn, "synthesized_macro_transmission"
    return None, "no_attention_source"


def classify_eligibility_tier(
    *,
    spec: Any,
    rec: dict[str, Any] | None,
    cot_resolved: bool,
    has_macro: bool,
    macro_generic: bool,
) -> int:
    if rec is None or spec is None:
        return ELIG_NONE
    if cot_resolved and spec.has_cot_mapping:
        return ELIG_COT_MACRO if has_macro and not macro_generic else ELIG_COT_ONLY
    if spec.cot_proxy_of:
        return ELIG_PROXY_MACRO if has_macro and not macro_generic else ELIG_PROXY_ONLY
    if (rec.get("institutional_context") or {}).get("data_mode") == "macro_only" or (
        not cot_resolved and has_macro
    ):
        return ELIG_MACRO_ONLY
    return ELIG_NONE


def data_confidence_badge(
    *,
    spec: Any,
    rec: dict[str, Any] | None,
    cot_resolved: bool,
    has_macro: bool,
    macro_generic: bool,
) -> str:
    status = str((rec or {}).get("data_status") or "")
    if status == "broken_mapping":
        return "broken_incomplete"
    if status in {"no_data", "cot_mapping_missing"}:
        return "broken_incomplete"
    if cot_resolved and has_macro and not macro_generic:
        return "full_data"
    if cot_resolved:
        return "partial_data"
    if spec and spec.cot_proxy_of and not cot_resolved:
        return "proxy_only"
    if (rec or {}).get("institutional_context", {}).get("data_mode") == "macro_only" or (
        has_macro and not cot_resolved
    ):
        return "macro_only"
    return "broken_incomplete"


def _anomaly_component(att: dict[str, Any] | None) -> float:
    if not att:
        return 0.0
    total = 0.0
    for a in att.get("alerts") or []:
        total += ALERT_ANOMALY_WEIGHTS.get(a.get("kind", ""), 6)
    return round(total, 1)


def _positioning_change_component(rec: dict[str, Any] | None) -> float:
    if not rec:
        return 0.0
    w1 = rec.get("weekly_change")
    try:
        w = abs(float(w1))
    except (TypeError, ValueError):
        return 0.0
    if w <= 0:
        return 0.0
    return round(min(22.0, w / 2500.0), 1)


def compute_score_breakdown(
    *,
    rec: dict[str, Any] | None,
    att: dict[str, Any] | None,
    spec: Any,
    eligibility_tier: int,
) -> dict[str, Any]:
    cot_resolved = _cot_resolved(rec) if rec else False
    has_macro = _has_macro_transmission(rec) if rec else False
    macro_generic = _macro_generic_only(rec)

    raw_attention = float(att.get("priority_score") or 0) if att else 0.0
    cot_score_component = 0.0
    macro_score_component = 0.0

    if cot_resolved and rec is not None:
        try:
            cs = float(rec.get("cot_score"))
            if cs == cs:  # not NaN
                cot_score_component = round(cs * 4.5, 1)
        except (TypeError, ValueError):
            pass
        if cot_score_component <= 0:
            cot_score_component = round(raw_attention * 0.55, 1)
        try:
            ms = float(rec.get("macro_score"))
            if ms == ms and has_macro and not macro_generic:
                macro_score_component = round(ms * 3.0, 1)
        except (TypeError, ValueError):
            pass
    elif eligibility_tier in (ELIG_PROXY_MACRO, ELIG_PROXY_ONLY):
        macro_score_component = round(raw_attention * 0.65, 1)
        cot_score_component = 0.0
    elif eligibility_tier == ELIG_MACRO_ONLY:
        macro_score_component = round(raw_attention, 1)
    else:
        macro_score_component = round(raw_attention * 0.4, 1) if has_macro else 0.0

    anomaly_component = _anomaly_component(att)
    positioning_change_component = _positioning_change_component(rec) if cot_resolved else 0.0

    penalty = 0.0
    if eligibility_tier == ELIG_MACRO_ONLY:
        penalty += 18.0
        if macro_generic:
            penalty += 12.0
    elif eligibility_tier in (ELIG_PROXY_MACRO, ELIG_PROXY_ONLY):
        penalty += 10.0
    if rec and str(rec.get("data_status") or "") in {"broken_mapping", "no_data", "cot_missing"}:
        penalty += 15.0
    if not att:
        penalty += 40.0

    data_confidence_score = 100.0
    badge = data_confidence_badge(
        spec=spec, rec=rec, cot_resolved=cot_resolved, has_macro=has_macro, macro_generic=macro_generic
    )
    badge_penalties = {
        "full_data": 0,
        "partial_data": 12,
        "proxy_only": 28,
        "macro_only": 38,
        "broken_incomplete": 50,
    }
    data_confidence_score = max(0.0, 100.0 - badge_penalties.get(badge, 40))

    final = round(
        cot_score_component
        + macro_score_component
        + anomaly_component
        + positioning_change_component
        - penalty,
        1,
    )
    final = max(0.0, final)

    return {
        "final_attention_score": final,
        "cot_score_component": cot_score_component,
        "macro_score_component": macro_score_component,
        "positioning_change_component": positioning_change_component,
        "anomaly_component": anomaly_component,
        "data_confidence_score": round(data_confidence_score, 1),
        "penalty_for_missing_data": round(penalty, 1),
        "data_confidence_badge": badge,
        "eligibility_tier": eligibility_tier,
        "eligibility_label": ELIG_LABELS.get(eligibility_tier, "unknown"),
        "attention_score_raw": raw_attention if att else None,
    }


def cap_display_tier(
    tier: str,
    *,
    eligibility_tier: int,
    att: dict[str, Any] | None,
    rec: dict[str, Any] | None,
) -> tuple[str, str | None]:
    """Macro-only / generic macro cannot present as HIGH ATTENTION without explicit flag."""
    if eligibility_tier >= ELIG_MACRO_ONLY:
        if tier == PRIORITY_HIGH:
            return PRIORITY_DEVELOPING, "macro_only_high_attention_capped"
        if _macro_generic_only(rec) and tier in {PRIORITY_HIGH, PRIORITY_DEVELOPING}:
            return PRIORITY_WATCHLIST, "generic_macro_only_capped"
    if eligibility_tier in (ELIG_PROXY_ONLY, ELIG_PROXY_MACRO):
        if tier == PRIORITY_HIGH and float((att or {}).get("priority_score") or 0) < 45:
            return PRIORITY_DEVELOPING, "proxy_high_attention_capped"
    return tier, None


def _tier_from_final_score(
    score: float,
    att: dict[str, Any] | None,
    *,
    eligibility_tier: int,
    rec: dict[str, Any] | None,
) -> str:
    if score < BOARD_SCORE_FLOOR:
        return PRIORITY_LOW
    if score >= 52 or any(a.get("kind") == "flow_extreme" for a in (att or {}).get("alerts") or []):
        tier = PRIORITY_HIGH
    elif score >= 30:
        tier = PRIORITY_DEVELOPING
    elif score >= BOARD_SCORE_FLOOR:
        tier = PRIORITY_WATCHLIST
    else:
        tier = PRIORITY_LOW
    tier, _ = cap_display_tier(tier, eligibility_tier=eligibility_tier, att=att, rec=rec)
    if eligibility_tier >= ELIG_MACRO_ONLY and tier == PRIORITY_HIGH:
        return PRIORITY_DEVELOPING
    return tier


def _sort_key_candidate(c: dict[str, Any]) -> tuple:
    return (
        c["eligibility_tier"],
        -float(c["final_attention_score"]),
        c["instrument_id"],
    )


def _pick_canonical_winners(candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """One representative per canonical group (best eligibility + score)."""
    by_canonical: dict[str, dict[str, Any]] = {}
    superseded: dict[str, str] = {}

    for c in candidates:
        cid = c["duplicate_canonical_id"]
        prev = by_canonical.get(cid)
        if prev is None or _sort_key_candidate(c) < _sort_key_candidate(prev):
            if prev is not None:
                superseded[prev["instrument_id"]] = f"duplicate_canonical_superseded_by_{c['instrument_id']}"
            by_canonical[cid] = c
        else:
            superseded[c["instrument_id"]] = f"duplicate_canonical_superseded_by_{prev['instrument_id']}"

    winners = sorted(by_canonical.values(), key=_sort_key_candidate)
    return winners, superseded


def _entry_from_candidate(c: dict[str, Any], att: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "market": c["instrument_id"],
        "canonical_id": c["duplicate_canonical_id"],
        "priority_tier": c["attention_priority"],
        "priority_label": c["attention_priority_label"],
        "priority_score": c["final_attention_score"],
        "dominant_narrative": att.get("dominant_narrative") if att else "",
        "priority_headline": att.get("priority_headline") if att else "",
        "icon": ((att.get("alerts") or [{}])[0].get("icon", "👀") if att else "👀"),
        "tactical_readable": att.get("tactical_readable") if att else "",
        "data_mode": c.get("data_mode") or "unknown",
        "data_confidence_badge": c.get("data_confidence_badge"),
        "eligibility_label": c.get("eligibility_label"),
        "scoring": {
            "final_attention_score": c["final_attention_score"],
            "cot_score_component": c["cot_score_component"],
            "macro_score_component": c["macro_score_component"],
            "positioning_change_component": c["positioning_change_component"],
            "anomaly_component": c["anomaly_component"],
            "data_confidence_score": c["data_confidence_score"],
            "penalty_for_missing_data": c["penalty_for_missing_data"],
        },
        "inclusion_reason": c.get("inclusion_reason"),
    }


def explain_exclusion(
    *,
    included: bool,
    spec: Any,
    rec: dict[str, Any] | None,
    att: dict[str, Any] | None,
    final_score: float,
    eligibility_tier: int,
    superseded_reason: str | None,
    universe_rank: int | None,
    board_rank: int | None,
) -> tuple[str | None, str | None]:
    if included:
        reason = "ranked_on_board_by_score_and_data_tier"
        if eligibility_tier == ELIG_COT_MACRO:
            reason = "direct_cot_and_macro_top_score"
        elif eligibility_tier == ELIG_COT_ONLY:
            reason = "direct_cot_top_score"
        return None, reason

    if superseded_reason:
        return superseded_reason, None
    if not spec:
        return "no_registry_entry", None
    if rec is None:
        return "no_week_record", None
    if not att:
        if spec.has_cot_mapping and not _cot_resolved(rec):
            return "no_cot_rows", None
        if not _has_macro_transmission(rec):
            return "no_macro_transmission", None
        return "no_attention_metadata", None
    if final_score < BOARD_SCORE_FLOOR:
        if eligibility_tier == ELIG_MACRO_ONLY:
            return "macro_only_low_confidence", None
        return "attention_score_below_threshold", None
    if eligibility_tier >= ELIG_MACRO_ONLY and universe_rank and universe_rank <= 12:
        return "macro_only_lower_priority_than_direct_cot", None
    if board_rank is None and universe_rank and universe_rank > 6:
        return "ranked_out_higher_priority_markets", None
    return "ranked_out_higher_priority_markets", None


def build_priority_debug(
    week_records: list[dict[str, Any]],
    *,
    calendar_week: str = "",
    top_n: int = 6,
) -> dict[str, Any]:
    """Full-universe priority debug — one row per registry instrument."""
    from hptl.cot.cot_quarantine import is_quarantined

    week_by_market = {str(r.get("market")): r for r in week_records if r.get("market")}
    reg = load_registry()
    universe = all_instrument_ids()

    candidates: list[dict[str, Any]] = []

    for iid in universe:
        if is_quarantined(iid):
            spec = reg.get(iid)
            candidates.append(
                {
                    "instrument_id": iid,
                    "display_name": spec.display_name if spec else iid,
                    "asset_class": spec.asset_class if spec else "unknown",
                    "subgroup": spec.subgroup if spec else "unknown",
                    "has_registry_entry": spec is not None,
                    "has_cot_mapping": bool(spec and spec.has_cot_mapping),
                    "has_cot_rows": False,
                    "latest_cot_date": None,
                    "has_macro_transmission": False,
                    "has_price_proxy": bool(spec and spec.cot_proxy_of),
                    "cot_proxy_of": spec.cot_proxy_of if spec else None,
                    "duplicate_canonical_id": canonical_priority_group(spec, iid) if spec else iid,
                    "attention_source": "cot_integrity_quarantine",
                    "data_mode": None,
                    "data_status": "quarantined",
                    "attention_priority": PRIORITY_LOW,
                    "attention_priority_label": PRIORITY_LABELS.get(PRIORITY_LOW, PRIORITY_LOW),
                    "tier_cap_reason": None,
                    "final_attention_score": 0.0,
                    "cot_score_component": 0.0,
                    "macro_score_component": 0.0,
                    "positioning_change_component": 0.0,
                    "anomaly_component": 0.0,
                    "data_confidence_score": 0.0,
                    "penalty_for_missing_data": 100.0,
                    "data_confidence_badge": "broken_incomplete",
                    "eligibility_tier": ELIG_NONE,
                    "eligibility_label": ELIG_LABELS.get(ELIG_NONE, "not_eligible"),
                    "attention_score_raw": None,
                    "exclusion_reason": "cot_integrity_quarantine",
                    "included_in_priority_list": False,
                    "_rec": None,
                    "_att": None,
                }
            )
            continue
        spec = reg.get(iid)
        rec = week_by_market.get(iid)
        att, att_source = resolve_attention_for_record(rec, week_by_market) if rec else (None, "no_week_record")

        cot_ok = _cot_resolved(rec) if rec else False
        has_macro = _has_macro_transmission(rec) if rec else False
        macro_generic = _macro_generic_only(rec)
        elig = classify_eligibility_tier(
            spec=spec, rec=rec, cot_resolved=cot_ok, has_macro=has_macro, macro_generic=macro_generic
        )
        canonical_id = canonical_priority_group(spec, iid)

        breakdown = compute_score_breakdown(rec=rec, att=att, spec=spec, eligibility_tier=elig)
        final_score = breakdown["final_attention_score"]

        raw_tier = _tier_from_final_score(final_score, att, eligibility_tier=elig, rec=rec) if att else PRIORITY_LOW
        display_tier, tier_cap_reason = cap_display_tier(
            raw_tier, eligibility_tier=elig, att=att, rec=rec
        )

        latest_cot = None
        if rec and cot_ok:
            latest_cot = str(rec.get("cot_report_date") or rec.get("latest_report_date") or "") or None

        candidates.append(
            {
                "instrument_id": iid,
                "display_name": spec.display_name if spec else iid,
                "asset_class": spec.asset_class if spec else "unknown",
                "subgroup": spec.subgroup if spec else "unknown",
                "has_registry_entry": spec is not None,
                "has_cot_mapping": bool(spec and spec.has_cot_mapping),
                "has_cot_rows": cot_ok,
                "latest_cot_date": latest_cot,
                "has_macro_transmission": has_macro,
                "has_price_proxy": bool(spec and spec.cot_proxy_of),
                "cot_proxy_of": spec.cot_proxy_of if spec else None,
                "duplicate_canonical_id": canonical_id,
                "attention_source": att_source,
                "data_mode": (rec.get("institutional_context") or {}).get("data_mode") if rec else None,
                "data_status": rec.get("data_status") if rec else "no_week_record",
                "attention_priority": display_tier,
                "attention_priority_label": PRIORITY_LABELS.get(display_tier, display_tier),
                "tier_cap_reason": tier_cap_reason,
                **breakdown,
                "_rec": rec,
                "_att": att,
            }
        )

    rankable = [c for c in candidates if c["final_attention_score"] >= BOARD_SCORE_FLOOR and c["_att"]]
    rankable.sort(key=lambda x: (_sort_key_candidate(x),))
    for i, c in enumerate(rankable):
        c["rank_before_deduplication"] = i + 1

    deduped, superseded_map = _pick_canonical_winners(rankable)
    for i, c in enumerate(deduped):
        c["rank_after_deduplication"] = i + 1

    universe_rank_map = {c["instrument_id"]: c["rank_before_deduplication"] for c in rankable}
    dedup_rank_map = {c["instrument_id"]: c["rank_after_deduplication"] for c in deduped}

    selected_entries: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    for c in deduped[:top_n]:
        selected_ids.add(c["instrument_id"])
        c["inclusion_reason"] = (
            "direct_cot_and_macro_top_score"
            if c["eligibility_tier"] == ELIG_COT_MACRO
            else "direct_cot_top_score"
            if c["eligibility_tier"] == ELIG_COT_ONLY
            else "ranked_on_board_by_score_and_data_tier"
        )
        selected_entries.append(_entry_from_candidate(c, c["_att"]))

    instruments_out: list[dict[str, Any]] = []
    for c in candidates:
        included = c["instrument_id"] in selected_ids
        board_rank = next(
            (i + 1 for i, s in enumerate(selected_entries) if s["market"] == c["instrument_id"]),
            None,
        )
        sup_reason = superseded_map.get(c["instrument_id"])
        excl, incl_reason = explain_exclusion(
            included=included,
            spec=reg.get(c["instrument_id"]),
            rec=c["_rec"],
            att=c["_att"],
            final_score=float(c["final_attention_score"] or 0),
            eligibility_tier=c["eligibility_tier"],
            superseded_reason=sup_reason,
            universe_rank=universe_rank_map.get(c["instrument_id"]),
            board_rank=board_rank,
        )
        row = {k: v for k, v in c.items() if not k.startswith("_")}
        row["included_in_priority_list"] = included
        row["priority_board_rank"] = board_rank
        row["universe_rank"] = universe_rank_map.get(c["instrument_id"])
        row["rank_before_deduplication"] = row.get("rank_before_deduplication")
        row["rank_after_deduplication"] = dedup_rank_map.get(c["instrument_id"])
        row["exclusion_reason"] = excl
        row["inclusion_reason"] = incl_reason if included else None
        if sup_reason and not included:
            row["exclusion_reason"] = sup_reason
        instruments_out.append(row)

    def _audit_row(c: dict[str, Any]) -> dict[str, Any]:
        return {
            "instrument_id": c["instrument_id"],
            "canonical_id": c["duplicate_canonical_id"],
            "rank": c.get("rank_before_deduplication") or c.get("rank_after_deduplication"),
            "rank_before_deduplication": c.get("rank_before_deduplication"),
            "rank_after_deduplication": c.get("rank_after_deduplication"),
            "final_attention_score": c["final_attention_score"],
            "eligibility_label": c["eligibility_label"],
            "data_confidence_badge": c["data_confidence_badge"],
            "data_mode": c.get("data_mode"),
            "cot_score_component": c["cot_score_component"],
            "macro_score_component": c["macro_score_component"],
            "exclusion_reason": superseded_map.get(c["instrument_id"]),
            "included_in_priority_list": c["instrument_id"] in selected_ids,
        }

    audit_before = [_audit_row(c) for c in rankable[:AUDIT_TOP_N]]
    audit_after = [_audit_row(c) for c in deduped[:AUDIT_TOP_N]]

    return {
        "calendar_week": calendar_week,
        "top_n": top_n,
        "score_floor": BOARD_SCORE_FLOOR,
        "universe_size": len(universe),
        "week_records_count": len(week_records),
        "candidates_above_floor": len(rankable),
        "candidates_after_deduplication": len(deduped),
        "ranking_rules": [
            "Sort by data tier: direct COT+macro, direct COT, proxy+macro, proxy, macro-only",
            "Then by final_attention_score (transparent component sum minus penalties)",
            "One board slot per canonical_id (proxies collapse to COT parent)",
            "Macro-only cannot displace direct COT; no diversity swap",
            "HIGH ATTENTION capped for macro-only and generic-macro instruments",
        ],
        "audit": {
            "top_30_before_deduplication": audit_before,
            "top_30_after_deduplication": audit_after,
        },
        "priority_markets": selected_entries,
        "instruments": instruments_out,
    }


def aggregate_priority_markets(
    week_records: list[dict[str, Any]],
    *,
    top_n: int = 6,
    calendar_week: str = "",
) -> dict[str, Any]:
    debug = build_priority_debug(week_records, calendar_week=calendar_week, top_n=top_n)
    excluded = [
        {"market": x["instrument_id"], "reason": x["exclusion_reason"] or "unknown"}
        for x in debug["instruments"]
        if not x["included_in_priority_list"] and x.get("exclusion_reason")
    ]
    return {
        "priority_markets": debug["priority_markets"],
        "high_attention": [m for m in debug["priority_markets"] if m.get("priority_tier") == PRIORITY_HIGH],
        "developing": [m for m in debug["priority_markets"] if m.get("priority_tier") == PRIORITY_DEVELOPING],
        "watchlist_included": [m for m in debug["priority_markets"] if m.get("priority_tier") == PRIORITY_WATCHLIST],
        "total_actionable": debug["candidates_after_deduplication"],
        "excluded_debug": excluded[:80],
        "calendar_week": calendar_week,
        "generated_from": "hptl.context.priority_board",
    }


# Back-compat for tests importing effective_priority_score
def effective_priority_score(
    rec: dict[str, Any],
    att: dict[str, Any],
    week_by_market: dict[str, dict[str, Any]],
) -> float:
    spec = get_instrument(str(rec.get("market") or ""))
    cot_ok = _cot_resolved(rec)
    has_macro = _has_macro_transmission(rec)
    elig = classify_eligibility_tier(
        spec=spec,
        rec=rec,
        cot_resolved=cot_ok,
        has_macro=has_macro,
        macro_generic=_macro_generic_only(rec),
    )
    return compute_score_breakdown(rec=rec, att=att, spec=spec, eligibility_tier=elig)["final_attention_score"]


def write_priority_debug(payload: dict[str, Any], *, path: Path | None = None, public_path: Path | None = None) -> Path:
    out = path or PRIORITY_DEBUG_PATH
    pub = public_path or PUBLIC_PRIORITY_DEBUG_PATH
    out.parent.mkdir(parents=True, exist_ok=True)
    pub.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    out.write_text(text, encoding="utf-8")
    pub.write_text(text, encoding="utf-8")
    return out
