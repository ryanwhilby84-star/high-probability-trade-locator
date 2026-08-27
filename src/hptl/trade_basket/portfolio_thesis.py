"""Phase 4.5 Portfolio Thesis Summary — presentation only.

Assembles deterministic prose from already-computed basket fields.
Does not compute correlations, exposures, Neff, or diversification scores.
"""

from __future__ import annotations

from typing import Any

from hptl.fx.currency_map import parse_fx_pair
from hptl.portfolio_intelligence.metrics import classify_pair_strength
from hptl.trade_basket.fx_decomposition import CURRENCY_LABELS

ENGINE_VERSION = "portfolio_thesis_v4_5"


def _currency_display_name(code: str) -> str:
    raw = CURRENCY_LABELS.get(str(code or "").upper(), str(code or "").upper())
    # "Australian-dollar" → "Australian Dollar"
    return " ".join(part.capitalize() for part in raw.replace("_", "-").split("-"))


def _primary_thesis_title(currency: str, direction: str) -> str:
    name = _currency_display_name(currency)
    d = str(direction or "").upper()
    if d == "LONG":
        return f"{name} Strength"
    if d == "SHORT":
        return f"{name} Weakness"
    return f"{name} Neutral"


def _parse_trade_label(label: str) -> tuple[str, str]:
    parts = str(label or "").strip().rsplit(" ", 1)
    if len(parts) != 2:
        return str(label or "").strip(), ""
    return parts[0], parts[1].upper()


def _counterpart_currency(instrument_id: str, primary: str) -> str | None:
    legs = parse_fx_pair(instrument_id)
    if legs is None:
        return None
    p = primary.upper()
    if legs.base == p:
        return legs.quote
    if legs.quote == p:
        return legs.base
    return None


def _find_pair_correlation(
    pairs: list[dict[str, Any]],
    supporting: list[str],
) -> dict[str, Any] | None:
    """Pick adjusted correlation for the first supporting trade pair, if present."""
    if len(supporting) < 2:
        return None
    parsed = [_parse_trade_label(s) for s in supporting]
    for i in range(len(parsed)):
        for j in range(i + 1, len(parsed)):
            a_id, a_dir = parsed[i]
            b_id, b_dir = parsed[j]
            for p in pairs:
                a_match = (
                    p.get("trade_a_instrument_id") == a_id
                    and p.get("trade_a_direction") == a_dir
                    and p.get("trade_b_instrument_id") == b_id
                    and p.get("trade_b_direction") == b_dir
                )
                b_match = (
                    p.get("trade_a_instrument_id") == b_id
                    and p.get("trade_a_direction") == b_dir
                    and p.get("trade_b_instrument_id") == a_id
                    and p.get("trade_b_direction") == a_dir
                )
                if a_match or b_match:
                    return p
    return None


def _diversification_interpretation(n_eff: float, n: int, div: float) -> str:
    """Reuse Phase 3 diversification ranges (high ≥70, moderate ≥40, else low)."""
    n_eff_s = f"{n_eff:.1f}"
    if n <= 0:
        return "No populated trades are available for diversification interpretation."
    if n == 1:
        lead = (
            "One position currently represents approximately 1.0 independent trading idea."
        )
    else:
        lead = (
            f"{n} positions currently represent approximately {n_eff_s} "
            "independent trading ideas."
        )
    if div >= 70:
        tail = "The basket therefore shows high diversification relative to the number of trades."
    elif div >= 40:
        tail = "The basket therefore contains some overlap but is not highly concentrated."
    else:
        tail = "The basket therefore shows substantial overlap among the proposed trades."
    return f"{lead} {tail}"


def _correlation_interpretation(adj: float) -> dict[str, Any]:
    """Reuse Phase 3 pair strength bands via classify_pair_strength."""
    classified = classify_pair_strength(adj)
    strength = classified["strength"]
    rel = classified["relationship"]
    if rel == "positive":
        interpretation = f"{strength} positive relationship."
    elif rel == "negative":
        interpretation = f"{strength} negative relationship."
    else:
        interpretation = f"{strength} relationship."
    sign = "+" if adj > 0 else ""
    return {
        "adjusted_correlation": round(float(adj), 2),
        "adjusted_correlation_display": f"{sign}{float(adj):.2f}",
        "strength": strength,
        "relationship": rel,
        "interpretation": interpretation,
    }


def build_portfolio_thesis(basket_payload: dict[str, Any]) -> dict[str, Any]:
    """Build thesis summary dict from an already-enriched basket payload."""
    if basket_payload.get("status") != "ok":
        return {
            "status": "skipped",
            "reason": "basket_not_ok",
            "engine": ENGINE_VERSION,
        }

    exposure = basket_payload.get("currency_exposure") or {}
    intel = basket_payload.get("portfolio_intelligence") or {}
    if exposure.get("status") != "ok" or not exposure.get("has_fx_trades"):
        return {
            "status": "skipped",
            "reason": "no_fx_currency_exposure",
            "engine": ENGINE_VERSION,
        }

    dominant = exposure.get("dominant_currency_exposure")
    if not dominant:
        return {
            "status": "skipped",
            "reason": "no_dominant_currency_exposure",
            "engine": ENGINE_VERSION,
        }

    currency = str(dominant.get("currency") or "")
    direction = str(dominant.get("direction") or "")
    supporting = list(dominant.get("contributing_trades") or [])
    share = float(dominant.get("share_of_gross") or 0.0)
    primary_title = _primary_thesis_title(currency, direction)
    primary_exposure = str(dominant.get("display") or f"{currency} {direction}")

    n = int(intel.get("trades_entered") or len(basket_payload.get("trades") or []))
    n_eff = float(intel.get("effective_independent_trades") or 0.0)
    div = float(intel.get("diversification_score") or 0.0)

    counterparts: list[str] = []
    for label in supporting:
        iid, _ = _parse_trade_label(label)
        other = _counterpart_currency(iid, currency)
        if other and other not in counterparts:
            counterparts.append(other)

    # Portfolio interpretation paragraphs (existing values only).
    paragraphs: list[str] = []
    count = len(supporting)
    side = "strength" if direction == "LONG" else ("weakness" if direction == "SHORT" else "exposure")
    ccy_name = _currency_display_name(currency)
    _count_words = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five"}
    count_word = _count_words.get(count, str(count))
    if count == 1:
        paragraphs.append(
            f"This portfolio contains one trade expressing {ccy_name.lower()} {side}."
        )
    else:
        paragraphs.append(
            f"This portfolio contains {count_word} trades expressing {ccy_name.lower()} {side}."
        )

    if count >= 2 and counterparts:
        if len(counterparts) >= 2:
            joined = " and ".join(counterparts)
            paragraphs.append(
                f"The trades are not duplicates because they use different counterpart "
                f"currencies ({joined})."
            )
        else:
            paragraphs.append(
                "The trades share the same primary currency exposure across distinct instruments."
            )
    elif count >= 2:
        paragraphs.append(
            "The trades share the same primary currency exposure across distinct instruments."
        )

    pairs = list(basket_payload.get("pairs") or [])
    pair_row = _find_pair_correlation(pairs, supporting)
    corr_block = None
    if pair_row is not None and pair_row.get("direction_adjusted_correlation") is not None:
        adj = float(pair_row["direction_adjusted_correlation"])
        corr_block = _correlation_interpretation(adj)
        paragraphs.append(
            f"Historical adjusted correlation between these trades is "
            f"{corr_block['strength'].lower()} "
            f"({corr_block['adjusted_correlation_display']}), "
            f"providing some diversification while maintaining a common directional thesis."
            if corr_block["strength"] in ("Moderate", "Low", "Minimal")
            else (
                f"Historical adjusted correlation between these trades is "
                f"{corr_block['strength'].lower()} "
                f"({corr_block['adjusted_correlation_display']}), "
                f"indicating substantial historical overlap under a common directional thesis."
            )
        )

    div_text = _diversification_interpretation(n_eff, n, div)

    return {
        "status": "ok",
        "engine": ENGINE_VERSION,
        "phase": "4.5",
        "primary_thesis": primary_title,
        "supporting_trades": supporting,
        "portfolio_interpretation": paragraphs,
        "risk_concentration": {
            "primary_exposure": primary_exposure,
            "shared_by_trades": count,
            "share_of_planned_risk": round(share, 8),
            "share_of_planned_risk_display": f"{share * 100:.0f}%",
            # Source field from Phase 4 dominant exposure (gross currency share).
            "source": "currency_exposure.dominant_currency_exposure.share_of_gross",
        },
        "diversification_interpretation": div_text,
        "correlation_interpretation": corr_block,
        "reused_inputs": [
            "currency_exposure.dominant_currency_exposure",
            "portfolio_intelligence.effective_independent_trades",
            "portfolio_intelligence.diversification_score",
            "portfolio_intelligence.largest_exposure_cluster",
            "pairs.direction_adjusted_correlation",
        ],
        "no_new_calculations": True,
    }


def enrich_basket_with_portfolio_thesis(
    basket_payload: dict[str, Any],
) -> dict[str, Any]:
    """Attach portfolio_thesis without altering Phase 2A/3/4 maths fields."""
    out = dict(basket_payload)
    out["portfolio_thesis"] = build_portfolio_thesis(out)
    return out
