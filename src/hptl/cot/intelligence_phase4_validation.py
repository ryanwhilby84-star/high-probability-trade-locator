"""COT Intelligence Engine — Phase 4 chronological walk-forward validation.

Falsifies Phase-3 candidate configuration families on chronologically unseen data.
All Phase 1–3 definitions are frozen — no retuning after seeing OOS results.

No UI, no intelligence score, no ML, no threshold optimization.
Copper remains excluded from price-outcome claims via the Phase-1 trustworthy gate.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.intelligence_phase1_audit import COT3Y_PATHS, _load_first
from hptl.cot.intelligence_phase2_turning_points import (
    _load_trustworthy_markets,
    _median,
    _mean,
    _stdev,
)
from hptl.cot.intelligence_phase3_configurations import (
    INTERACTION_QUERIES,
    PHASE3_JSON,
    SAMPLE_COOLDOWN,
    build_market_configurations,
)

AUDIT_DIR = PROJECT_ROOT / "data" / "audits"
PHASE4_JSON = AUDIT_DIR / "cot_intelligence_phase4_validation.json"
PHASE4_MD = AUDIT_DIR / "cot_intelligence_phase4_validation.md"
PHASE4_TRAIL = (
    PROCESSED_DIR / "cot_intelligence_phase4_audit_trail_latest.json"
)

# Minimum OOS evidence thresholds (predeclared — not tuned on results)
MIN_OOS_N_VALIDATED = 15
MIN_OOS_MARKETS_VALIDATED = 3
MIN_OOS_FOLDS_WITH_SIGNAL = 2
MIN_FOLD_N = 4
MIN_OOS_N_PROMISING = 8
TOP_MARKET_SHARE_MAX = 0.50
# Multiple-testing: 14 families; require stronger asymmetry for VALIDATED
N_FAMILIES_TESTED = 14
BONFERRONI_ALPHA = 0.05


def _wilson_ci(successes: int, n: int, z: float = 1.96) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    p = successes / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    margin = (z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)) / denom
    return round(max(0.0, centre - margin), 4), round(min(1.0, centre + margin), 4)


def _binom_pvalue_two_sided(successes: int, n: int, p0: float = 0.5) -> float | None:
    """Two-sided binomial p-value vs p0 (exact for n<=250; normal approx beyond)."""
    if n <= 0:
        return None
    if n > 250:
        # Continuity-corrected normal approximation
        mean = n * p0
        var = n * p0 * (1.0 - p0)
        if var <= 0:
            return None
        z = abs(successes - mean) - 0.5
        z = z / math.sqrt(var)
        # erfc-based two-sided normal tail
        p = math.erfc(z / math.sqrt(2.0))
        return min(1.0, round(p, 6))

    from math import comb

    def pmf(k: int) -> float:
        return comb(n, k) * (p0**k) * ((1 - p0) ** (n - k))

    obs = pmf(successes)
    total = 0.0
    for k in range(n + 1):
        pk = pmf(k)
        if pk <= obs + 1e-15:
            total += pk
    return min(1.0, round(total, 6))


def load_frozen_candidates() -> list[dict[str, Any]]:
    if not PHASE3_JSON.is_file():
        raise FileNotFoundError(f"Phase 3 audit not found: {PHASE3_JSON}")
    doc = json.loads(PHASE3_JSON.read_text(encoding="utf-8"))
    cands = (doc.get("families") or {}).get("candidate") or []
    if len(cands) != 14:
        # Still proceed with whatever is frozen on disk, but record the count
        pass
    frozen = []
    for i, f in enumerate(cands):
        frozen.append(
            {
                "candidate_id": f"P3C{i+1:02d}",
                "family_key": f["family_key"],
                "family_human": f["family_human"],
                "family_parts": f.get("family_parts"),
                "phase3": {
                    "n": f.get("n"),
                    "n_price": f.get("n_price"),
                    "n_markets": f.get("n_markets"),
                    "asset_classes": f.get("asset_classes"),
                    "top_market": f.get("top_market"),
                    "top_market_share": f.get("top_market_share"),
                    "outcomes": f.get("outcomes"),
                    "descriptive_outcome_asymmetric": f.get(
                        "descriptive_outcome_asymmetric"
                    ),
                    "onset_trigger_mix": f.get("onset_trigger_mix"),
                },
            }
        )
    return frozen


def build_chronological_folds(
    samples: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Strict chronological folds on global calendar of price-eligible samples."""
    dates = sorted(
        {
            str(s.get("date") or "")[:10]
            for s in samples
            if s.get("price_study_eligible") and s.get("date")
        }
    )
    if len(dates) < 40:
        # Fallback: single holdout last 30% by date
        cut = dates[int(len(dates) * 0.7)] if dates else "2019-01-01"
        return [
            {
                "fold_id": "WF1",
                "train_end": cut,
                "test_start": cut,
                "test_end": dates[-1] if dates else cut,
                "method": "single_holdout_last_30pct",
            }
        ]

    d0, d1 = dates[0], dates[-1]
    # Three expanding-window folds with ~2y test windows where possible
    # Anchors chosen as calendar year-ends inside the sample span.
    anchors = ["2019-12-31", "2021-12-31", "2023-12-31"]
    folds = []
    for i, train_end in enumerate(anchors):
        if train_end <= d0 or train_end >= d1:
            continue
        # test runs until next anchor or end
        test_end = anchors[i + 1] if i + 1 < len(anchors) else d1
        if i + 1 < len(anchors):
            # test period is (train_end, next_anchor]
            test_start = train_end
        else:
            test_start = train_end
        if test_end <= train_end:
            continue
        folds.append(
            {
                "fold_id": f"WF{len(folds)+1}",
                "train_start": d0,
                "train_end": train_end,
                "test_start": test_start,  # exclusive lower bound via date > train_end
                "test_end": test_end,
                "method": "expanding_train_fixed_test_window",
            }
        )

    if not folds:
        cut = dates[int(len(dates) * 0.7)]
        folds = [
            {
                "fold_id": "WF1",
                "train_start": d0,
                "train_end": cut,
                "test_start": cut,
                "test_end": d1,
                "method": "single_holdout_last_30pct",
            }
        ]
    return folds


def _in_test(date: str, fold: dict[str, Any]) -> bool:
    d = str(date)[:10]
    # test: train_end < d <= test_end
    return fold["test_start"] < d <= fold["test_end"]


def _in_train(date: str, fold: dict[str, Any]) -> bool:
    d = str(date)[:10]
    train_start = fold.get("train_start") or "0000-01-01"
    return train_start <= d <= fold["train_end"]


def _tpb_signed_return(sample: dict[str, Any], horizon: int = 4) -> float | None:
    """Sign price return by Commercial TP-B direction, else NC TP-B."""
    feat = sample.get("features") or {}
    direction = (feat.get("commercial") or {}).get("tp_b") or (
        (feat.get("noncommercial") or {}).get("tp_b")
    )
    block = (sample.get("outcome_labels") or {}).get(f"fwd_{horizon}w") or {}
    raw = block.get("return_pct")
    if raw is None or direction is None:
        return None
    return float(raw) if direction == "bullish" else -float(raw)


def summarize_outcomes(
    samples: list[dict[str, Any]],
    *,
    signed_fn: Callable[[dict[str, Any], int], float | None] | None = None,
) -> dict[str, Any]:
    keyed = [
        s
        for s in samples
        if s.get("price_study_eligible") and s.get("outcome_labels")
    ]
    out: dict[str, Any] = {
        "n": len(samples),
        "n_price": len(keyed),
        "n_markets": len({s["market"] for s in keyed}),
        "markets": sorted({s["market"] for s in keyed}),
        "n_asset_classes": len({s.get("asset_class") for s in keyed}),
        "asset_classes": sorted(
            {s.get("asset_class") for s in keyed if s.get("asset_class")}
        ),
        "by_market": dict(Counter(s["market"] for s in keyed)),
    }
    for h in (1, 4, 8, 12):
        vals: list[float] = []
        mfes: list[float] = []
        maes: list[float] = []
        for s in keyed:
            block = (s.get("outcome_labels") or {}).get(f"fwd_{h}w") or {}
            raw = block.get("return_pct")
            if raw is None:
                continue
            if signed_fn is not None:
                v = signed_fn(s, h)
                if v is None:
                    continue
            else:
                v = float(raw)
            vals.append(float(v))
            if block.get("mfe_pct") is not None:
                mfes.append(float(block["mfe_pct"]))
            if block.get("mae_pct") is not None:
                maes.append(float(block["mae_pct"]))
        pos = sum(1 for v in vals if v > 0)
        neg = sum(1 for v in vals if v < 0)
        lo, hi = _wilson_ci(pos, len(vals)) if vals else (None, None)
        pval = _binom_pvalue_two_sided(pos, len(vals)) if vals else None
        out[f"fwd_{h}w"] = {
            "n": len(vals),
            "median_return_pct": None if not vals else round(_median(vals) or 0, 4),
            "mean_return_pct": None if not vals else round(_mean(vals) or 0, 4),
            "stdev_return_pct": (
                None if _stdev(vals) is None else round(_stdev(vals) or 0, 4)
            ),
            "pct_positive": None if not vals else round(100.0 * pos / len(vals), 1),
            "pct_negative": None if not vals else round(100.0 * neg / len(vals), 1),
            "wilson_ci_positive": None if lo is None else [lo, hi],
            "binom_pvalue_vs_50": pval,
            "median_mfe_pct": None if not mfes else round(_median(mfes) or 0, 4),
            "median_mae_pct": None if not maes else round(_median(maes) or 0, 4),
        }
    return out


def unconditional_baseline(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """All price-eligible onset samples in the same windows — market base rate."""
    return summarize_outcomes(samples)


def leave_one_market_out(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {"applicable": False}
    by_m = Counter(s["market"] for s in samples if s.get("price_study_eligible"))
    if not by_m:
        return {"applicable": False}
    top_m, _ = by_m.most_common(1)[0]
    rest = [s for s in samples if s["market"] != top_m]
    return {
        "applicable": True,
        "excluded_market": top_m,
        "n_remaining_price": sum(1 for s in rest if s.get("price_study_eligible")),
        "outcomes": summarize_outcomes(rest),
    }


def leave_one_asset_out(samples: list[dict[str, Any]]) -> dict[str, Any]:
    price_s = [s for s in samples if s.get("price_study_eligible")]
    by_a = Counter(s.get("asset_class") for s in price_s)
    if len(by_a) < 2:
        return {"applicable": False}
    top_a, _ = by_a.most_common(1)[0]
    rest = [s for s in price_s if s.get("asset_class") != top_a]
    return {
        "applicable": True,
        "excluded_asset_class": top_a,
        "n_remaining_price": len(rest),
        "outcomes": summarize_outcomes(rest),
    }


def fold_stability(
    fold_stats: list[dict[str, Any]],
) -> dict[str, Any]:
    """Check whether %+4W stays on the same side of 50 across folds with enough n."""
    usable = []
    for fs in fold_stats:
        f4 = (fs.get("outcomes") or {}).get("fwd_4w") or {}
        n = f4.get("n") or 0
        pp = f4.get("pct_positive")
        if n >= MIN_FOLD_N and pp is not None:
            usable.append({"fold_id": fs["fold_id"], "n": n, "pct_positive": pp})
    if len(usable) < 2:
        return {
            "stable": None,
            "reason": "insufficient_folds_with_min_n",
            "folds": usable,
        }
    sides = [1 if u["pct_positive"] >= 50 else -1 for u in usable]
    # Also flag if all near 50
    spreads = [abs(u["pct_positive"] - 50) for u in usable]
    return {
        "stable": len(set(sides)) == 1 and max(spreads) >= 3,
        "same_side_of_50": len(set(sides)) == 1,
        "folds": usable,
        "dominated_by_one_fold": (
            max(u["n"] for u in usable) / sum(u["n"] for u in usable) >= 0.7
            if usable
            else None
        ),
    }


def classify_family(
    *,
    oos: dict[str, Any],
    phase3: dict[str, Any],
    folds: list[dict[str, Any]],
    lomo: dict[str, Any],
    lao: dict[str, Any],
    stability: dict[str, Any],
) -> dict[str, Any]:
    f4 = oos.get("fwd_4w") or {}
    n = f4.get("n") or 0
    pp = f4.get("pct_positive")
    med = f4.get("median_return_pct")
    pval = f4.get("binom_pvalue_vs_50")
    ci = f4.get("wilson_ci_positive")
    n_markets = oos.get("n_markets") or 0
    by_m = oos.get("by_market") or {}
    top_share = (max(by_m.values()) / n) if by_m and n else None

    p3_f4 = ((phase3.get("outcomes") or {}).get("fwd_4w") or {})
    p3_pp = p3_f4.get("pct_positive")
    p3_med = p3_f4.get("median_return_pct")

    # Direction of discovery claim
    p3_side = None
    if p3_pp is not None:
        if p3_pp >= 54:
            p3_side = "up"
        elif p3_pp <= 46:
            p3_side = "down"

    oos_side = None
    if pp is not None:
        if pp >= 54:
            oos_side = "up"
        elif pp <= 46:
            oos_side = "down"
        else:
            oos_side = "flat"

    reversed_vs_p3 = (
        p3_side in {"up", "down"}
        and oos_side in {"up", "down"}
        and p3_side != oos_side
        and n >= MIN_OOS_N_PROMISING
    )
    disappeared = (
        p3_side in {"up", "down"}
        and oos_side == "flat"
        and n >= MIN_OOS_N_VALIDATED
    )

    # Bonferroni-adjusted significance for VALIDATED claim
    bonf_alpha = BONFERRONI_ALPHA / max(N_FAMILIES_TESTED, 1)
    significant = pval is not None and pval < bonf_alpha
    ci_excludes_50 = (
        ci is not None
        and len(ci) == 2
        and ci[0] is not None
        and ci[1] is not None
        and (ci[1] < 0.5 or ci[0] > 0.5)
    )

    lomo_ok = True
    if lomo.get("applicable") and (lomo.get("n_remaining_price") or 0) >= MIN_OOS_N_PROMISING:
        lomo_pp = ((lomo.get("outcomes") or {}).get("fwd_4w") or {}).get("pct_positive")
        if p3_side == "up" and lomo_pp is not None and lomo_pp < 48:
            lomo_ok = False
        if p3_side == "down" and lomo_pp is not None and lomo_pp > 52:
            lomo_ok = False

    folds_ok = stability.get("same_side_of_50") is True and not stability.get(
        "dominated_by_one_fold"
    )
    n_signal_folds = sum(
        1
        for u in (stability.get("folds") or [])
        if abs((u.get("pct_positive") or 50) - 50) >= 3
    )

    reasons: list[str] = []

    if reversed_vs_p3:
        label = "FAILED"
        reasons.append("OOS direction reverses Phase-3 discovery asymmetry")
    elif disappeared:
        label = "FAILED"
        reasons.append("OOS collapses to ~coin-flip despite adequate sample")
    elif n < MIN_OOS_N_PROMISING:
        label = "PROMISING / MONITOR"
        reasons.append(f"Insufficient OOS n={n} (<{MIN_OOS_N_PROMISING})")
    elif oos_side == "flat" and n >= MIN_OOS_N_VALIDATED:
        label = "FAILED"
        reasons.append("No OOS asymmetry with adequate sample")
    elif (
        n >= MIN_OOS_N_VALIDATED
        and n_markets >= MIN_OOS_MARKETS_VALIDATED
        and oos_side in {"up", "down"}
        and (p3_side is None or oos_side == p3_side)
        and (top_share is None or top_share <= TOP_MARKET_SHARE_MAX)
        and lomo_ok
        and folds_ok
        and n_signal_folds >= MIN_OOS_FOLDS_WITH_SIGNAL
        and (significant or ci_excludes_50)
    ):
        label = "VALIDATED"
        reasons.append(
            "OOS persistence with sample size, cross-market breadth, fold stability, "
            "and uncertainty gate (CI or Bonferroni-adjusted p)"
        )
    elif (
        oos_side in {"up", "down"}
        and (p3_side is None or oos_side == p3_side)
        and n >= MIN_OOS_N_PROMISING
    ):
        label = "PROMISING / MONITOR"
        reasons.append(
            "Directionally persistent OOS signal but insufficient robustness "
            "(sample/folds/markets/uncertainty)"
        )
        if not folds_ok:
            reasons.append("Fold instability or single-fold dominance")
        if top_share and top_share > TOP_MARKET_SHARE_MAX:
            reasons.append(f"Top market share {top_share:.0%} exceeds {TOP_MARKET_SHARE_MAX:.0%}")
        if not (significant or ci_excludes_50):
            reasons.append("Not distinguishable from 50% after uncertainty adjustment")
        if not lomo_ok:
            reasons.append("Fails leave-one-top-market-out")
    else:
        label = "FAILED"
        reasons.append("OOS behaviour does not support Phase-3 claim")

    return {
        "classification": label,
        "reasons": reasons,
        "oos_side": oos_side,
        "phase3_side": p3_side,
        "reversed_vs_phase3": reversed_vs_p3,
        "top_market_share_oos": None if top_share is None else round(top_share, 4),
        "bonferroni_alpha": bonf_alpha,
        "significant_vs_50_bonferroni": significant,
        "wilson_ci_excludes_50": ci_excludes_50,
        "phase3_pct_positive_4w": p3_pp,
        "phase3_median_4w": p3_med,
        "oos_pct_positive_4w": pp,
        "oos_median_4w": med,
        "oos_n_price": n,
        "oos_n_markets": n_markets,
    }


def validate_interactions(
    all_samples: list[dict[str, Any]],
    folds: list[dict[str, Any]],
) -> dict[str, Any]:
    """Frozen Phase-3 interaction queries with OOS-only evaluation."""
    wanted = {
        "C_tpB_alone",
        "C_tpB_plus_div",
        "NC_tpB_alone",
        "NC_tpB_plus_div",
    }
    queries = [q for q in INTERACTION_QUERIES if q["id"] in wanted]
    results: dict[str, Any] = {}

    for q in queries:
        matched = [s for s in all_samples if q["test"](s["features"])]
        fold_blocks = []
        oos_all: list[dict[str, Any]] = []
        for fold in folds:
            oos = [
                s
                for s in matched
                if s.get("price_study_eligible") and _in_test(s["date"], fold)
            ]
            fold_blocks.append(
                {
                    "fold_id": fold["fold_id"],
                    "test_start": fold["test_start"],
                    "test_end": fold["test_end"],
                    "outcomes_raw": summarize_outcomes(oos),
                    "outcomes_signed_tpb": summarize_outcomes(
                        oos, signed_fn=_tpb_signed_return
                    ),
                }
            )
            oos_all.extend(oos)

        seen: set[tuple[str, str]] = set()
        uniq: list[dict[str, Any]] = []
        for s in oos_all:
            k = (s["market"], s["date"])
            if k in seen:
                continue
            seen.add(k)
            uniq.append(s)

        is_ref = summarize_outcomes(
            [s for s in matched if s.get("price_study_eligible")]
        )
        oos_raw = summarize_outcomes(uniq)
        oos_signed = summarize_outcomes(uniq, signed_fn=_tpb_signed_return)
        stability = fold_stability(
            [
                {"fold_id": fb["fold_id"], "outcomes": fb["outcomes_raw"]}
                for fb in fold_blocks
            ]
        )

        results[q["id"]] = {
            "label": q["label"],
            "definition_frozen": True,
            "full_sample_raw": is_ref,
            "oos_raw": oos_raw,
            "oos_signed_by_tpb": oos_signed,
            "folds": fold_blocks,
            "fold_stability_raw": stability,
            "lomo": leave_one_market_out(uniq),
        }

    def contrast(a_id: str, b_id: str) -> dict[str, Any]:
        a = results[a_id]["oos_raw"].get("fwd_4w") or {}
        b = results[b_id]["oos_raw"].get("fwd_4w") or {}
        a_s = results[a_id]["oos_signed_by_tpb"].get("fwd_4w") or {}
        b_s = results[b_id]["oos_signed_by_tpb"].get("fwd_4w") or {}
        return {
            "baseline_id": a_id,
            "interaction_id": b_id,
            "baseline_oos_pct_positive_4w": a.get("pct_positive"),
            "baseline_oos_median_4w": a.get("median_return_pct"),
            "baseline_oos_n": a.get("n"),
            "interaction_oos_pct_positive_4w": b.get("pct_positive"),
            "interaction_oos_median_4w": b.get("median_return_pct"),
            "interaction_oos_n": b.get("n"),
            "delta_pct_positive": (
                None
                if a.get("pct_positive") is None or b.get("pct_positive") is None
                else round(b["pct_positive"] - a["pct_positive"], 1)
            ),
            "baseline_oos_signed_pct_positive_4w": a_s.get("pct_positive"),
            "interaction_oos_signed_pct_positive_4w": b_s.get("pct_positive"),
            "delta_signed_pct_positive": (
                None
                if a_s.get("pct_positive") is None or b_s.get("pct_positive") is None
                else round(b_s["pct_positive"] - a_s["pct_positive"], 1)
            ),
        }

    results["contrast_commercial"] = contrast("C_tpB_alone", "C_tpB_plus_div")
    results["contrast_nc"] = contrast("NC_tpB_alone", "NC_tpB_plus_div")

    for key in ("commercial_tpB_div", "nc_tpB_div"):
        c = results[
            "contrast_commercial" if key.startswith("commercial") else "contrast_nc"
        ]
        inter_id = "C_tpB_plus_div" if key.startswith("commercial") else "NC_tpB_plus_div"
        inter_n = c.get("interaction_oos_n") or 0
        delta = c.get("delta_pct_positive")
        inter_pp = c.get("interaction_oos_pct_positive_4w")

        signed_delta = c.get("delta_signed_pct_positive")
        signed_inter_pp = c.get("interaction_oos_signed_pct_positive_4w")
        inter_ci = (
            (results[inter_id]["oos_raw"].get("fwd_4w") or {}).get("wilson_ci_positive")
        )
        ci_excludes_50 = (
            inter_ci is not None
            and len(inter_ci) == 2
            and (inter_ci[1] < 0.5 or inter_ci[0] > 0.5)
        )
        stab = results[inter_id]["fold_stability_raw"]

        if inter_n < MIN_OOS_N_PROMISING:
            verdict = "PROMISING / MONITOR"
            why = f"Insufficient OOS interaction n={inter_n}"
        elif delta is None:
            verdict = "FAILED"
            why = "Missing OOS statistics"
        elif key.startswith("commercial"):
            # Raw uplift can persist while TP-B-signed outcomes fail — do not
            # VALIDATED without directional confirmation + CI excluding 50%.
            raw_lift = delta >= 5 and (inter_pp or 0) >= 55
            signed_ok = (
                signed_inter_pp is not None
                and signed_inter_pp >= 55
                and signed_delta is not None
                and signed_delta >= 5
            )
            if (
                raw_lift
                and signed_ok
                and inter_n >= MIN_OOS_N_VALIDATED
                and stab.get("same_side_of_50")
                and ci_excludes_50
            ):
                verdict = "VALIDATED"
                why = (
                    "OOS Commercial TP-B+DIV stronger than alone on both raw and "
                    "TP-B-signed returns, with fold stability and CI excluding 50%"
                )
            elif raw_lift:
                verdict = "PROMISING / MONITOR"
                why = (
                    "Raw OOS uplift vs Commercial TP-B alone persists, but "
                    "TP-B-signed outcomes and/or uncertainty gates do not clear "
                    "VALIDATED (insufficient evidence for live matching)"
                )
            elif abs(delta) < 5 and inter_pp is not None and 45 <= inter_pp <= 55:
                verdict = "FAILED"
                why = "OOS Commercial TP-B+DIV edge vs baseline disappears"
            else:
                verdict = "FAILED"
                why = "OOS does not support Phase-3 Commercial TP-B+DIV claim"
        else:
            # Phase 3: NC+DIV looked weaker (~40% +). Persistence of adverse lift.
            if delta <= -5 and inter_n >= MIN_OOS_N_PROMISING:
                verdict = "PROMISING / MONITOR"
                why = (
                    "OOS still shows NC TP-B+DIV weaker vs alone — "
                    "adverse interaction to monitor, not a long setup"
                )
            elif abs(delta) < 5 and inter_pp is not None and 45 <= inter_pp <= 55:
                verdict = "FAILED"
                why = "OOS NC TP-B+DIV contrast disappears"
            else:
                verdict = "FAILED"
                why = "OOS does not support Phase-3 NC TP-B+DIV contrast"

        results[f"verdict_{key}"] = {
            "classification": verdict,
            "reason": why,
            "contrast": c,
            "wilson_ci_excludes_50": ci_excludes_50,
            "signed_directional_support": (
                None
                if key.startswith("nc")
                else bool(
                    signed_inter_pp is not None
                    and signed_inter_pp >= 55
                    and signed_delta is not None
                    and signed_delta >= 5
                )
            ),
        }

    return results


def mirrored_pair_check(candidates: list[dict[str, Any]], samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Find structural inverses among candidates and compare OOS behaviour."""
    # Simple heuristic: swap C_hi_NC_lo <-> C_lo_NC_hi and xh/xl, hi/lo bins in human string
    def invert_human(h: str) -> str:
        rep = (
            h.replace("C_hi_NC_lo", "TMP_OPP")
            .replace("C_lo_NC_hi", "C_hi_NC_lo")
            .replace("TMP_OPP", "C_lo_NC_hi")
            .replace("C xh/", "C __XL__/")
            .replace("C xl/", "C xh/")
            .replace("C __XL__/", "C xl/")
            .replace("NC xh/", "NC __XL__/")
            .replace("NC xl/", "NC xh/")
            .replace("NC __XL__/", "NC xl/")
            .replace("C hi/", "C __LO__/")
            .replace("C lo/", "C hi/")
            .replace("C __LO__/", "C lo/")
            .replace("NC hi/", "NC __LO__/")
            .replace("NC lo/", "NC hi/")
            .replace("NC __LO__/", "NC lo/")
        )
        return rep

    by_human = {c["family_human"]: c for c in candidates}
    pairs = []
    seen = set()
    for c in candidates:
        inv = invert_human(c["family_human"])
        if inv in by_human and inv != c["family_human"]:
            key = tuple(sorted([c["family_key"], by_human[inv]["family_key"]]))
            if key in seen:
                continue
            seen.add(key)
            a_s = [s for s in samples if s["family_key"] == c["family_key"] and s.get("price_study_eligible")]
            b_s = [
                s
                for s in samples
                if s["family_key"] == by_human[inv]["family_key"] and s.get("price_study_eligible")
            ]
            pairs.append(
                {
                    "family_a": c["family_human"],
                    "family_b": by_human[inv]["family_human"],
                    "a_outcomes": summarize_outcomes(a_s),
                    "b_outcomes": summarize_outcomes(b_s),
                    "note": (
                        "Structural mirror comparison on full eligible sample; "
                        "symmetry not required"
                    ),
                }
            )
    return pairs


def run_phase4_validation(*, markets: Sequence[str] | None = None) -> dict[str, Any]:
    frozen = load_frozen_candidates()
    frozen_keys = {f["family_key"] for f in frozen}
    cot3y = _load_first(COT3Y_PATHS)
    all_markets = cot3y.get("markets") or {}
    trustworthy = set(_load_trustworthy_markets())

    if markets is None:
        selected = sorted(str(k) for k in all_markets.keys())
    else:
        selected = list(markets)

    all_samples: list[dict[str, Any]] = []
    for mid in selected:
        block = all_markets.get(mid)
        if not block:
            continue
        result = build_market_configurations(mid, block, price_ok=mid in trustworthy)
        if result.get("available"):
            all_samples.extend(result["samples"])

    folds = build_chronological_folds(all_samples)
    price_samples = [s for s in all_samples if s.get("price_study_eligible")]

    # Unconditional baseline on all OOS windows combined
    oos_all_onsets = [
        s for s in price_samples if any(_in_test(s["date"], f) for f in folds)
    ]
    baseline_oos = unconditional_baseline(oos_all_onsets)
    baseline_full = unconditional_baseline(price_samples)

    family_results = []
    for fam in frozen:
        key = fam["family_key"]
        members = [s for s in all_samples if s["family_key"] == key]
        price_members = [s for s in members if s.get("price_study_eligible")]

        fold_blocks = []
        oos_members: list[dict[str, Any]] = []
        for fold in folds:
            train_s = [s for s in price_members if _in_train(s["date"], fold)]
            test_s = [s for s in price_members if _in_test(s["date"], fold)]
            fold_blocks.append(
                {
                    "fold_id": fold["fold_id"],
                    "train_start": fold.get("train_start"),
                    "train_end": fold["train_end"],
                    "test_start": fold["test_start"],
                    "test_end": fold["test_end"],
                    "train_outcomes": summarize_outcomes(train_s),
                    "outcomes": summarize_outcomes(test_s),
                    "n_test": len(test_s),
                    "markets_test": sorted({s["market"] for s in test_s}),
                }
            )
            oos_members.extend(test_s)

        # Dedup OOS
        seen = set()
        oos_uniq = []
        for s in oos_members:
            k = (s["market"], s["date"], s["family_key"])
            if k in seen:
                continue
            seen.add(k)
            oos_uniq.append(s)

        oos_stats = summarize_outcomes(oos_uniq)
        full_stats = summarize_outcomes(price_members)
        lomo = leave_one_market_out(oos_uniq)
        lao = leave_one_asset_out(oos_uniq)
        stability = fold_stability(fold_blocks)
        classification = classify_family(
            oos=oos_stats,
            phase3=fam["phase3"],
            folds=fold_blocks,
            lomo=lomo,
            lao=lao,
            stability=stability,
        )

        family_results.append(
            {
                **fam,
                "full_eligible_outcomes": full_stats,
                "oos_outcomes": oos_stats,
                "folds": fold_blocks,
                "fold_stability": stability,
                "leave_one_market_out": lomo,
                "leave_one_asset_class_out": lao,
                "vs_unconditional_oos": {
                    "family_pct_positive_4w": (oos_stats.get("fwd_4w") or {}).get(
                        "pct_positive"
                    ),
                    "unconditional_pct_positive_4w": (baseline_oos.get("fwd_4w") or {}).get(
                        "pct_positive"
                    ),
                },
                "classification": classification,
            }
        )

    interactions = validate_interactions(all_samples, folds)
    mirrors = mirrored_pair_check(frozen, all_samples)

    counts = Counter(f["classification"]["classification"] for f in family_results)
    validated = [
        f for f in family_results if f["classification"]["classification"] == "VALIDATED"
    ]
    promising = [
        f
        for f in family_results
        if f["classification"]["classification"] == "PROMISING / MONITOR"
    ]
    failed = [
        f for f in family_results if f["classification"]["classification"] == "FAILED"
    ]

    inter_c = interactions.get("verdict_commercial_tpB_div") or {}
    inter_nc = interactions.get("verdict_nc_tpB_div") or {}
    any_validated_interaction = inter_c.get("classification") == "VALIDATED"

    executive = {
        "question": (
            "Did Phase 3 discover any COT configurations that genuinely survive "
            "unseen historical data strongly enough to justify live pattern matching "
            "in Phase 5?"
        ),
        "answer": (
            "YES — limited"
            if validated or any_validated_interaction
            else (
                "NO — not yet strong enough for Phase 5"
                if promising or inter_c.get("classification") == "PROMISING / MONITOR"
                else "NO — null result"
            )
        ),
        "n_validated_families": len(validated),
        "n_promising_families": len(promising),
        "n_failed_families": len(failed),
        "validated_ids": [f["candidate_id"] for f in validated],
        "promising_ids": [f["candidate_id"] for f in promising],
        "failed_ids": [f["candidate_id"] for f in failed],
        # backward-compatible aliases
        "n_validated": len(validated),
        "n_promising": len(promising),
        "n_failed": len(failed),
        "interaction_commercial": inter_c,
        "interaction_nc": inter_nc,
        "selection_caveat": (
            "The 14 family keys were shortlisted in Phase 3 using full-history "
            "outcomes. Fold OOS tests temporal persistence of those frozen keys; "
            "it is not selection-naive rediscovery. Prefer insufficient evidence "
            "over VALIDATED when uncertainty or signed-direction gates fail."
        ),
        "guidance": (
            "Phase 5 may only consume VALIDATED configurations. "
            "PROMISING items stay offline for more history. FAILED definitions stay frozen-failed."
            if validated or any_validated_interaction
            else (
                "Do not begin live pattern matching in Phase 5. "
                "Zero of 14 families cleared VALIDATED. "
                "Commercial TP-B+DIV shows descriptive raw-return persistence but "
                "fails TP-B-signed / uncertainty gates — monitor only. "
                "Do not retune failed families."
                if promising
                or inter_c.get("classification") == "PROMISING / MONITOR"
                else "Do not begin live pattern matching. Null result accepted."
            )
        ),
    }

    audit = {
        "version": "cot_intelligence_phase4_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "chronological_walk_forward_falsification",
        "constraints_honored": [
            "Phase 3 family definitions frozen",
            "No threshold tuning after OOS",
            "No UI / score / ML",
            "Copper excluded from price claims via trustworthy gate",
            "Strict chronological folds (no random split)",
            "Failed configs not rescued",
        ],
        "frozen_from_phase3": {
            "n_candidates": len(frozen),
            "candidate_ids": [f["candidate_id"] for f in frozen],
            "source": str(PHASE3_JSON),
            "sample_cooldown_weeks": SAMPLE_COOLDOWN,
        },
        "walk_forward": {
            "folds": folds,
            "note": (
                "Expanding train through train_end; OOS samples are those with "
                "train_end < date <= test_end. Definitions never rebuilt from test data."
            ),
        },
        "baselines": {
            "unconditional_full_eligible": baseline_full,
            "unconditional_oos_windows": baseline_oos,
        },
        "multiple_testing": {
            "n_families_tested": N_FAMILIES_TESTED,
            "bonferroni_alpha_0_05": BONFERRONI_ALPHA / N_FAMILIES_TESTED,
            "uncertainty": "Wilson CI on pct_positive; exact binomial p vs 50%",
        },
        "families": family_results,
        "interactions": interactions,
        "directional_symmetry_pairs": mirrors,
        "classification_counts": dict(counts),
        "executive_verdict": executive,
    }

    trail = {
        "version": "cot_intelligence_phase4_audit_trail_v1",
        "generated_at": audit["generated_at"],
        "families": [
            {
                "candidate_id": f["candidate_id"],
                "family_key": f["family_key"],
                "family_human": f["family_human"],
                "family_parts": f.get("family_parts"),
                "phase3": f["phase3"],
                "oos_outcomes": f["oos_outcomes"],
                "folds": f["folds"],
                "robustness": {
                    "fold_stability": f["fold_stability"],
                    "lomo": f["leave_one_market_out"],
                    "lao": f["leave_one_asset_class_out"],
                },
                "classification": f["classification"],
            }
            for f in family_results
        ],
        "interactions": interactions,
        "executive_verdict": executive,
    }

    return {"audit": audit, "trail": trail}


def write_phase4_markdown(audit: dict[str, Any]) -> str:
    ev = audit["executive_verdict"]
    base = (audit.get("baselines") or {}).get("unconditional_oos_windows") or {}
    b4 = base.get("fwd_4w") or {}
    lines = [
        "# COT Intelligence — Phase 4 Walk-Forward Validation",
        "",
        f"Generated: `{audit['generated_at']}`",
        "",
        "Falsification phase. Definitions frozen. No retuning. No UI / score. Copper excluded.",
        "",
        "## Executive verdict",
        "",
        f"**{ev['answer']}**",
        "",
        ev["question"],
        "",
        f"- VALIDATED families: {ev['n_validated']} → {ev['validated_ids']}",
        f"- PROMISING / MONITOR: {ev['n_promising']} → {ev['promising_ids']}",
        f"- FAILED: {ev['n_failed']} → {ev['failed_ids']}",
        "",
        ev.get("selection_caveat") or "",
        "",
        ev["guidance"],
        "",
        "## Walk-forward methodology",
        "",
        "Strict chronological expanding-window folds. No random train/test split.",
        "At each fold, configuration definitions stay frozen from Phase 3; only",
        "occurrences with `train_end < date ≤ test_end` count as OOS.",
        "",
    ]
    for f in audit["walk_forward"]["folds"]:
        lines.append(
            f"- **{f['fold_id']}**: train `{f.get('train_start','?')} → {f['train_end']}` | "
            f"test `{f['test_start']} < date ≤ {f['test_end']}` ({f['method']})"
        )
    lines += [
        "",
        audit["walk_forward"]["note"],
        "",
        f"Unconditional OOS baseline (all price-eligible onsets in test windows): "
        f"n={b4.get('n')}, %+4W={b4.get('pct_positive')}, med4W={b4.get('median_return_pct')}%",
        "",
        "## Family validation table (all 14)",
        "",
        "| ID | Class | OOS n | Mkts | AC | %+4W OOS | Med4W | %+4W P3 | CI excl 50? | Fold same-side | Human |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for fam in audit["families"]:
        c = fam["classification"]
        o4 = (fam.get("oos_outcomes") or {}).get("fwd_4w") or {}
        stab = fam.get("fold_stability") or {}
        lines.append(
            "| {id} | {cls} | {n} | {m} | {ac} | {pp} | {med} | {p3} | {ci} | {fs} | {h} |".format(
                id=fam["candidate_id"],
                cls=c["classification"],
                n=o4.get("n"),
                m=fam["oos_outcomes"].get("n_markets"),
                ac=fam["oos_outcomes"].get("n_asset_classes"),
                pp=o4.get("pct_positive"),
                med=o4.get("median_return_pct"),
                p3=c.get("phase3_pct_positive_4w"),
                ci=c.get("wilson_ci_excludes_50"),
                fs=stab.get("same_side_of_50"),
                h=(fam.get("family_human") or "")[:64],
            )
        )

    lines += ["", "## Fold-by-fold (4W %+)", ""]
    for fam in audit["families"]:
        bits = []
        for fb in fam.get("folds") or []:
            f4 = (fb.get("outcomes") or {}).get("fwd_4w") or {}
            bits.append(
                f"{fb['fold_id']}: n={f4.get('n')} %+={f4.get('pct_positive')} "
                f"med={f4.get('median_return_pct')}"
            )
        lines.append(f"- **{fam['candidate_id']}** ({fam['classification']['classification']}): " + "; ".join(bits))
        reasons = "; ".join(fam["classification"].get("reasons") or [])
        lines.append(f"  - reasons: {reasons}")

    ix = audit["interactions"]
    vc = ix.get("verdict_commercial_tpB_div") or {}
    vn = ix.get("verdict_nc_tpB_div") or {}
    cc = ix.get("contrast_commercial") or {}
    cn = ix.get("contrast_nc") or {}
    lines += [
        "",
        "## Interaction validation (frozen Phase-3 definitions)",
        "",
        "### Commercial TP-B alone vs Commercial TP-B + active DIV",
        "",
        f"- Classification: **{vc.get('classification')}**",
        f"- Reason: {vc.get('reason')}",
        f"- OOS alone: n={cc.get('baseline_oos_n')}, %+4W={cc.get('baseline_oos_pct_positive_4w')}, med={cc.get('baseline_oos_median_4w')}",
        f"- OOS +DIV: n={cc.get('interaction_oos_n')}, %+4W={cc.get('interaction_oos_pct_positive_4w')}, med={cc.get('interaction_oos_median_4w')}, Δ%+={cc.get('delta_pct_positive')}",
        f"- OOS signed-by-TP-B: alone %+={cc.get('baseline_oos_signed_pct_positive_4w')}, +DIV %+={cc.get('interaction_oos_signed_pct_positive_4w')}, Δ={cc.get('delta_signed_pct_positive')}",
        "",
        "### NC TP-B alone vs NC TP-B + active DIV",
        "",
        f"- Classification: **{vn.get('classification')}**",
        f"- Reason: {vn.get('reason')}",
        f"- OOS alone: n={cn.get('baseline_oos_n')}, %+4W={cn.get('baseline_oos_pct_positive_4w')}, med={cn.get('baseline_oos_median_4w')}",
        f"- OOS +DIV: n={cn.get('interaction_oos_n')}, %+4W={cn.get('interaction_oos_pct_positive_4w')}, med={cn.get('interaction_oos_median_4w')}, Δ%+={cn.get('delta_pct_positive')}",
        "",
        "## Multiple testing / uncertainty",
        "",
        str(audit["multiple_testing"]),
        "",
        "Wilson CI and exact/approx binomial p vs 50% are reported per family OOS 4W.",
        "VALIDATED requires Bonferroni-adjusted significance or CI excluding 50%, plus",
        "sample, market, and fold gates. Prefer insufficient evidence over false edge.",
        "",
        "## Directional symmetry",
        "",
    ]
    mirrors = audit.get("directional_symmetry_pairs") or []
    if not mirrors:
        lines.append("No structural mirror pairs found among the 14 candidates.")
    else:
        for m in mirrors:
            a4 = (m.get("a_outcomes") or {}).get("fwd_4w") or {}
            b4 = (m.get("b_outcomes") or {}).get("fwd_4w") or {}
            lines.append(
                f"- `{m['family_a'][:50]}` %+4W={a4.get('pct_positive')} vs "
                f"`{m['family_b'][:50]}` %+4W={b4.get('pct_positive')} "
                f"(symmetry not required)"
            )

    lines += ["", "## Constraints honored", ""]
    for c in audit.get("constraints_honored") or []:
        lines.append(f"- {c}")
    lines.append("")
    return "\n".join(lines)


def write_phase4_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    audit = payload["audit"]
    trail = payload["trail"]
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    PHASE4_JSON.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    PHASE4_TRAIL.write_text(json.dumps(trail, indent=2), encoding="utf-8")
    PHASE4_MD.write_text(write_phase4_markdown(audit), encoding="utf-8")
    return {
        "audit_json": PHASE4_JSON,
        "audit_md": PHASE4_MD,
        "trail": PHASE4_TRAIL,
    }
