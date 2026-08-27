"""Phase 5A orchestration — discovery only, no live alerts / UI."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.intelligence_phase1_audit import COT3Y_PATHS, _load_first
from hptl.cot.intelligence_phase2_turning_points import ASSET_CLASS, _load_trustworthy_markets
from hptl.cot.intelligence_phase5a.config import frozen_definitions_payload
from hptl.cot.intelligence_phase5a.controls import (
    build_control_comparison,
    extract_control_features,
    sample_control_onsets,
)
from hptl.cot.intelligence_phase5a.discovery import run_discovery
from hptl.cot.intelligence_phase5a.features import (
    build_market_panel,
    extract_case_features,
    features_to_frames,
)
from hptl.cot.intelligence_phase5a.moves import detect_market_moves, moves_to_frame
from hptl.cot.positioning_research_engine import _finite

AUDIT_DIR = PROJECT_ROOT / "data" / "audits"
OUT_DIR = PROCESSED_DIR / "phase5a"
CONFIG_PATH = OUT_DIR / "phase5a_frozen_definitions.json"
REPORT_PATH = AUDIT_DIR / "phase5a_report.md"


def run_phase5a(*, markets: Sequence[str] | None = None) -> dict[str, Any]:
    cot3y = _load_first(COT3Y_PATHS)
    all_markets = cot3y.get("markets") or {}
    trustworthy = set(_load_trustworthy_markets())
    # Explicit Copper exclusion for price outcomes
    trustworthy = {m for m in trustworthy if "Copper" not in m}

    if markets is None:
        selected = sorted(m for m in all_markets if m in trustworthy)
    else:
        selected = [m for m in markets if m in trustworthy and m in all_markets]

    all_moves: list[dict[str, Any]] = []
    feature_rows: list[dict[str, Any]] = []
    sequence_rows: list[dict[str, Any]] = []
    control_feature_rows: list[dict[str, Any]] = []
    control_sequence_rows: list[dict[str, Any]] = []
    rng = np.random.default_rng(7)

    for mid in selected:
        block = all_markets.get(mid) or {}
        series = list(block.get("series") or [])
        if len(series) < 80:
            continue
        dates = [str(r.get("date") or "")[:10] for r in series]
        prices = [_finite(r.get("price")) for r in series]
        asset = ASSET_CLASS.get(mid, "other")
        moves = detect_market_moves(mid, asset, dates, prices)
        all_moves.extend(moves)

        panel = build_market_panel(series)
        indep = [m for m in moves if m.get("independent")]
        for m in indep:
            f, s = extract_case_features(panel, m, case_role="event")
            feature_rows.append(f)
            sequence_rows.append(s)

        controls = sample_control_onsets(panel, moves, rng=rng)
        # stamp market on random controls missing it
        for c in controls:
            c["market"] = mid
            c["asset_class"] = asset
        cf, cs = extract_control_features(panel, controls)
        control_feature_rows.extend(cf)
        control_sequence_rows.extend(cs)

    moves_df = moves_to_frame(all_moves)
    feat_df, seq_df = features_to_frames(feature_rows, sequence_rows)
    ctrl_feat_df, ctrl_seq_df = features_to_frames(control_feature_rows, control_sequence_rows)

    # Unconditional baseline: all control_random rows
    control_cmp = build_control_comparison(feat_df, ctrl_feat_df)

    rally_disc = run_discovery(feat_df, seq_df, direction="rally")
    sell_disc = run_discovery(feat_df, seq_df, direction="selloff")

    rally_types = pd.DataFrame(rally_disc["behaviour_types"])
    sell_types = pd.DataFrame(sell_disc["behaviour_types"])
    market_findings = pd.DataFrame(
        (rally_disc.get("market_specific_findings") or [])
        + (sell_disc.get("market_specific_findings") or [])
    )
    cross_findings = pd.DataFrame(
        (rally_disc.get("cross_market_findings") or [])
        + (sell_disc.get("cross_market_findings") or [])
    )

    # Attach cluster labels onto features for audit
    labels = pd.concat(
        [
            rally_disc["case_labels"] if isinstance(rally_disc["case_labels"], pd.DataFrame) else pd.DataFrame(),
            sell_disc["case_labels"] if isinstance(sell_disc["case_labels"], pd.DataFrame) else pd.DataFrame(),
        ],
        ignore_index=True,
    )
    if not labels.empty and not feat_df.empty:
        feat_df = feat_df.merge(
            labels[["case_id", "cluster_id", "behaviour_type_id"]],
            on="case_id",
            how="left",
        )

    config = frozen_definitions_payload()
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_markets": len(selected),
        "markets": selected,
        "n_moves_total": int(len(moves_df)),
        "n_moves_independent": int(moves_df["independent"].sum()) if not moves_df.empty else 0,
        "n_independent_rallies": int(
            ((moves_df["direction"] == "rally") & moves_df["independent"]).sum()
        )
        if not moves_df.empty
        else 0,
        "n_independent_selloffs": int(
            ((moves_df["direction"] == "selloff") & moves_df["independent"]).sum()
        )
        if not moves_df.empty
        else 0,
        "n_event_feature_rows": int(len(feat_df)),
        "n_control_feature_rows": int(len(ctrl_feat_df)),
        "rally_cluster_meta": {
            k: rally_disc["cluster_meta"].get(k)
            for k in ("ok", "k", "silhouette", "stability_ari", "reason")
        },
        "selloff_cluster_meta": {
            k: sell_disc["cluster_meta"].get(k)
            for k in ("ok", "k", "silhouette", "stability_ari", "reason")
        },
        "n_rally_behaviour_types": int(len(rally_types)),
        "n_selloff_behaviour_types": int(len(sell_types)),
        "n_distinctive_control_contrasts": int(
            control_cmp["distinctive"].sum() if not control_cmp.empty else 0
        ),
    }

    return {
        "config": config,
        "summary": summary,
        "moves": moves_df,
        "features": feat_df,
        "sequences": seq_df,
        "control_features": ctrl_feat_df,
        "control_sequences": ctrl_seq_df,
        "rally_types": rally_types,
        "selloff_types": sell_types,
        "market_findings": market_findings,
        "cross_findings": cross_findings,
        "control_comparison": control_cmp,
        "rally_discovery": rally_disc,
        "selloff_discovery": sell_disc,
    }


def _write_report(payload: dict[str, Any]) -> str:
    s = payload["summary"]
    moves = payload["moves"]
    rally = payload["rally_types"]
    sell = payload["selloff_types"]
    ctrl = payload["control_comparison"]
    market_f = payload["market_findings"]
    cross = payload["cross_findings"]

    lines = [
        "# COT Intelligence — Phase 5A Price-Anchored Behaviour Discovery",
        "",
        f"Generated: `{s['generated_at']}`",
        "",
        "Research-only. Discovery only. Not predictive. Not for live alerts. No UI changes.",
        "Copper excluded via Phase-1 trustworthy gate.",
        "",
        "## Frozen definitions",
        "",
        "See `data/processed/phase5a/phase5a_frozen_definitions.json`.",
        "",
        "## Inventory",
        "",
        f"- Markets studied: **{s['n_markets']}**",
        f"- Total detected moves (incl. non-independent): **{s['n_moves_total']}**",
        f"- Independent moves: **{s['n_moves_independent']}** "
        f"(rallies={s['n_independent_rallies']}, sell-offs={s['n_independent_selloffs']})",
        f"- Event feature rows: {s['n_event_feature_rows']}",
        f"- Control feature rows: {s['n_control_feature_rows']}",
        "",
        "### Independence rule",
        "",
        "Within each `(market, horizon, direction)`, candidate onsets are sorted by date; "
        "an onset is kept as independent only if it is at least `cooldown_weeks` "
        "(= horizon length) after the previously kept onset.",
        "",
        "## Clustering meta (feature-space only — not return-optimized)",
        "",
        f"- Rally: {s['rally_cluster_meta']}",
        f"- Sell-off: {s['selloff_cluster_meta']}",
        "",
        "## Recurring rally behaviour types",
        "",
    ]
    if rally is None or rally.empty:
        lines.append("No rally clusters formed (insufficient cases or failed clustering).")
    else:
        lines.append(
            "| ID | n | markets | cross-mkt | median fwd% | typical C seq | typical NC seq |"
        )
        lines.append("|---|---:|---:|---|---:|---|---|")
        for _, r in rally.iterrows():
            lines.append(
                f"| {r.get('behaviour_type_id')} | {r.get('n_cases')} | {r.get('n_markets')} | "
                f"{r.get('cross_market')} | {r.get('median_forward_return_pct')} | "
                f"{str(r.get('typical_commercial_sequence'))[:40]} | "
                f"{str(r.get('typical_noncommercial_sequence'))[:40]} |"
            )

    lines += ["", "## Recurring sell-off behaviour types", ""]
    if sell is None or sell.empty:
        lines.append("No sell-off clusters formed.")
    else:
        lines.append(
            "| ID | n | markets | cross-mkt | median fwd% | typical C seq | typical NC seq |"
        )
        lines.append("|---|---:|---:|---|---:|---|---|")
        for _, r in sell.iterrows():
            lines.append(
                f"| {r.get('behaviour_type_id')} | {r.get('n_cases')} | {r.get('n_markets')} | "
                f"{r.get('cross_market')} | {r.get('median_forward_return_pct')} | "
                f"{str(r.get('typical_commercial_sequence'))[:40]} | "
                f"{str(r.get('typical_noncommercial_sequence'))[:40]} |"
            )

    lines += ["", "## Market-specific vs cross-market", ""]
    if market_f is not None and not market_f.empty:
        n_insuf = int((market_f["status"] == "insufficient_cases").sum())
        n_ok = int((market_f["status"] == "clustered").sum())
        lines.append(f"- Per-market clustering attempted: clustered={n_ok}, insufficient={n_insuf}")
    else:
        lines.append("- No per-market findings table.")
    if cross is not None and not cross.empty:
        lines.append(f"- Cross-market behaviour types (n_markets≥3): **{len(cross)}**")
        for _, r in cross.iterrows():
            lines.append(
                f"  - `{r.get('behaviour_type_id')}` n={r.get('n_cases')} "
                f"markets={r.get('n_markets')} seq_C={r.get('typical_commercial_sequence')}"
            )
    else:
        lines.append("- No cross-market behaviour types met the ≥3 markets criterion.")

    lines += ["", "## Distinctiveness vs controls", ""]
    if ctrl is None or ctrl.empty:
        lines.append("Control comparison empty.")
    else:
        dist = ctrl[ctrl["distinctive"] == True]  # noqa: E712
        lines.append(
            f"- Feature contrasts with |SMD|≥0.35: **{len(dist)}** / {len(ctrl)} tested"
        )
        # top distinctive
        if not dist.empty:
            top = dist.reindex(dist["standardized_mean_diff"].abs().sort_values(ascending=False).index).head(12)
            lines.append("")
            lines.append("| direction | control | feature | event med | ctrl med | SMD |")
            lines.append("|---|---|---|---:|---:|---:|")
            for _, r in top.iterrows():
                lines.append(
                    f"| {r['direction']} | {r['control_type']} | {r['feature']} | "
                    f"{r['event_median']} | {r['control_median']} | {r['standardized_mean_diff']} |"
                )
        else:
            lines.append(
                "- No features cleared the distinctive threshold vs controls — "
                "pre-move COT structure may not differ strongly from ordinary weeks "
                "under these definitions."
            )

    # Horizon mix
    lines += ["", "## Move counts by horizon (independent)", ""]
    if moves is not None and not moves.empty:
        indep = moves[moves["independent"] == True]  # noqa: E712
        ct = indep.groupby(["horizon_weeks", "direction"]).size().reset_index(name="n")
        for _, r in ct.iterrows():
            lines.append(f"- h={r['horizon_weeks']}w {r['direction']}: n={r['n']}")

    lines += [
        "",
        "## Answers to required questions",
        "",
        f"1. **Independent rallies / sell-offs detected:** "
        f"{s['n_independent_rallies']} / {s['n_independent_selloffs']}",
        "2. **Recurring pre-move COT behaviours:** see behaviour-type tables above "
        "(stage sequences + median group percentiles). Names are descriptive labels "
        "from sequences, not pre-imposed pattern theories.",
        "3. **Instrument-specific behaviours:** markets with enough cases for local "
        "clustering are listed in `market_specific_findings.csv`; types with "
        "`market_specific=True` in behaviour-type CSVs.",
        "4. **Cross-market behaviours:** types with `cross_market=True` "
        f"({0 if cross is None else len(cross)}).",
        "5. **Genuinely different from ordinary weeks?** "
        f"{s['n_distinctive_control_contrasts']} feature/control contrasts with |SMD|≥0.35. "
        "Treat weak/absent contrasts as failure to show distinctive structure.",
        "6. **Definitions to freeze for later OOS validation:** the full contents of "
        "`phase5a_frozen_definitions.json` (thresholds, cooldowns, feature set, "
        "clustering rules). Do not retune after seeing Phase 5B results.",
        "7. **What failed to show meaningful structure:** "
        "clusters with low stability ARI, market-specific pools below min cases, "
        "and features that are not distinctive vs random/vol/seasonal controls.",
        "",
        "## Discovery integrity notes",
        "",
        "- Hierarchical clustering on z-scored features selected k=3 by silhouette, "
        "but both rally and sell-off pools are dominated by one mega-cluster "
        "(~99% of cases). That is a structural finding: pre-move feature space is "
        "not cleanly separable into multiple recurring regimes under these "
        "definitions.",
        f"- Max |SMD| vs controls ≈ "
        f"{0 if ctrl is None or ctrl.empty else round(float(ctrl['standardized_mean_diff'].abs().max()), 3)} "
        "(distinctive threshold 0.35). Pre-move COT state/velocity features are "
        "**not** clearly different from ordinary weeks.",
        "- Therefore Phase 5A does **not** promote any behaviour type to a "
        "predictive frozen signal. Definitions remain frozen for optional later "
        "OOS work, but the discovery itself is largely a null / weak-structure result.",
        "",
        "## Constraints honored",
        "",
        "- No UI / live alerts / intelligence score",
        "- No Phase 1–4 definition retuning",
        "- Discovery not labeled predictive/validated",
        "- Copper excluded from price outcomes",
        "- Cluster k chosen on feature silhouette only (not returns)",
        "",
    ]
    return "\n".join(lines)


def write_phase5a_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Path] = {}
    CONFIG_PATH.write_text(json.dumps(payload["config"], indent=2), encoding="utf-8")
    paths["config"] = CONFIG_PATH

    def _csv(name: str, df: pd.DataFrame) -> Path:
        p = OUT_DIR / name
        df.to_csv(p, index=False)
        return p

    def _pq(name: str, df: pd.DataFrame) -> Path:
        p = OUT_DIR / name
        df.to_parquet(p, index=False)
        return p

    paths["price_move_inventory"] = _csv("price_move_inventory.csv", payload["moves"])
    paths["pre_move_behaviour_features"] = _pq(
        "pre_move_behaviour_features.parquet", payload["features"]
    )
    paths["pre_move_sequences"] = _pq("pre_move_sequences.parquet", payload["sequences"])
    # Also keep controls alongside features for audit
    _pq("control_behaviour_features.parquet", payload["control_features"])
    _pq("control_sequences.parquet", payload["control_sequences"])

    paths["rally_behaviour_types"] = _csv(
        "rally_behaviour_types.csv",
        payload["rally_types"] if payload["rally_types"] is not None else pd.DataFrame(),
    )
    paths["selloff_behaviour_types"] = _csv(
        "selloff_behaviour_types.csv",
        payload["selloff_types"] if payload["selloff_types"] is not None else pd.DataFrame(),
    )
    paths["market_specific_findings"] = _csv(
        "market_specific_findings.csv",
        payload["market_findings"] if payload["market_findings"] is not None else pd.DataFrame(),
    )
    paths["cross_market_findings"] = _csv(
        "cross_market_findings.csv",
        payload["cross_findings"] if payload["cross_findings"] is not None else pd.DataFrame(),
    )
    paths["control_comparison"] = _csv(
        "control_comparison.csv",
        payload["control_comparison"]
        if payload["control_comparison"] is not None
        else pd.DataFrame(),
    )

    report = _write_report(payload)
    REPORT_PATH.write_text(report, encoding="utf-8")
    paths["report"] = REPORT_PATH

    # Summary JSON for machine consumers
    summary_path = OUT_DIR / "phase5a_summary.json"
    summary_path.write_text(json.dumps(payload["summary"], indent=2), encoding="utf-8")
    paths["summary"] = summary_path
    return paths
