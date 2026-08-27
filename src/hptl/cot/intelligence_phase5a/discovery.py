"""Unsupervised grouping of pre-move behavioural feature vectors."""

from __future__ import annotations

from collections import Counter
from typing import Any

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.spatial.distance import pdist, squareform

from hptl.cot.intelligence_phase5a.config import (
    CLUSTER_K_CANDIDATES,
    CLUSTER_LINKAGE,
    CLUSTER_MIN_CASES,
    CLUSTER_STABILITY_BOOTSTRAPS,
    CLUSTER_STABILITY_FRACTION,
    FEATURE_COLS_FOR_CLUSTER,
)


def _feature_matrix(df: pd.DataFrame) -> tuple[np.ndarray, list[str], pd.Index]:
    cols = [c for c in FEATURE_COLS_FOR_CLUSTER if c in df.columns]
    sub = df[cols].apply(pd.to_numeric, errors="coerce")
    # median impute within pool then z-score
    for c in cols:
        med = sub[c].median()
        if pd.isna(med):
            med = 0.0
        sub[c] = sub[c].fillna(med)
    arr = sub.to_numpy(dtype=float)
    mu = arr.mean(axis=0)
    sd = arr.std(axis=0)
    sd[sd < 1e-9] = 1.0
    z = (arr - mu) / sd
    return z, cols, df.index


def _silhouette_approx(dist: np.ndarray, labels: np.ndarray) -> float:
    """Mean silhouette from a condensed distance matrix (euclidean pairwise)."""
    n = len(labels)
    if n < 3:
        return float("nan")
    D = squareform(dist)
    uniq = sorted(set(labels))
    if len(uniq) < 2:
        return float("nan")
    sil = []
    for i in range(n):
        same = [j for j in range(n) if labels[j] == labels[i] and j != i]
        if not same:
            continue
        a = float(np.mean([D[i, j] for j in same]))
        b = None
        for lab in uniq:
            if lab == labels[i]:
                continue
            others = [j for j in range(n) if labels[j] == lab]
            if not others:
                continue
            mean_o = float(np.mean([D[i, j] for j in others]))
            b = mean_o if b is None else min(b, mean_o)
        if b is None:
            continue
        sil.append((b - a) / max(a, b, 1e-12))
    return float(np.mean(sil)) if sil else float("nan")


def _adjusted_rand(a: np.ndarray, b: np.ndarray) -> float:
    """Simple ARI implementation."""
    n = len(a)
    if n == 0:
        return float("nan")
    # contingency
    la, lb = sorted(set(a)), sorted(set(b))
    cont = np.zeros((len(la), len(lb)), dtype=int)
    ia = {v: i for i, v in enumerate(la)}
    ib = {v: i for i, v in enumerate(lb)}
    for x, y in zip(a, b):
        cont[ia[x], ib[y]] += 1
    sum_comb_c = 0.0
    for i in range(cont.shape[0]):
        for j in range(cont.shape[1]):
            nij = cont[i, j]
            sum_comb_c += nij * (nij - 1) / 2.0
    sum_comb_a = sum(int(s) * (int(s) - 1) / 2.0 for s in cont.sum(axis=1))
    sum_comb_b = sum(int(s) * (int(s) - 1) / 2.0 for s in cont.sum(axis=0))
    comb_n = n * (n - 1) / 2.0
    if comb_n == 0:
        return float("nan")
    expected = sum_comb_a * sum_comb_b / comb_n
    max_index = 0.5 * (sum_comb_a + sum_comb_b)
    if max_index == expected:
        return 1.0 if sum_comb_c == max_index else 0.0
    return float((sum_comb_c - expected) / (max_index - expected))


def choose_k_and_cluster(z: np.ndarray) -> dict[str, Any]:
    if len(z) < CLUSTER_MIN_CASES:
        return {
            "ok": False,
            "reason": f"n={len(z)} < min {CLUSTER_MIN_CASES}",
            "labels": np.zeros(len(z), dtype=int),
            "k": 1,
            "silhouette": None,
            "stability_ari": None,
        }
    dist = pdist(z, metric="euclidean")
    Z = linkage(dist, method=CLUSTER_LINKAGE)
    best = None
    for k in CLUSTER_K_CANDIDATES:
        if k >= len(z):
            continue
        labels = fcluster(Z, t=k, criterion="maxclust")
        sil = _silhouette_approx(dist, labels)
        if best is None or (sil == sil and sil > best["silhouette"]):
            best = {"k": k, "labels": labels, "silhouette": sil}
    if best is None:
        labels = fcluster(Z, t=min(3, len(z) - 1), criterion="maxclust")
        best = {"k": int(labels.max()), "labels": labels, "silhouette": _silhouette_approx(dist, labels)}

    # Stability: subsample, reclusters, ARI on overlap
    rng = np.random.default_rng(42)
    aris = []
    n = len(z)
    for _ in range(CLUSTER_STABILITY_BOOTSTRAPS):
        m = max(CLUSTER_MIN_CASES, int(n * CLUSTER_STABILITY_FRACTION))
        idx = np.sort(rng.choice(n, size=m, replace=False))
        z_sub = z[idx]
        d_sub = pdist(z_sub, metric="euclidean")
        Z_sub = linkage(d_sub, method=CLUSTER_LINKAGE)
        lab_sub = fcluster(Z_sub, t=best["k"], criterion="maxclust")
        # map back: compare to full labels on idx
        aris.append(_adjusted_rand(best["labels"][idx], lab_sub))
    return {
        "ok": True,
        "reason": None,
        "labels": best["labels"],
        "k": best["k"],
        "silhouette": None if best["silhouette"] != best["silhouette"] else round(best["silhouette"], 4),
        "stability_ari": None if not aris else round(float(np.nanmean(aris)), 4),
        "linkage_method": CLUSTER_LINKAGE,
    }


def summarize_clusters(
    df: pd.DataFrame,
    labels: np.ndarray,
    *,
    direction: str,
    scope: str,
) -> list[dict[str, Any]]:
    rows = []
    work = df.copy()
    work["_cluster"] = labels
    n_total = len(work)
    for cid, g in work.groupby("_cluster"):
        markets = sorted(g["market"].unique())
        seq_c = Counter(g.get("commercial_sequence", pd.Series(dtype=str)).fillna("NONE"))
        seq_nc = Counter(g.get("noncommercial_sequence", pd.Series(dtype=str)).fillna("NONE"))
        seq_x = Counter(g.get("cross_sequence", pd.Series(dtype=str)).fillna("NONE"))
        rets = pd.to_numeric(g["forward_return_pct"], errors="coerce").dropna()
        rows.append(
            {
                "behaviour_type_id": f"{scope}_{direction}_C{int(cid)}",
                "scope": scope,
                "direction": direction,
                "cluster_id": int(cid),
                "n_cases": int(len(g)),
                "pct_of_direction_pool": round(100.0 * len(g) / n_total, 2) if n_total else None,
                "n_markets": len(markets),
                "markets": "|".join(markets),
                "market_specific": len(markets) == 1,
                "cross_market": len(markets) >= 3,
                "typical_commercial_sequence": seq_c.most_common(1)[0][0] if seq_c else "NONE",
                "typical_noncommercial_sequence": seq_nc.most_common(1)[0][0] if seq_nc else "NONE",
                "typical_cross_sequence": seq_x.most_common(1)[0][0] if seq_x else "NONE",
                "median_forward_return_pct": None if rets.empty else round(float(rets.median()), 4),
                "p25_forward_return_pct": None if rets.empty else round(float(rets.quantile(0.25)), 4),
                "p75_forward_return_pct": None if rets.empty else round(float(rets.quantile(0.75)), 4),
                "mean_c_pct": round(float(pd.to_numeric(g["c_pct"], errors="coerce").median()), 3)
                if "c_pct" in g
                else None,
                "mean_nc_pct": round(float(pd.to_numeric(g["nc_pct"], errors="coerce").median()), 3)
                if "nc_pct" in g
                else None,
                "mean_c_nc_opp_score": round(
                    float(pd.to_numeric(g["c_nc_opp_score"], errors="coerce").median()), 4
                )
                if "c_nc_opp_score" in g
                else None,
            }
        )
    return rows


def run_discovery(
    features: pd.DataFrame,
    sequences: pd.DataFrame,
    *,
    direction: str,
) -> dict[str, Any]:
    """Cluster independent event cases for one direction (rally or selloff)."""
    ev = features[
        (features["case_role"] == "event")
        & (features["direction"] == direction)
        & (features["independent"] == True)  # noqa: E712
    ].copy()
    if ev.empty:
        return {
            "direction": direction,
            "n_cases": 0,
            "cluster_meta": {"ok": False, "reason": "no_cases"},
            "behaviour_types": [],
            "case_labels": pd.DataFrame(),
            "market_specific_findings": [],
            "cross_market_findings": [],
        }

    # Attach sequences
    seq = sequences[sequences["case_id"].isin(ev["case_id"])][
        ["case_id", "commercial_sequence", "noncommercial_sequence", "cross_sequence"]
    ]
    ev = ev.merge(seq, on="case_id", how="left")

    z, cols, idx = _feature_matrix(ev)
    meta = choose_k_and_cluster(z)
    labels = meta["labels"]
    types = summarize_clusters(ev, labels, direction=direction, scope="global")
    for t in types:
        t["silhouette"] = meta.get("silhouette")
        t["stability_ari"] = meta.get("stability_ari")
        t["k_selected"] = meta.get("k")
        t["cluster_ok"] = meta.get("ok")

    case_labels = ev[["case_id", "market", "onset_date", "horizon_weeks"]].copy()
    case_labels["direction"] = direction
    case_labels["cluster_id"] = labels
    case_labels["behaviour_type_id"] = [
        f"global_{direction}_C{int(c)}" for c in labels
    ]

    # Per-market discovery (only markets with enough cases)
    market_findings = []
    for market, g in ev.groupby("market"):
        if len(g) < CLUSTER_MIN_CASES:
            market_findings.append(
                {
                    "market": market,
                    "direction": direction,
                    "n_cases": int(len(g)),
                    "status": "insufficient_cases",
                    "n_clusters": None,
                }
            )
            continue
        z_m, _, _ = _feature_matrix(g)
        meta_m = choose_k_and_cluster(z_m)
        seq_col = g["commercial_sequence"] if "commercial_sequence" in g.columns else None
        if seq_col is None:
            dom = "NONE"
        else:
            dom = Counter(seq_col.fillna("NONE")).most_common(1)[0][0]
        market_findings.append(
            {
                "market": market,
                "direction": direction,
                "n_cases": int(len(g)),
                "status": "clustered" if meta_m["ok"] else "failed",
                "n_clusters": meta_m.get("k"),
                "silhouette": meta_m.get("silhouette"),
                "stability_ari": meta_m.get("stability_ari"),
                "dominant_commercial_sequence": dom,
            }
        )

    cross = [t for t in types if t.get("cross_market")]
    specific = [t for t in types if t.get("market_specific")]

    return {
        "direction": direction,
        "n_cases": int(len(ev)),
        "feature_columns_used": cols,
        "cluster_meta": meta,
        "behaviour_types": types,
        "case_labels": case_labels,
        "market_specific_findings": market_findings,
        "cross_market_findings": cross,
        "market_specific_types": specific,
    }
