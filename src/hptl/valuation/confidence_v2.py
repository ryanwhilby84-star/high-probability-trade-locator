"""Valuation confidence v2 — composite fit / data / error scoring.

Does not alter fair value, deviation, or regression math. Assigns confidence only.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

ConfidenceLabel = Literal["high", "medium", "low", "none"]
SubBand = Literal["High", "Medium", "Low"]

WEIGHT_FIT = 0.55
WEIGHT_DATA = 0.30
WEIGHT_ERROR = 0.15

NEUTRAL_ERROR_SCORE = 70.0

FX_MODEL = "fx_carry_real_yield_v3"
METALS_MODEL = "metals_real_yield_v1"
AGRI_REGRESSION = "agri_stu_regression_v1"
AGRI_PERCENTILE = "agri_stu_percentile_v1"


@dataclass(frozen=True)
class ConfidenceV2Result:
    confidence: ConfidenceLabel
    confidence_v2_score: float
    confidence_subscores: dict[str, float]
    confidence_subscore_bands: dict[str, SubBand]
    confidence_explanation: str
    confidence_v1: str | None = None

    def as_export_fields(self) -> dict[str, Any]:
        return {
            "confidence": self.confidence,
            "confidence_v2_score": round(self.confidence_v2_score, 1),
            "confidence_subscores": {k: round(v, 1) for k, v in self.confidence_subscores.items()},
            "confidence_subscore_bands": dict(self.confidence_subscore_bands),
            "confidence_explanation": self.confidence_explanation,
            "confidence_v1": self.confidence_v1,
        }


def _clamp(score: float) -> float:
    return max(0.0, min(100.0, score))


def _subscore_band(score: float) -> SubBand:
    if score >= 65:
        return "High"
    if score >= 40:
        return "Medium"
    return "Low"


def _composite(fit: float, data: float, error: float) -> float:
    return _clamp(WEIGHT_FIT * fit + WEIGHT_DATA * data + WEIGHT_ERROR * error)


def _band_from_composite(composite: float, fit: float, data: float) -> ConfidenceLabel:
    if composite >= 72 and fit >= 65 and data >= 55:
        return "high"
    if composite >= 48 and fit >= 40:
        return "medium"
    if composite >= 25:
        return "low"
    return "none"


def _stale_penalty_fx(stale_inputs: list[str]) -> float:
  penalty = 0.0
  for item in stale_inputs:
    field = item.split(".")[-1] if "." in item else item
    if field == "cpi_yoy":
      penalty += 10
    elif field in {"y2", "y10"}:
      penalty += 25
    elif field == "policy_rate":
      penalty += 25
  return penalty


def _fx_fit_score(n: int, r2: float | None) -> float:
    if r2 is None or n <= 0:
        return 0.0
    r2_part = 40.0 * min(r2 / 0.5, 1.0)
    n_part = 30.0 * min(n / 2600.0, 1.0)
    if r2 >= 0.25:
        stability = 30.0
    elif r2 >= 0.18:
        stability = 20.0
    elif r2 >= 0.08:
        stability = 10.0
    else:
        stability = 0.0
    return _clamp(r2_part + n_part + stability)


def _metals_fit_score(n: int, r2: float | None) -> float:
    if r2 is None or n <= 0:
        return 0.0
    return _clamp(50.0 * min(r2 / 0.5, 1.0) + 50.0 * min(n / 400.0, 1.0))


def _agri_fit_score(
    *,
    n: int,
    r2: float | None,
    regression_path: bool,
) -> float:
    if n <= 0:
        return 0.0
    if regression_path and r2 is not None:
        return _clamp(60.0 * min(r2 / 0.5, 1.0) + 40.0 * min(n / 48.0, 1.0))
    # Percentile fallback — depth only (no R²)
    return _clamp(70.0 * min(n / 48.0, 1.0))


def _error_score_from_mad(mad_pct: float | None) -> float:
    if mad_pct is None or mad_pct < 0:
        return NEUTRAL_ERROR_SCORE
    return _clamp(100.0 - min(100.0, (mad_pct / 30.0) * 100.0))


def _metals_high_eligible(trust_grade: str | None, r2: float | None, mad_pct: float | None) -> bool:
    if trust_grade != "A" or r2 is None or mad_pct is None:
        return False
    return r2 >= 0.35 and mad_pct <= 20.0


def _build_explanation(
    band: ConfidenceLabel,
    fit_band: SubBand,
    data_band: SubBand,
    error_band: SubBand,
    *,
    notes: list[str],
) -> str:
    if band == "none":
        base = "Confidence unavailable — insufficient model fit, data quality, or publish gates."
    else:
        base = f"Confidence: {band.capitalize()} — fit {fit_band}, data {data_band}, error {error_band}."
    if notes:
        return base + " " + " ".join(notes)
    return base


def compute_confidence_v2(
    *,
    model_id: str,
    publishable: bool,
    n: int = 0,
    r_squared: float | None = None,
    stale_inputs: list[str] | None = None,
    missing_inputs: list[str] | None = None,
    trust_grade: str | None = None,
    inputs_fresh: bool | None = None,
    mean_abs_deviation_pct: float | None = None,
    agri_regression_path: bool = False,
    confidence_v1: str | None = None,
) -> ConfidenceV2Result:
    """Compute v2 composite confidence for a published valuation block."""
    stale = list(stale_inputs or [])
    missing = list(missing_inputs or [])

    if not publishable:
        return ConfidenceV2Result(
            confidence="none",
            confidence_v2_score=0.0,
            confidence_subscores={"fit_score": 0.0, "data_score": 0.0, "error_score": 0.0},
            confidence_subscore_bands={"fit": "Low", "data": "Low", "error": "Low"},
            confidence_explanation=_build_explanation(
                "none", "Low", "Low", "Low", notes=["Publish gates not met."]
            ),
            confidence_v1=confidence_v1,
        )

    notes: list[str] = []
    fit = 0.0
    data = 100.0
    error = NEUTRAL_ERROR_SCORE

    if model_id == FX_MODEL:
        core_missing = [m for m in missing if not m.endswith(".cpi_yoy")]
        if core_missing or n < 52 or r_squared is None or r_squared < 0.08:
            return ConfidenceV2Result(
                confidence="none",
                confidence_v2_score=0.0,
                confidence_subscores={"fit_score": 0.0, "data_score": 0.0, "error_score": 0.0},
                confidence_subscore_bands={"fit": "Low", "data": "Low", "error": "Low"},
                confidence_explanation=_build_explanation(
                    "none", "Low", "Low", "Low", notes=["FX publish gates not met (missing core inputs or weak R²)."]
                ),
                confidence_v1=confidence_v1,
            )
        fit = _fx_fit_score(n, r_squared)
        penalty = _stale_penalty_fx(stale)
        data = _clamp(100.0 - penalty)
        cpi_stale = [s for s in stale if s.endswith(".cpi_yoy") or s.endswith("cpi_yoy")]
        hard_stale = [s for s in stale if s not in cpi_stale]
        if cpi_stale:
            notes.append(
                f"Input freshness weaker due to stale CPI ({', '.join(cpi_stale)})."
            )
        if hard_stale:
            notes.append(f"Stale core inputs: {', '.join(hard_stale)}.")
        error = NEUTRAL_ERROR_SCORE

    elif model_id == METALS_MODEL:
        if n < 52 or r_squared is None or r_squared < 0.08:
            return ConfidenceV2Result(
                confidence="none",
                confidence_v2_score=0.0,
                confidence_subscores={"fit_score": 0.0, "data_score": 0.0, "error_score": 0.0},
                confidence_subscore_bands={"fit": "Low", "data": "Low", "error": "Low"},
                confidence_explanation=_build_explanation(
                    "none", "Low", "Low", "Low", notes=["Metals publish gates not met."]
                ),
                confidence_v1=confidence_v1,
            )
        fit = _metals_fit_score(n, r_squared)
        data = 100.0 if inputs_fresh else 60.0
        if not inputs_fresh:
            notes.append("Macro inputs (real yield / DXY) are not fully fresh.")
        error = _error_score_from_mad(mean_abs_deviation_pct)
        if mean_abs_deviation_pct is not None and mean_abs_deviation_pct > 25:
            notes.append(f"Historical mean absolute deviation is elevated ({mean_abs_deviation_pct:.1f}%).")

    elif model_id in {AGRI_REGRESSION, AGRI_PERCENTILE, "agri_fundamental_valuation"}:
        if n < 12:
            return ConfidenceV2Result(
                confidence="none",
                confidence_v2_score=0.0,
                confidence_subscores={"fit_score": 0.0, "data_score": 0.0, "error_score": 0.0},
                confidence_subscore_bands={"fit": "Low", "data": "Low", "error": "Low"},
                confidence_explanation=_build_explanation(
                    "none",
                    "Low",
                    "Low",
                    "Low",
                    notes=["Insufficient aligned balance-sheet history (need 12+ pairs)."],
                ),
                confidence_v1=confidence_v1,
            )
        regression_path = agri_regression_path or model_id == AGRI_REGRESSION
        fit = _agri_fit_score(n=n, r2=r_squared, regression_path=regression_path)
        data = 100.0 if n >= 24 else 70.0 if n >= 12 else 40.0
        error = NEUTRAL_ERROR_SCORE
        if not regression_path:
            notes.append(
                "Fit score uses aligned observation depth (percentile path — no regression R²)."
            )
        if regression_path and r_squared is not None and r_squared < 0.25:
            notes.append(f"Regression R² ({r_squared:.3f}) is below the high-confidence path (0.25).")

    else:
        fit = _fx_fit_score(n, r_squared) if r_squared is not None else 0.0
        data = _clamp(100.0 - _stale_penalty_fx(stale))
        error = _error_score_from_mad(mean_abs_deviation_pct)

    composite = _composite(fit, data, error)
    band = _band_from_composite(composite, fit, data)

    if model_id == METALS_MODEL and _metals_high_eligible(trust_grade, r_squared, mean_abs_deviation_pct):
        if fit >= 65 and data >= 55 and composite >= 72:
            band = "high"
            notes.append("Strong metals evidence: trust A, R²≥0.35, MAD≤20%.")
        elif band == "low" and composite >= 48:
            band = "medium"
            notes.append("Metals trust A with moderate error — capped at medium despite fit.")

    if model_id in {AGRI_REGRESSION, AGRI_PERCENTILE, "agri_fundamental_valuation"}:
        if (
            agri_regression_path or model_id == AGRI_REGRESSION
        ) and r_squared is not None and r_squared >= 0.25 and n >= 24:
            if fit >= 65 and data >= 55 and composite >= 72:
                band = "high"
                notes.append("Agri regression path with R²≥0.25 and sufficient depth.")

    fit_band = _subscore_band(fit)
    data_band = _subscore_band(data)
    error_band = _subscore_band(error)

    if band == "high" and fit_band == "Low":
        band = "medium"
        notes.append("Capped at medium — fit sub-score too weak for high confidence.")
    if band == "high" and r_squared is not None and r_squared < 0.15:
        band = "medium"
        notes.append("Capped at medium — R² below 0.15.")

    explanation = _build_explanation(band, fit_band, data_band, error_band, notes=notes)

    return ConfidenceV2Result(
        confidence=band,
        confidence_v2_score=composite,
        confidence_subscores={
            "fit_score": fit,
            "data_score": data,
            "error_score": error,
        },
        confidence_subscore_bands={
            "fit": fit_band,
            "data": data_band,
            "error": error_band,
        },
        confidence_explanation=explanation,
        confidence_v1=confidence_v1,
    )


def attach_confidence_v2(block: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    """Merge v2 confidence fields into a valuation export block."""
    result = compute_confidence_v2(**kwargs)
    block.update(result.as_export_fields())
    return block


def fx_confidence_display_label(label: ConfidenceLabel) -> str:
    """Map v2 label to legacy FX Title Case."""
    return {"high": "High", "medium": "Medium", "low": "Low", "none": "None"}.get(label, "None")


def result_to_dict(result: ConfidenceV2Result) -> dict[str, Any]:
    return asdict(result)
