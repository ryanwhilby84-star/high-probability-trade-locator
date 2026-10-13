"""Regression checks for fail-closed Institutional Edge release price validation."""
import importlib.util
import json
from datetime import date, timedelta
from pathlib import Path


def load_validator():
    path = Path(__file__).resolve().parents[1] / "scripts" / "package_dashboard_data_release.py"
    spec = importlib.util.spec_from_file_location("dashboard_release_validation", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.validate


def snapshot(tmp_path, *, with_daily=True, age_days=1, summary_daily=1):
    latest = (date.today() - timedelta(days=age_days)).isoformat()
    (tmp_path / "instrument_registry.json").write_text(json.dumps({"markets": [{"id": "Gold"}]}))
    (tmp_path / "prices_latest.json").write_text(json.dumps({
        "instruments": {"Gold": {"daily": [{"date": latest, "open": 1, "high": 2, "low": 1, "close": 2}] if with_daily else [],
                                  "weekly": [{"date": latest, "open": 1, "high": 2, "low": 1, "close": 2}]}},
        "summary": {"instruments_total": 1, "with_daily_bars": summary_daily}
    }))
    (tmp_path / "confluence_history_latest.json").write_text(json.dumps({"records": [{"id": "Gold"}], "latest_cot_report_date": latest}))
    (tmp_path / "cot_weekly_inspector_latest.json").write_text(json.dumps({"markets": [{"id": "Gold"}]}))
    (tmp_path / "cot_commercial_attention_latest.json").write_text(json.dumps({"attention_board": [{"id": "Gold"}]}))


def test_valid_complete_snapshot(tmp_path):
    snapshot(tmp_path)
    errors, _ = load_validator()(tmp_path, max_cot_age_days=14)
    assert errors == []


def test_missing_price_snapshot_fails(tmp_path):
    snapshot(tmp_path)
    (tmp_path / "prices_latest.json").unlink()
    errors, _ = load_validator()(tmp_path, max_cot_age_days=14)
    assert any("prices_latest.json: missing" in e for e in errors)


def test_stale_price_history_fails(tmp_path):
    snapshot(tmp_path, age_days=11)
    errors, _ = load_validator()(tmp_path, max_cot_age_days=14)
    assert any("latest bar age" in e for e in errors)


def test_weekly_only_cannot_claim_daily_coverage(tmp_path):
    snapshot(tmp_path, with_daily=False, summary_daily=1)
    errors, _ = load_validator()(tmp_path, max_cot_age_days=14)
    assert any("summary daily coverage" in e for e in errors)


def test_no_daily_coverage_fails(tmp_path):
    snapshot(tmp_path, with_daily=False, summary_daily=0)
    errors, _ = load_validator()(tmp_path, max_cot_age_days=14)
    assert any("no verified price coverage" in e for e in errors)
