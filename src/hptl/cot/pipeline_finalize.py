"""Pipeline run finalization — health + logs on every exit path."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from hptl.cot.cot_failures import log_cot_failure
from hptl.cot.pipeline_health import write_health_from_pipeline_result
from hptl.cot.weekly_run_log import persist_weekly_run

if TYPE_CHECKING:
    from hptl.cot.pipeline import CotPipelineResult

logger = logging.getLogger(__name__)


def finalize_pipeline_run(result: "CotPipelineResult", *, human_lines: list[str]) -> "CotPipelineResult":
    """Persist weekly log, write health JSON, and record failures — never silent."""
    payload = result.to_log_dict()
    payload["download_success"] = result.download_success
    payload["ingest_success"] = result.ingest_success
    if result.download_validation is not None:
        payload["download_validation"] = result.download_validation
    if result.ingest_validation is not None:
        payload["ingest_validation"] = result.ingest_validation

    try:
        persist_weekly_run(payload, human_lines=human_lines)
    except OSError as exc:
        logger.warning("Could not persist weekly run log: %s", exc)
        log_cot_failure(failure_type="logging", source="persist_weekly_run", error=str(exc))

    try:
        write_health_from_pipeline_result(result)
    except Exception as exc:
        logger.warning("COT pipeline health export failed: %s", exc)
        log_cot_failure(failure_type="health_export", source="pipeline_health", error=str(exc))

    if result.exit_code != 0 or result.error:
        log_cot_failure(
            failure_type="pipeline",
            source="run_full_pipeline",
            error=result.error or f"exit_code={result.exit_code}",
            detail={
                "exit_code": result.exit_code,
                "download_success": result.download_success,
                "ingest_success": result.ingest_success,
            },
        )

    return result
