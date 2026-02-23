from datetime import date
import json
import logging
from pathlib import Path
from typing import TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.db_models import PipelineRun, StepRun
from app.retry import RetryExhaustedError, run_with_retries
from app.run_store import (
    create_or_get_run,
    create_step_attempt,
    finish_step_failure,
    finish_step_success,
    mark_run_failed,
    mark_run_running,
    mark_run_succeeded,
    reset_failed_run_state,
    store_dead_letters,
    store_published_records,
)
from app.schemas import InvalidRecord, PipelineResult
from app.step_logic import ingest_records, transform_records, validate_records, write_json, write_jsonl


logger = logging.getLogger(__name__)
T = TypeVar("T")


class PipelineRunner:
    def __init__(self, settings: Settings, session_factory: sessionmaker[Session]) -> None:
        self.settings = settings
        self.session_factory = session_factory

    def run(self, *, run_date: date, run_key: str, trigger_source: str = "manual") -> PipelineResult:
        with self.session_factory() as db:
            run, created = create_or_get_run(
                db,
                run_key=run_key,
                run_date=run_date,
                trigger_source=trigger_source,
            )
            if not created:
                if run.status == "failed":
                    # Keep the same run key and clear prior failed state.
                    logger.info("retrying previously failed run", extra={"run_key": run_key})
                    reset_failed_run_state(db, run)
                else:
                    logger.info("idempotent run reused", extra={"run_key": run_key, "status": run.status})
                    return self._result_from_run(run, report_path=self._report_path(run_key), reused_existing_run=True)

            mark_run_running(db, run)

            total_records = 0
            valid_records: list[dict[str, object]] = []
            invalid_records: list[InvalidRecord] = []

            try:
                ingested = self._run_step(db, run, "ingest", lambda: self._ingest(run_date))
                total_records = len(ingested)

                transformed = self._run_step(db, run, "transform", lambda: transform_records(ingested))
                valid_records, invalid_records = self._run_step(
                    db,
                    run,
                    "validate",
                    lambda: validate_records(transformed),
                )

                self._run_step(
                    db,
                    run,
                    "publish_report",
                    lambda: self._publish_outputs(
                        run_key=run_key,
                        run_date=run_date,
                        valid_records=valid_records,
                        invalid_records=invalid_records,
                        total_records=total_records,
                    ),
                )

                store_published_records(db, run_id=run.id, records=valid_records)
                store_dead_letters(db, run_id=run.id, invalid_records=invalid_records)

                mark_run_succeeded(
                    db,
                    run,
                    total_records=total_records,
                    valid_records=len(valid_records),
                    invalid_records=len(invalid_records),
                )
            except Exception as exc:
                mark_run_failed(
                    db,
                    run,
                    error=str(exc),
                    total_records=total_records,
                    valid_records=len(valid_records),
                    invalid_records=len(invalid_records),
                )
                logger.exception("pipeline run failed", extra={"run_key": run_key})
                return self._result_from_run(run, report_path=self._report_path(run_key), reused_existing_run=False)

            return self._result_from_run(run, report_path=self._report_path(run_key), reused_existing_run=False)

    def _run_step(self, db: Session, run: PipelineRun, step_name: str, fn):
        def execute_once(attempt: int):
            # Persist each attempt so retries stay auditable.
            step = create_step_attempt(db, run_id=run.id, step_name=step_name, attempt=attempt)
            try:
                result = fn()
                finish_step_success(db, step)
                return result
            except Exception as exc:
                finish_step_failure(db, step, str(exc))
                raise

        try:
            return run_with_retries(
                lambda: execute_once(self._next_attempt(db, run.id, step_name)),
                max_retries=self.settings.max_step_retries,
                backoff_seconds=self.settings.retry_backoff_seconds,
                should_retry=lambda exc: self._is_retryable(step_name, exc),
            )
        except RetryExhaustedError as exc:
            raise RuntimeError(f"step '{step_name}' failed after retries: {exc}") from exc

    def _next_attempt(self, db: Session, run_id: int, step_name: str) -> int:
        stmt = (
            select(StepRun.attempt)
            .where(StepRun.run_id == run_id, StepRun.step_name == step_name)
            .order_by(StepRun.attempt.desc())
            .limit(1)
        )
        current = db.execute(stmt).scalar_one_or_none()
        if current is not None:
            return current + 1
        return 1

    def _is_retryable(self, step_name: str, exc: Exception) -> bool:
        # Ingest parse and missing file errors do not recover on retry.
        if step_name == "ingest" and isinstance(exc, (FileNotFoundError, json.JSONDecodeError)):
            return False
        return True

    def _ingest(self, run_date: date) -> list[dict[str, object]]:
        input_path = Path(self.settings.input_dir) / f"records-{run_date.isoformat()}.jsonl"
        return ingest_records(input_path)

    def _publish_outputs(
        self,
        *,
        run_key: str,
        run_date: date,
        valid_records: list[dict[str, object]],
        invalid_records: list[InvalidRecord],
        total_records: int,
    ) -> None:
        output_root = Path(self.settings.output_dir)
        publish_path = output_root / "published" / f"{run_key}.jsonl"
        dead_letter_path = output_root / "dead-letter" / f"{run_key}.jsonl"
        report_path = output_root / "reports" / f"{run_key}.json"

        write_jsonl(publish_path, valid_records)
        write_jsonl(
            dead_letter_path,
            [
                {
                    "record_index": invalid.record_index,
                    "reason": invalid.reason,
                    "record": invalid.record,
                }
                for invalid in invalid_records
            ],
        )
        write_json(
            report_path,
            {
                "run_key": run_key,
                "run_date": run_date.isoformat(),
                "total_records": total_records,
                "valid_records": len(valid_records),
                "invalid_records": len(invalid_records),
                "published_output": str(publish_path),
                "dead_letter_output": str(dead_letter_path),
            },
        )

    def _report_path(self, run_key: str) -> str:
        return str(Path(self.settings.output_dir) / "reports" / f"{run_key}.json")

    def _result_from_run(self, run: PipelineRun, report_path: str, reused_existing_run: bool) -> PipelineResult:
        return PipelineResult(
            run_id=run.id,
            run_key=run.run_key,
            run_date=run.run_date,
            trigger_source=run.trigger_source,
            status=run.status,
            total_records=run.total_records,
            valid_records=run.valid_records,
            invalid_records=run.invalid_records,
            report_path=report_path,
            reused_existing_run=reused_existing_run,
        )
