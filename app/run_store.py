from datetime import UTC, datetime
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db_models import DeadLetterRecord, PipelineRun, PublishedRecord, StepRun
from app.schemas import InvalidRecord


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def get_run_by_key(db: Session, run_key: str) -> PipelineRun | None:
    stmt = select(PipelineRun).where(PipelineRun.run_key == run_key)
    return db.execute(stmt).scalar_one_or_none()


def create_or_get_run(db: Session, *, run_key: str, run_date, trigger_source: str) -> tuple[PipelineRun, bool]:
    run = PipelineRun(run_key=run_key, run_date=run_date, trigger_source=trigger_source, status="queued")
    db.add(run)
    try:
        db.commit()
    except IntegrityError:
        # Unique run_key enforces idempotent run creation.
        db.rollback()
        existing = get_run_by_key(db, run_key)
        if existing:
            return existing, False
        raise

    db.refresh(run)
    return run, True


def reset_failed_run_state(db: Session, run: PipelineRun) -> None:
    db.execute(delete(StepRun).where(StepRun.run_id == run.id))
    db.execute(delete(DeadLetterRecord).where(DeadLetterRecord.run_id == run.id))
    db.execute(delete(PublishedRecord).where(PublishedRecord.run_id == run.id))

    run.status = "queued"
    run.error = None
    run.completed_at = None
    run.total_records = 0
    run.valid_records = 0
    run.invalid_records = 0
    db.commit()


def mark_run_running(db: Session, run: PipelineRun) -> None:
    run.status = "running"
    run.started_at = utc_now()
    run.error = None
    db.commit()


def mark_run_succeeded(
    db: Session,
    run: PipelineRun,
    *,
    total_records: int,
    valid_records: int,
    invalid_records: int,
) -> None:
    run.status = "succeeded"
    run.total_records = total_records
    run.valid_records = valid_records
    run.invalid_records = invalid_records
    run.completed_at = utc_now()
    run.error = None
    db.commit()


def mark_run_failed(
    db: Session,
    run: PipelineRun,
    *,
    error: str,
    total_records: int = 0,
    valid_records: int = 0,
    invalid_records: int = 0,
) -> None:
    run.status = "failed"
    run.error = error
    run.total_records = total_records
    run.valid_records = valid_records
    run.invalid_records = invalid_records
    run.completed_at = utc_now()
    db.commit()


def create_step_attempt(db: Session, *, run_id: int, step_name: str, attempt: int) -> StepRun:
    step = StepRun(run_id=run_id, step_name=step_name, attempt=attempt, status="started", started_at=utc_now())
    db.add(step)
    db.commit()
    db.refresh(step)
    return step


def finish_step_success(db: Session, step: StepRun) -> None:
    finished_at = utc_now()
    step.status = "succeeded"
    step.completed_at = finished_at
    step.duration_ms = (finished_at - step.started_at).total_seconds() * 1000
    step.error = None
    db.commit()


def finish_step_failure(db: Session, step: StepRun, error: str) -> None:
    finished_at = utc_now()
    step.status = "failed"
    step.completed_at = finished_at
    step.duration_ms = (finished_at - step.started_at).total_seconds() * 1000
    step.error = error
    db.commit()


def store_dead_letters(db: Session, *, run_id: int, invalid_records: list[InvalidRecord]) -> None:
    for invalid in invalid_records:
        db.add(
            DeadLetterRecord(
                run_id=run_id,
                record_index=invalid.record_index,
                raw_record=str(invalid.record),
                reason=invalid.reason,
            )
        )
    db.commit()


def store_published_records(db: Session, *, run_id: int, records: list[dict[str, object]]) -> None:
    existing_stmt = select(PublishedRecord.record_key).where(PublishedRecord.run_id == run_id)
    existing_keys = set(db.execute(existing_stmt).scalars().all())

    for record in records:
        record_key = str(record["record_key"])
        # Skip duplicates when publish is retried.
        if record_key in existing_keys:
            continue
        db.add(PublishedRecord(run_id=run_id, record_key=record_key, payload=str(record)))
    db.commit()
