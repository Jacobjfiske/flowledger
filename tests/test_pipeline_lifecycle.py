from datetime import date
import json
from pathlib import Path

from sqlalchemy import select

from app.db_models import DeadLetterRecord, PipelineRun, StepRun


def write_input_file(root: Path, run_date: date) -> None:
    input_file = root / "data" / "input" / f"records-{run_date.isoformat()}.jsonl"
    rows = [
        {
            "record_key": "1",
            "full_name": "Grace Hopper",
            "email": "grace@example.com",
            "age": 36,
            "source": "web",
        },
        {
            "record_key": "2",
            "full_name": "",
            "email": "bad-email",
            "age": 15,
            "source": "partner",
        },
    ]

    with input_file.open("w", encoding="utf-8") as outfile:
        for row in rows:
            outfile.write(json.dumps(row))
            outfile.write("\n")


def test_full_run_lifecycle_and_idempotency(runner, temp_workspace: Path) -> None:
    run_date = date(2026, 2, 22)
    run_key = "daily-2026-02-22"
    write_input_file(temp_workspace, run_date)

    first = runner.run(run_date=run_date, run_key=run_key)
    second = runner.run(run_date=run_date, run_key=run_key)

    assert first.status == "succeeded"
    assert first.total_records == 2
    assert first.valid_records == 1
    assert first.invalid_records == 1
    assert first.trigger_source == "manual"
    assert first.reused_existing_run is False

    assert second.reused_existing_run is True
    assert second.run_id == first.run_id

    report_path = temp_workspace / "outputs" / "reports" / f"{run_key}.json"
    dead_letter_path = temp_workspace / "outputs" / "dead-letter" / f"{run_key}.jsonl"
    published_path = temp_workspace / "outputs" / "published" / f"{run_key}.jsonl"

    assert report_path.exists()
    assert dead_letter_path.exists()
    assert published_path.exists()

    with runner.session_factory() as db:
        run = db.execute(select(PipelineRun).where(PipelineRun.run_key == run_key)).scalar_one()
        assert run.status == "succeeded"
        assert run.trigger_source == "manual"

        steps = db.execute(select(StepRun).where(StepRun.run_id == run.id)).scalars().all()
        step_names = sorted(step.step_name for step in steps)
        assert step_names == ["ingest", "publish_report", "transform", "validate"]

        dead_letters = db.execute(select(DeadLetterRecord).where(DeadLetterRecord.run_id == run.id)).scalars().all()
        assert len(dead_letters) == 1


def test_run_trigger_source_persisted_for_scheduled_runs(runner, temp_workspace: Path) -> None:
    run_date = date(2026, 2, 23)
    run_key = "scheduled-2026-02-23"
    write_input_file(temp_workspace, run_date)

    result = runner.run(run_date=run_date, run_key=run_key, trigger_source="scheduled")
    assert result.status == "succeeded"
    assert result.trigger_source == "scheduled"

    with runner.session_factory() as db:
        run = db.execute(select(PipelineRun).where(PipelineRun.run_key == run_key)).scalar_one()
        assert run.trigger_source == "scheduled"


def test_failed_run_can_be_retried_with_same_run_key(runner, temp_workspace: Path) -> None:
    run_date = date(2026, 2, 24)
    run_key = "daily-2026-02-24"

    first = runner.run(run_date=run_date, run_key=run_key)
    assert first.status == "failed"

    write_input_file(temp_workspace, run_date)
    second = runner.run(run_date=run_date, run_key=run_key)
    assert second.status == "succeeded"
    assert second.reused_existing_run is False

    with runner.session_factory() as db:
        run = db.execute(select(PipelineRun).where(PipelineRun.run_key == run_key)).scalar_one()
        assert run.status == "succeeded"

        steps = db.execute(select(StepRun).where(StepRun.run_id == run.id)).scalars().all()
        step_names = sorted(step.step_name for step in steps)
        assert step_names == ["ingest", "publish_report", "transform", "validate"]


def test_ingest_file_not_found_is_not_retried(runner) -> None:
    run_date = date(2026, 2, 25)
    run_key = "daily-2026-02-25"

    result = runner.run(run_date=run_date, run_key=run_key)
    assert result.status == "failed"

    with runner.session_factory() as db:
        run = db.execute(select(PipelineRun).where(PipelineRun.run_key == run_key)).scalar_one()
        attempts = db.execute(
            select(StepRun).where(StepRun.run_id == run.id, StepRun.step_name == "ingest")
        ).scalars().all()
        assert len(attempts) == 1
