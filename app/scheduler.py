from datetime import UTC, datetime
import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.pipeline import PipelineRunner


logger = logging.getLogger(__name__)


def _run_daily_pipeline(settings: Settings, session_factory: sessionmaker[Session]) -> None:
    run_date = datetime.now(UTC).date()
    run_key = f"scheduled-{run_date.isoformat()}"

    runner = PipelineRunner(settings, session_factory)
    result = runner.run(run_date=run_date, run_key=run_key, trigger_source="scheduled")
    if result.status == "failed":
        logger.error(
            "scheduled pipeline run failed",
            extra={
                "run_key": result.run_key,
                "status": result.status,
                "reused_existing_run": result.reused_existing_run,
            },
        )
        return
    logger.info(
        "scheduled pipeline run completed",
        extra={
            "run_key": result.run_key,
            "status": result.status,
            "reused_existing_run": result.reused_existing_run,
        },
    )


def start_scheduler(settings: Settings, session_factory: sessionmaker[Session], *, run_now: bool = False) -> None:
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        _run_daily_pipeline,
        "cron",
        args=[settings, session_factory],
        hour=settings.schedule_hour_utc,
        minute=settings.schedule_minute_utc,
        id="daily_pipeline",
        replace_existing=True,
    )

    logger.info(
        "scheduler started",
        extra={
            "schedule_hour_utc": settings.schedule_hour_utc,
            "schedule_minute_utc": settings.schedule_minute_utc,
        },
    )

    if run_now:
        _run_daily_pipeline(settings, session_factory)

    scheduler.start()
