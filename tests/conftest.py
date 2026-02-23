from collections.abc import Generator
from pathlib import Path

import pytest

from app.config import Settings
from app.database import build_session_factory
from app.pipeline import PipelineRunner


@pytest.fixture()
def temp_workspace(tmp_path: Path) -> Path:
    (tmp_path / "data" / "input").mkdir(parents=True, exist_ok=True)
    (tmp_path / "outputs").mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture()
def test_settings(temp_workspace: Path) -> Settings:
    return Settings(
        app_name="flowledger",
        database_url=f"sqlite:///{temp_workspace / 'test.db'}",
        log_level="INFO",
        input_dir=str(temp_workspace / "data" / "input"),
        output_dir=str(temp_workspace / "outputs"),
        max_step_retries=1,
        retry_backoff_seconds=0,
        schedule_hour_utc=2,
        schedule_minute_utc=0,
    )


@pytest.fixture()
def runner(test_settings: Settings) -> Generator[PipelineRunner, None, None]:
    session_factory = build_session_factory(test_settings.database_url)
    yield PipelineRunner(test_settings, session_factory)
