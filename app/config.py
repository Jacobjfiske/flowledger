from dataclasses import dataclass
import os

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    app_name: str
    database_url: str
    log_level: str
    input_dir: str
    output_dir: str
    max_step_retries: int
    retry_backoff_seconds: float
    schedule_hour_utc: int
    schedule_minute_utc: int


def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("APP_NAME", "flowledger"),
        database_url=os.getenv("DATABASE_URL", "sqlite:///./pipeline.db"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        input_dir=os.getenv("INPUT_DIR", "./data/input"),
        output_dir=os.getenv("OUTPUT_DIR", "./outputs"),
        max_step_retries=int(os.getenv("MAX_STEP_RETRIES", "2")),
        retry_backoff_seconds=float(os.getenv("RETRY_BACKOFF_SECONDS", "1")),
        schedule_hour_utc=int(os.getenv("SCHEDULE_HOUR_UTC", "2")),
        schedule_minute_utc=int(os.getenv("SCHEDULE_MINUTE_UTC", "0")),
    )
