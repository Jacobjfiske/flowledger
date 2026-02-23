from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class InvalidRecord:
    record_index: int
    record: dict[str, object]
    reason: str


@dataclass(frozen=True)
class PipelineResult:
    run_id: int
    run_key: str
    run_date: date
    trigger_source: str
    status: str
    total_records: int
    valid_records: int
    invalid_records: int
    report_path: str | None
    reused_existing_run: bool
