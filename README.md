# FlowLedger

Run keyed automation pipeline with reliable scheduling and execution tracking.

## Stack
- Python
- PostgreSQL (SQLite in tests/local fallback)
- SQLAlchemy
- pytest
- Docker Compose
- GitHub Actions

## What it does
- Reads daily JSONL input records (`ingest`).
- Normalizes fields (`transform`).
- Applies data quality rules (`validate`).
- Publishes valid records and writes dead-letter + run report outputs (`publish/report`).
- Stores run and step state in PostgreSQL (or SQLite for local tests and metrics runs).

## Entrypoints
- `python -m app.main run ...`
- `python -m app.main schedule`

## Architecture
- `app/main.py`: CLI entrypoint with `run` and `schedule` modes.
- `app/scheduler.py`: daily UTC scheduler job.
- `app/pipeline.py`: orchestration flow and retry execution.
- `app/step_logic.py`: pure step logic for ingest/transform/validate/publish file output.
- `app/run_store.py`: DB persistence for runs, steps, published records, dead letters.
- `app/db_models.py`: SQLAlchemy models.

Data path:
1. Input file: `data/input/records-<YYYY-MM-DD>.jsonl`
2. Valid output: `outputs/published/<run_key>.jsonl`
3. Invalid output: `outputs/dead-letter/<run_key>.jsonl`
4. Run report: `outputs/reports/<run_key>.json`

Note: `data/input/records-2026-02-22.jsonl` is an example input file for local runs.

## Run
Local setup:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Manual run (debug/local):
```bash
python -m app.main run --run-date 2026-02-22 --run-key manual-2026-02-22 --trigger-source manual
```

Start daily scheduler (UTC):
```bash
python -m app.main schedule
```

Start scheduler and run once immediately:
```bash
python -m app.main schedule --run-now
```

Run tests:
```bash
pytest -q
```

Docker Compose (PostgreSQL + pipeline + scheduler):
```bash
cp .env.example .env
docker compose up -d postgres scheduler
docker compose run --rm pipeline python -m app.main run --run-date 2026-02-22 --run-key manual-2026-02-22 --trigger-source manual
```

## Test
```bash
pytest -q
```

## CI
- GitHub Actions workflow: `.github/workflows/ci.yml`
- Runs `pytest -q` on every `push` and `pull_request`.

## Reliability behavior
- Idempotent run key: `run_key` is unique in `pipeline_runs`. Reusing a key returns the existing run instead of duplicating work.
- Retry per step: each step is retried with linear backoff (`MAX_STEP_RETRIES`, `RETRY_BACKOFF_SECONDS`).
- Persistent observability: step attempts, statuses, durations, and errors are stored in `step_runs`.
- Dead-letter path: invalid records are stored in DB (`dead_letter_records`) and filesystem output (`outputs/dead-letter/`).
- Trigger metadata: each run stores `trigger_source` (`manual` or `scheduled`) to separate operational runs from local debugging runs.

## Current limits
- Scheduler is process-based. If scheduler service is down at scheduled time, no catch-up run is triggered automatically.
- Report/metrics are file + DB based; no dashboard service in MVP.
- Schema migrations are not added yet (`create_all()` only).

## Naming
- Repository and project name: `flowledger` / `FlowLedger`
