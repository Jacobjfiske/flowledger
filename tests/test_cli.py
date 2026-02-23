from datetime import date
import json
import os
from pathlib import Path
import subprocess
import sys


def _base_env(tmp_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite:///{tmp_path / 'cli.db'}"
    env["INPUT_DIR"] = str(tmp_path / "data" / "input")
    env["OUTPUT_DIR"] = str(tmp_path / "outputs")
    env["MAX_STEP_RETRIES"] = "1"
    env["RETRY_BACKOFF_SECONDS"] = "0"
    return env


def test_cli_returns_nonzero_on_pipeline_failure(tmp_path: Path) -> None:
    (tmp_path / "data" / "input").mkdir(parents=True, exist_ok=True)
    env = _base_env(tmp_path)

    proc = subprocess.run(
        [sys.executable, "-m", "app.main", "run", "--run-date", "2026-02-26", "--run-key", "daily-2026-02-26"],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 1
    assert "status=failed" in proc.stdout


def test_cli_returns_zero_on_success(tmp_path: Path) -> None:
    run_date = date(2026, 2, 27)
    input_dir = tmp_path / "data" / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    input_file = input_dir / f"records-{run_date.isoformat()}.jsonl"
    with input_file.open("w", encoding="utf-8") as outfile:
        outfile.write(json.dumps({"record_key": "1", "full_name": "Ada Lovelace", "email": "ada@example.com", "age": 31}))
        outfile.write("\n")

    env = _base_env(tmp_path)
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "app.main",
            "run",
            "--run-date",
            run_date.isoformat(),
            "--run-key",
            "daily-2026-02-27",
            "--trigger-source",
            "manual",
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0
    assert "status=succeeded" in proc.stdout
