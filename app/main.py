import argparse
from datetime import date
import logging

from app.config import get_settings
from app.database import build_session_factory
from app.pipeline import PipelineRunner
from app.scheduler import start_scheduler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the automation pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run one pipeline execution")
    run_parser.add_argument("--run-date", required=True, help="Run date in YYYY-MM-DD format")
    run_parser.add_argument("--run-key", required=False, help="Idempotency key for this run")
    run_parser.add_argument(
        "--trigger-source",
        default="manual",
        choices=["manual", "scheduled"],
        help="Metadata label for how this run was triggered",
    )

    schedule_parser = subparsers.add_parser("schedule", help="start daily scheduler")
    schedule_parser.add_argument("--run-now", action="store_true", help="also run once immediately")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    session_factory = build_session_factory(settings.database_url)
    if args.command == "schedule":
        start_scheduler(settings, session_factory, run_now=args.run_now)
        return

    run_date = date.fromisoformat(args.run_date)
    run_key = args.run_key or run_date.isoformat()

    runner = PipelineRunner(settings, session_factory)
    result = runner.run(
        run_date=run_date,
        run_key=run_key,
        trigger_source=args.trigger_source,
    )

    print(
        "run_id={run_id} run_key={run_key} trigger={trigger} status={status} total={total} valid={valid} invalid={invalid} reused={reused} report={report}".format(
            run_id=result.run_id,
            run_key=result.run_key,
            trigger=result.trigger_source,
            status=result.status,
            total=result.total_records,
            valid=result.valid_records,
            invalid=result.invalid_records,
            reused=result.reused_existing_run,
            report=result.report_path,
        )
    )
    if result.status == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
