import json
from pathlib import Path

from app.schemas import InvalidRecord


def ingest_records(input_path: Path) -> list[dict[str, object]]:
    if not input_path.exists():
        raise FileNotFoundError(f"input file not found: {input_path}")

    records: list[dict[str, object]] = []
    with input_path.open("r", encoding="utf-8") as infile:
        for line in infile:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def transform_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    transformed: list[dict[str, object]] = []
    for record in records:
        transformed.append(
            {
                "record_key": str(record.get("record_key", "")).strip(),
                "full_name": str(record.get("full_name", "")).strip(),
                "email": str(record.get("email", "")).strip().lower(),
                "age": record.get("age"),
                "source": str(record.get("source", "unknown")).strip().lower(),
            }
        )
    return transformed


def validate_records(records: list[dict[str, object]]) -> tuple[list[dict[str, object]], list[InvalidRecord]]:
    valid: list[dict[str, object]] = []
    invalid: list[InvalidRecord] = []

    for index, record in enumerate(records):
        record_key = str(record.get("record_key", "")).strip()
        full_name = str(record.get("full_name", "")).strip()
        email = str(record.get("email", "")).strip().lower()

        age_raw = record.get("age")
        age_value: int | None = None
        try:
            age_value = int(age_raw)
        except (TypeError, ValueError):
            invalid.append(InvalidRecord(index, record, "age must be an integer"))
            continue

        if not record_key:
            invalid.append(InvalidRecord(index, record, "record_key is required"))
            continue

        if not full_name:
            invalid.append(InvalidRecord(index, record, "full_name is required"))
            continue

        if "@" not in email or "." not in email:
            invalid.append(InvalidRecord(index, record, "email format is invalid"))
            continue

        if age_value < 18 or age_value > 120:
            invalid.append(InvalidRecord(index, record, "age must be between 18 and 120"))
            continue

        age_group = "18-34" if age_value <= 34 else "35-54" if age_value <= 54 else "55+"
        valid.append(
            {
                "record_key": record_key,
                "full_name": full_name,
                "email": email,
                "age": age_value,
                "age_group": age_group,
                "source": str(record.get("source", "unknown")),
            }
        )

    return valid, invalid


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as outfile:
        for row in rows:
            outfile.write(json.dumps(row, sort_keys=True))
            outfile.write("\n")


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as outfile:
        json.dump(payload, outfile, indent=2, sort_keys=True)
        outfile.write("\n")
