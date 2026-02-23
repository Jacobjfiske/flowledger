from app.step_logic import transform_records, validate_records


def test_transform_normalizes_email_and_source() -> None:
    raw = [
        {
            "record_key": "A-1",
            "full_name": "Ada Lovelace",
            "email": " ADA@EXAMPLE.COM ",
            "age": "30",
            "source": "  WEB ",
        }
    ]

    transformed = transform_records(raw)

    assert transformed[0]["email"] == "ada@example.com"
    assert transformed[0]["source"] == "web"


def test_validate_splits_valid_and_invalid() -> None:
    records = [
        {
            "record_key": "A-1",
            "full_name": "Ada Lovelace",
            "email": "ada@example.com",
            "age": "30",
            "source": "web",
        },
        {
            "record_key": "",
            "full_name": "",
            "email": "broken",
            "age": "nope",
            "source": "web",
        },
    ]

    valid, invalid = validate_records(records)

    assert len(valid) == 1
    assert valid[0]["age_group"] == "18-34"
    assert len(invalid) == 1
    assert invalid[0].reason == "age must be an integer"
