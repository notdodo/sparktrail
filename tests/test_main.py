from __future__ import annotations

import json
from typing import TYPE_CHECKING

import duckdb

from main import create_cloudtrail_view, create_json_view, normalize_s3_uri

if TYPE_CHECKING:
    from pathlib import Path


def test_normalize_s3_uri_adds_scheme_and_recursive_json_glob() -> None:
    assert normalize_s3_uri("audit-cloudtrail-logs/AWSLogs/") == "s3://audit-cloudtrail-logs/AWSLogs/**/*.json*"


def test_normalize_s3_uri_preserves_explicit_glob() -> None:
    assert normalize_s3_uri("s3://bucket/AWSLogs/**/*.json.gz") == "s3://bucket/AWSLogs/**/*.json.gz"


def test_create_cloudtrail_view_exposes_record_struct(tmp_path: Path) -> None:
    source = tmp_path / "cloudtrail.json"
    source.write_text(
        json.dumps(
            {
                "Records": [
                    {"eventName": "AssumeRole", "eventSource": "sts.amazonaws.com"},
                    {"eventName": "ListBuckets", "eventSource": "s3.amazonaws.com"},
                ],
            },
        ),
        encoding="utf-8",
    )

    connection = duckdb.connect()
    create_cloudtrail_view(connection, str(source))

    rows = connection.sql(
        """
        SELECT record.eventName
        FROM cloudtrail_records
        ORDER BY record.eventName
        """,
    ).fetchall()

    assert rows == [("AssumeRole",), ("ListBuckets",)]


def test_create_json_view_reads_generic_json(tmp_path: Path) -> None:
    source = tmp_path / "logs.json"
    source.write_text(json.dumps([{"event": "login"}, {"event": "logout"}]), encoding="utf-8")

    connection = duckdb.connect()
    create_json_view(connection, str(source), view_name="logs")

    assert connection.sql("SELECT count(*) FROM logs").fetchone() == (2,)
