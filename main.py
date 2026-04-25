from __future__ import annotations

import os
from typing import TYPE_CHECKING

import boto3
import duckdb

if TYPE_CHECKING:
    from pathlib import Path

DEFAULT_CLOUDTRAIL_VIEW = "cloudtrail_records"
DEFAULT_JSON_VIEW = "json_logs"
DEFAULT_JSON_GLOB = "**/*.json*"
JSON_SUFFIXES = (".json", ".json.gz", ".jsonl", ".jsonl.gz", ".ndjson", ".ndjson.gz")


def get_sso_credentials(profile_name: str | None = None, region_name: str | None = None) -> dict[str, str]:
    resolved_profile_name = profile_name or os.environ.get("AWS_PROFILE")
    if not resolved_profile_name:
        msg = "Set AWS_PROFILE or pass profile_name=... before connecting to S3."
        raise RuntimeError(msg)

    session = boto3.session.Session(profile_name=resolved_profile_name, region_name=region_name)
    credentials = session.get_credentials()
    if credentials is None:
        msg = f"No AWS credentials were found for profile {resolved_profile_name!r}. Run aws sso login first."
        raise RuntimeError(msg)

    frozen_credentials = credentials.get_frozen_credentials()
    if not frozen_credentials.access_key or not frozen_credentials.secret_key:
        msg = f"Incomplete AWS credentials were found for profile {resolved_profile_name!r}."
        raise RuntimeError(msg)

    resolved_region = region_name or session.region_name or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    result = {
        "key_id": frozen_credentials.access_key,
        "secret": frozen_credentials.secret_key,
        "region": resolved_region,
    }
    if frozen_credentials.token:
        result["session_token"] = frozen_credentials.token
    return result


def configure_duckdb(
    database: str | Path = ":memory:",
    *,
    profile_name: str | None = None,
    region_name: str | None = None,
) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(str(database))
    _load_extension(connection, "httpfs")
    _load_extension(connection, "json")
    _configure_s3_secret(connection, profile_name=profile_name, region_name=region_name)
    return connection


def link_s3(
    path: str,
    *,
    view_name: str = DEFAULT_CLOUDTRAIL_VIEW,
    profile_name: str | None = None,
    region_name: str | None = None,
    database: str | Path = ":memory:",
    connection: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyConnection:
    return link_cloudtrail_s3(
        path,
        view_name=view_name,
        profile_name=profile_name,
        region_name=region_name,
        database=database,
        connection=connection,
    )


def link_cloudtrail_s3(
    path: str,
    *,
    view_name: str = DEFAULT_CLOUDTRAIL_VIEW,
    profile_name: str | None = None,
    region_name: str | None = None,
    database: str | Path = ":memory:",
    connection: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyConnection:
    connection = connection or configure_duckdb(database, profile_name=profile_name, region_name=region_name)
    source_uri = normalize_s3_uri(path)
    create_cloudtrail_view(connection, source_uri, view_name=view_name)
    return connection


def link_local(
    path: str,
    *,
    view_name: str = DEFAULT_CLOUDTRAIL_VIEW,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyConnection:
    connection = connection or duckdb.connect()
    source_uri = normalize_local_uri(path)
    create_cloudtrail_view(connection, source_uri, view_name=view_name)
    return connection


def link_json_local(
    path: str,
    *,
    view_name: str = DEFAULT_JSON_VIEW,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyConnection:
    connection = connection or duckdb.connect()
    source_uri = normalize_local_uri(path)
    create_json_view(connection, source_uri, view_name=view_name)
    return connection


def link_json_s3(
    path: str,
    *,
    view_name: str = DEFAULT_JSON_VIEW,
    profile_name: str | None = None,
    region_name: str | None = None,
    database: str | Path = ":memory:",
    connection: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyConnection:
    connection = connection or configure_duckdb(database, profile_name=profile_name, region_name=region_name)
    source_uri = normalize_s3_uri(path)
    create_json_view(connection, source_uri, view_name=view_name)
    return connection


def create_cloudtrail_view(connection: duckdb.DuckDBPyConnection, source_uri: str, *, view_name: str = DEFAULT_CLOUDTRAIL_VIEW) -> None:
    query = f"""
        CREATE OR REPLACE VIEW {_sql_identifier(view_name)} AS
        SELECT
            unnest(Records) AS record,
            filename
        FROM read_json_auto({_sql_string(source_uri)}, filename = true, union_by_name = true, maximum_object_size = 268435456)
        """  # noqa: S608
    connection.execute(query)


def create_json_view(connection: duckdb.DuckDBPyConnection, source_uri: str, *, view_name: str = DEFAULT_JSON_VIEW) -> None:
    query = f"""
        CREATE OR REPLACE VIEW {_sql_identifier(view_name)} AS
        SELECT *
        FROM read_json_auto({_sql_string(source_uri)}, filename = true, union_by_name = true)
        """  # noqa: S608
    connection.execute(query)


def normalize_local_uri(path: str, *, default_glob: str = DEFAULT_JSON_GLOB) -> str:
    if _looks_like_glob(path) or path.endswith(JSON_SUFFIXES):
        return path
    return f"{path.rstrip('/')}/{default_glob}"


def normalize_s3_uri(path: str, *, default_glob: str = DEFAULT_JSON_GLOB) -> str:
    uri = path if path.startswith("s3://") else f"s3://{path.lstrip('/')}"
    if _looks_like_glob(uri) or uri.endswith(JSON_SUFFIXES):
        return uri
    return f"{uri.rstrip('/')}/{default_glob}"


def _configure_s3_secret(
    connection: duckdb.DuckDBPyConnection,
    *,
    profile_name: str | None = None,
    region_name: str | None = None,
) -> None:
    credentials = get_sso_credentials(profile_name=profile_name, region_name=region_name)
    options = [
        "TYPE s3",
        "PROVIDER config",
        f"KEY_ID {_sql_string(credentials['key_id'])}",
        f"SECRET {_sql_string(credentials['secret'])}",
        f"REGION {_sql_string(credentials['region'])}",
    ]
    session_token = credentials.get("session_token")
    if session_token:
        options.append(f"SESSION_TOKEN {_sql_string(session_token)}")

    connection.execute(f"CREATE OR REPLACE SECRET sparktrail_s3 ({', '.join(options)})")


def _load_extension(connection: duckdb.DuckDBPyConnection, name: str) -> None:
    try:
        connection.execute(f"LOAD {name}")
    except duckdb.Error:
        connection.execute(f"INSTALL {name}")
        connection.execute(f"LOAD {name}")


def _looks_like_glob(value: str) -> bool:
    return any(character in value for character in "*?[")


def _sql_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def _sql_string(value: str) -> str:
    return f"'{value.replace(chr(39), chr(39) * 2)}'"
