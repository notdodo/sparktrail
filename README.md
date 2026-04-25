# SparkTrail

Query CloudTrail JSON logs in S3 from an interactive Python shell.

SparkTrail uses DuckDB instead of PySpark. That removes the Spark/Hadoop/AWS SDK jar compatibility problem: runtime dependencies are Python packages in `uv.lock`, while DuckDB's `httpfs` and `json` extensions are loaded for the installed DuckDB version.

## Requirements

- `uv`
- AWS SSO credentials for the target account
- No Java runtime

Run SSO login before opening SparkTrail:

```sh
aws sso login --profile audit
```

## Install

```sh
scripts/install
```

## Usage

Start an interactive IPython shell with the project helpers loaded:

```sh
AWS_PROFILE=audit scripts/sparktrail
```

Link a CloudTrail prefix and query it with SQL:

```python
db = link_s3("audit-cloudtrail-logs/AWSLogs/")
db.sql("SELECT DISTINCT record.eventName FROM cloudtrail_records LIMIT 10").show()
```

`link_s3` creates a `cloudtrail_records` view with one row per CloudTrail record. The original event is available as the `record` struct, and the source object path is available as `filename`.

```python
db.sql("""
    SELECT
        record.eventSource,
        record.eventName,
        count(*) AS events
    FROM cloudtrail_records
    GROUP BY 1, 2
    ORDER BY events DESC
    LIMIT 20
""").show()
```

For generic partitioned JSON logs, use `link_json_s3`:

```python
db = link_json_s3("audit-cloudtrail-logs/AWSLogs/", view_name="raw_logs")
db.sql("SELECT * FROM raw_logs LIMIT 10").show()
```

Paths can be passed as `bucket/prefix`, `s3://bucket/prefix`, or a full glob. If the path is a prefix, SparkTrail appends `**/*.json*`.

## Local logs

To query local CloudTrail files without AWS credentials, use `link_local`:

```python
db = link_local("/path/to/logs/")
db.sql("SELECT record.eventName, record.sourceIPAddress FROM cloudtrail_records LIMIT 10").show()
```

For generic local JSON logs, use `link_json_local`:

```python
db = link_json_local("/path/to/logs/", view_name="raw_logs")
db.sql("SELECT * FROM raw_logs LIMIT 10").show()
```

Paths follow the same rules as S3: a directory gets `**/*.json*` appended, or pass an explicit glob like `/path/to/logs/*.json.gz`.

## Docker

Build the image:

```sh
docker build -t sparktrail .
```

Run it with your AWS config mounted read-only:

```sh
docker run --rm -it \
  -e AWS_PROFILE=audit \
  -v "$HOME/.aws:/root/.aws:ro" \
  sparktrail
```

Run `aws sso login --profile audit` on the host first so the container can read the cached SSO credentials.

To query local logs without AWS credentials, mount the log directory and use `link_local`:

```sh
docker run --rm -it \
  -v "/path/to/logs:/data/logs:ro" \
  sparktrail
```

```python
db = link_local("/data/logs/")
```

## Updates

- `uv.lock` pins the Python dependency graph.
- Dependabot opens `uv` PRs for Python dependency updates.
- Dependabot opens Docker PRs for the base and tool images.
- There are no Spark, Hadoop, or AWS SDK jar coordinates to keep manually aligned.
