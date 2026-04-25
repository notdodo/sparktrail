# syntax=docker/dockerfile:1
FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /bin/

ENV PATH="/opt/sparktrail/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    SPARKTRAIL_SKIP_SYNC=1 \
    UV_PROJECT_ENVIRONMENT=/opt/sparktrail/.venv

WORKDIR /opt/sparktrail

COPY pyproject.toml uv.lock README.md main.py ./
RUN uv sync --frozen --no-dev \
    && uv run --frozen --no-dev python -c "import duckdb; con = duckdb.connect(); con.install_extension('httpfs'); con.install_extension('json')"

COPY scripts ./scripts

ENTRYPOINT ["./scripts/sparktrail"]
