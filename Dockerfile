# syntax=docker/dockerfile:1
FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /bin/

RUN groupadd --gid 10001 sparktrail \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin sparktrail

ENV HOME="/home/sparktrail" \
    PATH="/opt/sparktrail/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    SPARKTRAIL_SKIP_SYNC=1 \
    UV_PROJECT_ENVIRONMENT=/opt/sparktrail/.venv

WORKDIR /opt/sparktrail

COPY pyproject.toml uv.lock README.md main.py ./
RUN uv sync --frozen --no-dev \
    && uv run --frozen --no-dev python -c "import duckdb; con = duckdb.connect(); con.install_extension('httpfs'); con.install_extension('json')" \
    && chown -R sparktrail:sparktrail /home/sparktrail

COPY scripts ./scripts

USER 10001:10001

HEALTHCHECK NONE

ENTRYPOINT ["./scripts/sparktrail"]
