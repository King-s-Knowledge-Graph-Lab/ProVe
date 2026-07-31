# syntax=docker/dockerfile:1
#
# Build context must be the repo root, not this directory, because prove-api
# depends on the sibling `prove-shared` package via a path source
# ([tool.uv.sources] in prove-api/pyproject.toml). Build with:
#
#   docker build -f prove-api/Dockerfile -t prove-api .
#
FROM python:3.10.16-slim-bookworm AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy both packages, preserving their relative layout so the "../prove-shared"
# path source resolves the same way it does outside Docker.
COPY prove-shared/ /app/prove-shared/
COPY prove-api/ /app/prove-api/

WORKDIR /app/prove-api

# --frozen: install exactly what's in the committed uv.lock, fail instead of
# re-resolving if pyproject.toml and the lock have drifted apart.
RUN uv sync --frozen --no-dev

ENV PATH="/app/prove-api/.venv/bin:${PATH}"

EXPOSE 8000

# app.py does `from custom_decorators import ...` (no `api.` package prefix),
# which resolves only because gunicorn adds the current working directory to
# sys.path — so the container's CWD must stay /app/prove-api.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "4", "app:app"]
