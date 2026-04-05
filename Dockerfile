FROM python:3.12-slim-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app
COPY . /app

# Install compilation dependencies (needed for some Python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install runtime dependencies (web + cli + chain, no dev)
RUN uv sync --frozen --no-dev --extra web --extra cli --extra chain

# --- Production stage (no build tools) ---
FROM python:3.12-slim-bookworm

ARG VERSION=unknown
LABEL version="${VERSION}"

ENV PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Copy only the venv and source code from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY --from=builder /app/pyproject.toml /app/pyproject.toml

# Run as non-root user
RUN useradd -r -u 1000 -s /sbin/nologin app \
    && chown -R app:app /app
USER app

COPY --chown=app:app scripts/quickstart.sh /app/scripts/quickstart.sh
COPY --chown=app:app scripts/updater.sh /app/scripts/updater.sh
COPY --chown=app:app docker-compose.yml /app/docker-compose.yml
COPY --chown=app:app secrets.env.example /app/secrets.env.example

EXPOSE 8000

CMD ["python", "-m", "micromech", "run"]
