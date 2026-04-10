FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ARG VERSION=unknown
LABEL version="${VERSION}"

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY . /app

# Install compilation dependencies (needed for some Python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install runtime dependencies (web + cli + chain + tasks + llm, no dev)
# llm extra installs llama-cpp-python (compiled from source) and huggingface-hub.
# The LLM model itself is NOT bundled — it downloads to data/models/ on first use.
RUN uv sync --frozen --no-dev --extra web --extra cli --extra chain --extra tasks --extra llm

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8090

CMD ["python", "-m", "micromech", "run"]
