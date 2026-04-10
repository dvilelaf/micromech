# ── Stage 1: builder ─────────────────────────────────────────────────────────
# Has full build toolchain to compile llama-cpp-python from source.
# The compiled .venv is copied to the lean runtime stage below.
FROM python:3.12-slim-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy only dependency files first so the expensive uv sync layer is cached
# independently of source code changes.
COPY pyproject.toml uv.lock ./

# Install all dependencies but not the project itself (no src/ needed yet).
# GGML_NATIVE=OFF disables CPU-specific optimizations that break QEMU-based
# cross-compilation for arm64. The resulting binary still works on arm64 hardware.
RUN CMAKE_ARGS="-DGGML_NATIVE=OFF" \
    uv sync --frozen --no-dev --no-install-project \
    --extra web --extra cli --extra chain --extra tasks --extra llm

# Copy the rest of the source code (does NOT invalidate the compiled .venv cache)
COPY . /app

# Install the project itself (fast — all dependencies already cached above)
RUN uv sync --frozen --no-dev \
    --extra web --extra cli --extra chain --extra tasks --extra llm

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
# Lean image — no compiler, no cmake. Only the C++ runtime libs that
# llama-cpp-python's compiled extension needs at runtime.
FROM python:3.12-slim-bookworm

ARG VERSION=unknown
LABEL version="${VERSION}"

ENV PYTHONUNBUFFERED=1
# Redirect HuggingFace cache to /app/data so non-root users can write to it.
# Users should mount /app/data as a volume so the model persists across restarts.
ENV HF_HOME=/app/data/.hf_cache

# libstdc++6: C++ standard library (llama.cpp compiled extension)
# libgomp1:   OpenMP runtime (llama.cpp uses it for parallel inference)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app /app

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8090

# Note: docker-compose overrides this with --host 0.0.0.0 so the port is
# reachable inside the container (the host-side binding stays loopback-only).
CMD ["python", "-m", "micromech", "run"]
