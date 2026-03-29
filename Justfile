set shell := ["bash", "-uc"]

# Install dependencies (including dev)
install:
    uv sync --all-extras

# Format code
format:
    uv run ruff format src/ tests/
    uv run ruff check --fix src/ tests/

# Check code (static analysis)
check: types
    uv run ruff check src/ tests/

# Type check
types:
    uv run mypy src/

# Run tests with coverage
test:
    uv run pytest --cov=src/micromech --cov-report=term-missing tests/

# Run unit tests only
test-unit:
    uv run pytest tests/unit/ -v

# Run integration tests only
test-integration:
    uv run pytest tests/integration/ -v -s

# Build package
build:
    uv build

# Run the server
run:
    uv run micromech run

# Run the web UI
web port="8000":
    uv run micromech web --port {{port}}
