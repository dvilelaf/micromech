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

# Run Anvil E2E test (requires Anvil fork running on port 18545)
test-anvil:
    #!/usr/bin/env bash
    set -e
    RPC_URL=$(grep -m1 'gnosis_rpc=' /media/david/DATA/repos/triton/secrets.env | cut -d= -f2 | cut -d, -f1)
    if [ -z "$RPC_URL" ]; then
        echo "No gnosis_rpc found"
        exit 1
    fi
    ~/.foundry/bin/anvil --fork-url "$RPC_URL" --port 18545 --auto-impersonate --silent &
    ANVIL_PID=$!
    trap "kill $ANVIL_PID 2>/dev/null" EXIT
    sleep 3
    echo "Anvil running (PID $ANVIL_PID)"
    ANVIL_URL=http://localhost:18545 uv run pytest tests/integration/test_anvil_e2e.py -v -s
    echo "Anvil E2E tests passed"

# Build package
build:
    uv build

# Run the server
run:
    uv run micromech run

# Run the web UI
web port="8000":
    uv run micromech web --port {{port}}
