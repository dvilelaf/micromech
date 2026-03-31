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

# Run multi-chain E2E tests (forks Gnosis, Base, Ethereum, Polygon)
test-multichain:
    #!/usr/bin/env bash
    set -e
    SECRETS=/media/david/DATA/repos/triton/secrets.env
    ANVIL=~/.foundry/bin/anvil
    PIDS=()

    get_rpc() {
        grep -m1 "${1}=" "$SECRETS" | cut -d= -f2 | cut -d, -f1
    }

    start_fork() {
        local name=$1 port=$2 rpc=$3
        if [ -z "$rpc" ]; then
            echo "  $name: no RPC found, skipping"
            return
        fi
        lsof -ti:$port | xargs -r kill 2>/dev/null || true
        $ANVIL --fork-url "$rpc" --port $port --auto-impersonate --silent &
        PIDS+=($!)
        echo "  $name: forking on port $port (PID ${PIDS[-1]})"
    }

    cleanup() {
        for pid in "${PIDS[@]}"; do
            kill $pid 2>/dev/null || true
        done
    }
    trap cleanup EXIT

    echo "Starting Anvil forks..."
    start_fork "gnosis"   18545 "$(get_rpc gnosis_rpc)"
    start_fork "base"     18546 "$(get_rpc base_rpc)"
    start_fork "ethereum" 18547 "$(get_rpc ethereum_rpc)"
    start_fork "polygon"  18548 "$(get_rpc polygon_rpc)"
    start_fork "optimism" 18549 "$(get_rpc optimism_rpc)"
    start_fork "arbitrum" 18550 "$(get_rpc arbitrum_rpc)"
    start_fork "celo"     18551 "$(get_rpc celo_rpc)"

    sleep 5
    echo "Forks ready. Running multi-chain E2E tests..."

    ANVIL_GNOSIS=http://localhost:18545 \
    ANVIL_BASE=http://localhost:18546 \
    ANVIL_ETHEREUM=http://localhost:18547 \
    ANVIL_POLYGON=http://localhost:18548 \
    ANVIL_OPTIMISM=http://localhost:18549 \
    ANVIL_ARBITRUM=http://localhost:18550 \
    ANVIL_CELO=http://localhost:18551 \
    uv run pytest tests/integration/test_multichain_e2e.py -v -s

    echo "Multi-chain E2E tests passed"

# --- Anvil ---

# Start an Anvil fork of Gnosis (background, port 18545)
anvil-fork:
    #!/usr/bin/env bash
    set -e
    RPC_URL=$(grep -m1 'gnosis_rpc=' /media/david/DATA/repos/triton/secrets.env | cut -d= -f2 | cut -d, -f1)
    if [ -z "$RPC_URL" ]; then
        echo "Error: No gnosis_rpc found in triton/secrets.env"
        exit 1
    fi
    # Kill any existing Anvil on this port
    lsof -ti:18545 | xargs -r kill 2>/dev/null || true
    echo "Forking Gnosis from $RPC_URL..."
    ~/.foundry/bin/anvil --fork-url "$RPC_URL" --port 18545 --auto-impersonate --silent &
    sleep 3
    echo "Anvil fork running on http://localhost:18545 (PID $!)"

# Stop Anvil fork
anvil-stop:
    lsof -ti:18545 | xargs -r kill 2>/dev/null && echo "Anvil stopped" || echo "Anvil not running"

# Run micromech server against Anvil fork (sets gnosis_rpc to localhost:18545)
run-anvil:
    #!/usr/bin/env bash
    set -e
    if ! curl -s http://localhost:18545 -X POST -H 'Content-Type: application/json' \
        -d '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' | grep -q '0x64'; then
        echo "Error: Anvil fork not running on port 18545. Run 'just anvil-fork' first."
        exit 1
    fi
    echo "Starting micromech against Anvil fork..."
    gnosis_rpc=http://localhost:18545 testing=true uv run micromech run

# Build package
build:
    uv build

# Run the server
run:
    uv run micromech run

# Run the web dashboard
web port="8000":
    uv run micromech web --port {{port}}

# --- Docker ---

# Docker build (native platform)
docker-build:
    docker build -t micromech:latest .

# Docker build multi-arch (amd64 + arm64)
docker-build-multi:
    docker buildx build --platform linux/amd64,linux/arm64 -t micromech:latest .

# Start with docker compose
up:
    docker compose up -d

# Stop docker compose
down:
    docker compose down

# Force full rebuild (no cache) - use when dependencies change
rebuild-clean:
    uv lock
    docker compose build --no-cache
    docker compose up -d

# View logs
logs:
    docker compose logs -f --tail=100

# --- Release ---

# Validate git state (uncommitted changes, lockfile sync)
_validate-git-state:
    #!/usr/bin/env bash
    set -e

    if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "Error: uncommitted changes"
        git status --short
        exit 1
    fi

    if ! git diff --quiet HEAD @{upstream} 2>/dev/null; then
        echo "Error: unpushed commits"
        exit 1
    fi

    echo "Git state OK"

# Validate tag exists at HEAD
_validate-tag-at-head:
    #!/usr/bin/env bash
    set -e

    VERSION=$(grep -m1 'version = ' pyproject.toml | cut -d '"' -f2)
    TAG="v$VERSION"

    if ! git rev-parse "$TAG" >/dev/null 2>&1; then
        echo "Error: Tag $TAG does not exist!"
        echo "Create it with: git tag $TAG && git push origin $TAG"
        exit 1
    fi

    TAG_COMMIT=$(git rev-parse "$TAG")
    HEAD_COMMIT=$(git rev-parse HEAD)
    if [ "$TAG_COMMIT" != "$HEAD_COMMIT" ]; then
        echo "Error: Tag $TAG is not at HEAD"
        exit 1
    fi

# Run full release quality gate
release-check:
    #!/usr/bin/env bash
    set -e

    echo "Running quality checks..."
    just check
    echo "Running tests..."
    just test-unit

    echo "Validating git state..."
    just _validate-git-state
    echo "Validating release tag..."
    just _validate-tag-at-head

    VERSION=$(grep -m1 'version = ' pyproject.toml | cut -d '"' -f2)
    echo "All checks passed! Ready to publish v$VERSION."

# Create a new release tag
release version:
    #!/usr/bin/env bash
    set -e
    TAG="v{{version}}"
    echo "Creating tag $TAG..."
    git tag "$TAG"
    git push origin "$TAG"
    echo "Tag $TAG pushed."

# Build and publish docker image (run release-check first)
publish: release-check
    #!/usr/bin/env bash
    set -e

    VERSION=$(grep -m1 'version = ' pyproject.toml | cut -d '"' -f2)

    echo "Publishing version $VERSION to DockerHub (multi-arch)..."
    docker buildx build --no-cache --platform linux/amd64,linux/arm64 \
        --build-arg VERSION=$VERSION \
        -t dvilela/micromech:latest -t dvilela/micromech:$VERSION \
        -f Dockerfile --push .
    echo "Published version $VERSION to DockerHub (amd64 + arm64)"
