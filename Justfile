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

# Security checks (gitleaks + bandit)
security:
    # Check for secrets in git history (requires gitleaks installed)
    gitleaks detect --source . -v
    # Check for common security issues in Python code
    uv run bandit -c pyproject.toml -r src/

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
    RPC_URL=$(grep -m1 'gnosis_rpc=' secrets.env | cut -d= -f2 | cut -d, -f1)
    if [ -z "$RPC_URL" ]; then
        echo "No gnosis_rpc found"
        exit 1
    fi
    ~/.foundry/bin/anvil --fork-url "$RPC_URL" --port 18545 --auto-impersonate --silent &
    ANVIL_PID=$!
    trap "kill $ANVIL_PID 2>/dev/null" EXIT
    sleep 3
    echo "Anvil running (PID $ANVIL_PID)"
    ANVIL_URL=http://localhost:18545 CHAINLIST_ENRICHMENT=false uv run pytest tests/integration/test_anvil_e2e.py -v -s
    echo "Anvil E2E tests passed"

# Run multi-chain E2E tests (forks Gnosis, Base, Ethereum, Polygon)
test-multichain:
    #!/usr/bin/env bash
    set -e
    SECRETS=secrets.env
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
    CHAINLIST_ENRICHMENT=false \
    uv run pytest tests/integration/test_multichain_e2e.py -v -s

    echo "Multi-chain E2E tests passed"

# --- Anvil (multi-chain) ---
# Ports: gnosis=18545, base=18546, ethereum=18547, polygon=18548,
#         optimism=18549, arbitrum=18550, celo=18551

# Start Anvil forks. Default: gnosis only. Pass chains to fork more.
# Examples:
#   just anvil-fork                    # gnosis only
#   just anvil-fork gnosis,base        # gnosis + base
#   just anvil-fork all                # all 7 chains
anvil-fork chains="gnosis":
    #!/usr/bin/env bash
    set -e
    SECRETS=secrets.env
    ANVIL=~/.foundry/bin/anvil

    declare -A RPC_KEYS=( [gnosis]=gnosis_rpc [base]=base_rpc [ethereum]=ethereum_rpc \
        [polygon]=polygon_rpc [optimism]=optimism_rpc [arbitrum]=arbitrum_rpc [celo]=celo_rpc )
    declare -A PORTS=( [gnosis]=18545 [base]=18546 [ethereum]=18547 \
        [polygon]=18548 [optimism]=18549 [arbitrum]=18550 [celo]=18551 )

    get_rpc() { grep -m1 "${1}=" "$SECRETS" 2>/dev/null | cut -d= -f2 | cut -d, -f1; }

    REQUESTED="{{chains}}"
    if [ "$REQUESTED" = "all" ]; then
        REQUESTED="gnosis,base,ethereum,polygon,optimism,arbitrum,celo"
    fi

    STARTED=0
    IFS=',' read -ra CHAIN_LIST <<< "$REQUESTED"
    for chain in "${CHAIN_LIST[@]}"; do
        chain=$(echo "$chain" | tr -d ' ')
        port=${PORTS[$chain]:-""}
        rpc_key=${RPC_KEYS[$chain]:-""}
        if [ -z "$port" ]; then
            echo "  Unknown chain: $chain (available: ${!PORTS[*]})"
            continue
        fi
        rpc=$(get_rpc "$rpc_key")
        if [ -z "$rpc" ]; then
            echo "  $chain: no $rpc_key in secrets.env, skipping"
            continue
        fi
        lsof -ti:$port | xargs -r kill 2>/dev/null || true
        $ANVIL --fork-url "$rpc" --port $port --auto-impersonate --silent &
        echo "  $chain: forking on port $port (PID $!)"
        STARTED=$((STARTED + 1))
    done

    if [ $STARTED -eq 0 ]; then
        echo "No forks started. Check secrets.env has RPC URLs."
        exit 1
    fi
    sleep 3
    echo "$STARTED fork(s) running."

# Stop all Anvil forks (ports 18545-18551)
anvil-stop:
    #!/usr/bin/env bash
    STOPPED=0
    for port in 18545 18546 18547 18548 18549 18550 18551; do
        PIDS=$(lsof -ti:$port 2>/dev/null)
        if [ -n "$PIDS" ]; then
            echo "$PIDS" | xargs kill 2>/dev/null
            STOPPED=$((STOPPED + 1))
        fi
    done
    echo "Stopped $STOPPED Anvil fork(s)."

# Fund an address on running Anvil forks (native + OLAS)
# Examples:
#   just anvil-fund 0xADDR               # fund on all running forks
#   just anvil-fund 0xADDR gnosis,base   # fund on specific chains
anvil-fund address chains="":
    #!/usr/bin/env bash
    set -e
    if [ -n "{{chains}}" ]; then
        uv run python scripts/anvil_fund.py {{address}} {{chains}}
    else
        uv run python scripts/anvil_fund.py {{address}}
    fi

# Run micromech server against Anvil forks
run-anvil:
    #!/usr/bin/env bash
    set -e
    ENV_VARS=""
    for chain_port in gnosis:18545 base:18546 ethereum:18547 polygon:18548 \
                      optimism:18549 arbitrum:18550 celo:18551; do
        chain=${chain_port%%:*}
        port=${chain_port##*:}
        if curl -sf http://localhost:$port -X POST -H 'Content-Type: application/json' \
            -d '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' >/dev/null 2>&1; then
            ENV_VARS="$ENV_VARS ${chain}_rpc=http://localhost:$port"
        fi
    done
    if [ -z "$ENV_VARS" ]; then
        echo "No Anvil forks running. Start with: just anvil-fork"
        exit 1
    fi
    echo "Detected forks:$ENV_VARS"
    env $ENV_VARS testing=true CHAINLIST_ENRICHMENT=false uv run micromech run

# Run web dashboard against Anvil forks (auto-detects running forks)
web-anvil port="8090":
    #!/usr/bin/env bash
    set -e
    ENV_VARS=""
    for chain_port in gnosis:18545 base:18546 ethereum:18547 polygon:18548 \
                      optimism:18549 arbitrum:18550 celo:18551; do
        chain=${chain_port%%:*}
        port=${chain_port##*:}
        if curl -sf http://localhost:$port -X POST -H 'Content-Type: application/json' \
            -d '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' >/dev/null 2>&1; then
            ENV_VARS="$ENV_VARS ${chain}_rpc=http://localhost:$port"
        fi
    done
    if [ -z "$ENV_VARS" ]; then
        echo "No Anvil forks running. Start with: just anvil-fork"
        exit 1
    fi
    echo "Detected forks:$ENV_VARS"
    TOKEN="dev-$(date +%s)"
    # Open browser after a short delay
    (sleep 2 && xdg-open "http://localhost:{{port}}?token=$TOKEN" 2>/dev/null || open "http://localhost:{{port}}?token=$TOKEN" 2>/dev/null) &
    env $ENV_VARS testing=true CHAINLIST_ENRICHMENT=false MICROMECH_AUTH_TOKEN="$TOKEN" uv run micromech web --port {{port}}

# Run CLI init wizard against Anvil fork
init-anvil chain="gnosis":
    #!/usr/bin/env bash
    set -e
    ENV_VARS=""
    for chain_port in gnosis:18545 base:18546 ethereum:18547 polygon:18548 \
                      optimism:18549 arbitrum:18550 celo:18551; do
        c=${chain_port%%:*}
        p=${chain_port##*:}
        if curl -sf http://localhost:$p -X POST -H 'Content-Type: application/json' \
            -d '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' >/dev/null 2>&1; then
            ENV_VARS="$ENV_VARS ${c}_rpc=http://localhost:$p"
        fi
    done
    env $ENV_VARS testing=true CHAINLIST_ENRICHMENT=false uv run micromech init --chain {{chain}}

# Build package
build:
    uv build

# Run the server
run:
    uv run micromech run

# Run the web dashboard
web port="8090":
    uv run micromech web --port {{port}}

# Send random on-chain requests to all deployed chains (demo)
demo:
    uv run python scripts/demo_requests.py

# --- Docker ---

# Docker build (native platform)
docker-build:
    docker build -t micromech:latest .

# Docker build multi-arch (amd64 + arm64)
docker-build-multi:
    docker buildx build --platform linux/amd64,linux/arm64 -t micromech:latest .

# Docker run for local testing
docker-run:
    docker run --rm -it \
        -v $(pwd)/data:/app/data \
        --env-file secrets.env \
        -p 8090:8090 \
        micromech:latest

# Start with docker compose
up:
    docker compose up -d

# Stop docker compose
down:
    docker compose down

# Update to the latest published image
update:
    docker compose pull
    docker compose up -d

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

    TAG_COMMIT=$(git rev-parse "$TAG^{}")
    HEAD_COMMIT=$(git rev-parse HEAD)
    if [ "$TAG_COMMIT" != "$HEAD_COMMIT" ]; then
        echo "Error: Tag $TAG is not at HEAD"
        exit 1
    fi

# Run full release quality gate
release-check:
    #!/usr/bin/env bash
    set -e

    VERSION=$(grep -m1 'version = ' pyproject.toml | cut -d '"' -f2)
    TAG="v$VERSION"

    echo "Running security checks..."
    just security
    echo "Running quality checks..."
    just check
    echo "Running tests..."
    just test-unit
    echo "Building package..."
    just build

    echo "Validating git state..."
    just _validate-git-state

    # Check tag doesn't already exist (tag is created by `just release`)
    if git rev-parse "$TAG" >/dev/null 2>&1; then
        echo "Error: Tag $TAG already exists!"
        echo "  If you need to recreate it: git tag -d $TAG && git push origin :refs/tags/$TAG"
        exit 1
    fi

    echo "All checks passed! Ready to release $TAG."

# Run checks, create and push release tag
release: release-check
    #!/usr/bin/env bash
    set -e

    VERSION=$(grep -m1 'version = ' pyproject.toml | cut -d '"' -f2)
    TAG="v$VERSION"

    echo "Creating and pushing tag $TAG..."
    git tag -a "$TAG" -m "Release $TAG"
    git push origin main
    git push origin "$TAG"
    echo "Release $TAG created and pushed!"
    echo "Next: wait for CI to go green, then run: just publish"

# Build and publish docker image (requires tag at HEAD)
publish: _validate-git-state _validate-tag-at-head
    #!/usr/bin/env bash
    set -e

    VERSION=$(grep -m1 'version = ' pyproject.toml | cut -d '"' -f2)

    echo "Publishing version $VERSION to DockerHub (multi-arch)..."
    docker buildx build --no-cache --platform linux/amd64,linux/arm64 \
        --build-arg VERSION=$VERSION \
        -t dvilela/micromech:latest -t dvilela/micromech:$VERSION \
        -f Dockerfile --push .
    echo "Published version $VERSION to DockerHub (amd64 + arm64)"
