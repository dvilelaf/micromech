#!/bin/bash

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

MICROMECH_IMAGE="dvilela/micromech:latest"

# --- docker-compose.yml generator (also used by 'just update-config') ---
generate_compose() {
    local user_uid user_gid

    # Read UID:GID from existing compose (reliable even inside containers)
    if [ -f docker-compose.yml ]; then
        local existing_user
        existing_user=$(sed -n 's/.*user: *"\([^"]*\)".*/\1/p' docker-compose.yml | head -1)
        if [ -n "$existing_user" ]; then
            user_uid=$(echo "$existing_user" | cut -d: -f1)
            user_gid=$(echo "$existing_user" | cut -d: -f2)
        fi
    fi
    user_uid=${user_uid:-$(id -u)}
    user_gid=${user_gid:-$(id -g)}

    # Preserve existing host volume paths before overwriting
    local _vol_preserve=""
    if [ -f docker-compose.yml ]; then
        _vol_preserve=$(grep -E '^\s*-\s+.+:/' docker-compose.yml || true)
    fi

    docker run --rm --entrypoint cat "$MICROMECH_IMAGE" /app/docker-compose.yml > docker-compose.yml.tmp

    # Portable sed -i (works on both GNU and BSD/macOS)
    _sed_i() { if [ "$(uname)" = "Darwin" ]; then sed -i '' "$@"; else sed -i "$@"; fi; }

    # Transform for end-user deployment:
    _sed_i "s|    build:|    image: $MICROMECH_IMAGE|" docker-compose.yml.tmp
    _sed_i '/context: \./d' docker-compose.yml.tmp
    _sed_i '/dockerfile: Dockerfile/d' docker-compose.yml.tmp
    _sed_i '/# Mount source/d' docker-compose.yml.tmp
    _sed_i '/# .*\.\/.*:\/app/d' docker-compose.yml.tmp
    _sed_i "s|    user: .*|    user: \"$user_uid:$user_gid\"|" docker-compose.yml.tmp

    # Restore preserved host volume paths (if any)
    if [ -n "$_vol_preserve" ]; then
        echo "$_vol_preserve" | while IFS= read -r line; do
            host_path=$(echo "$line" | sed 's/.*- *//;s/:.*//')
            container_path=$(echo "$line" | sed 's/.*://;s/ *$//')
            if [ -n "$host_path" ] && [ -n "$container_path" ]; then
                _sed_i "s|^\([[:space:]]*-[[:space:]]*\).*:${container_path}|\1${host_path}:${container_path}|" docker-compose.yml.tmp
            fi
        done
    fi

    # Add updater sidecar service (if not already present)
    if ! grep -q 'updater:' docker-compose.yml.tmp; then
    cat >> docker-compose.yml.tmp << 'UPDATER_COMPOSE'
  updater:
    image: docker:cli
    environment:
      - TZ=${TZ:-Europe/Madrid}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./:/host
    working_dir: /host
    restart: unless-stopped
    command: ["sh", "-c", "while [ ! -f ./updater.sh ]; do sleep 5; done; exec sh ./updater.sh"]
UPDATER_COMPOSE
    fi

    # Set fixed project name if not already present
    if ! grep -q '^name:' docker-compose.yml.tmp; then
        { printf 'name: micromech\n'; cat docker-compose.yml.tmp; } > docker-compose.yml.tmp2
        mv docker-compose.yml.tmp2 docker-compose.yml.tmp
    fi

    mv docker-compose.yml.tmp docker-compose.yml
}

# --- Justfile generator (also used by 'just update-config') ---
generate_justfile() {
    local user_uid user_gid

    # Read UID:GID from existing compose (reliable even inside containers)
    if [ -f docker-compose.yml ]; then
        local existing_user
        existing_user=$(sed -n 's/.*user: *"\([^"]*\)".*/\1/p' docker-compose.yml | head -1)
        if [ -n "$existing_user" ]; then
            user_uid=$(echo "$existing_user" | cut -d: -f1)
            user_gid=$(echo "$existing_user" | cut -d: -f2)
        fi
    fi
    user_uid=${user_uid:-$(id -u)}
    user_gid=${user_gid:-$(id -g)}

    cat <<JUSTEOF > Justfile
set shell := ["bash", "-uc"]

up:
    docker compose up -d --remove-orphans
    @echo "🚀 Micromech is running!"
    @echo "🌐 Dashboard: http://localhost:8000"
    @echo "📜 View logs with: just logs"

down:
    docker compose down --remove-orphans

logs:
    docker compose logs -f

update:
    #!/usr/bin/env bash
    set -e
    MICROMECH_IMAGE=\$(sed -n 's/.*image: *\(dvilela\/micromech[^:]*:latest\).*/\1/p' docker-compose.yml | head -1)
    MICROMECH_IMAGE=\${MICROMECH_IMAGE:-$MICROMECH_IMAGE}
    current=\$(docker inspect --format '{{{{index .Config.Labels "version"}}' "\$MICROMECH_IMAGE" 2>/dev/null || echo "unknown")
    current_digest=\$(docker inspect --format '{{{{.Id}}' "\$MICROMECH_IMAGE" 2>/dev/null || echo "none")
    docker compose pull
    new=\$(docker inspect --format '{{{{index .Config.Labels "version"}}' "\$MICROMECH_IMAGE" 2>/dev/null || echo "unknown")
    new_digest=\$(docker inspect --format '{{{{.Id}}' "\$MICROMECH_IMAGE" 2>/dev/null || echo "none")
    if [ "\$current" != "\$new" ] || [ "\$current_digest" != "\$new_digest" ]; then
        echo "🔄 Updated v\$current -> v\$new"
        echo "📝 Updating config files..."
        docker run --rm --entrypoint cat "\$MICROMECH_IMAGE" /app/scripts/quickstart.sh > /tmp/micromech-qs.sh
        UPDATE_CONFIG=1 bash /tmp/micromech-qs.sh
        rm -f /tmp/micromech-qs.sh
        docker compose up -d
    else
        echo "✅ Already at latest version (v\$current)"
    fi
    # Warn if Docker has significant reclaimable space
    waste_gb=\$(docker system df 2>/dev/null | tail -n +2 | awk '/GB \(/ {gsub(/[^0-9.]/, "", \$4); t+=\$4} END {printf "%.0f", t}')
    if [ "\${waste_gb:-0}" -ge 5 ]; then
        echo "⚠️  Docker has ~\${waste_gb}GB of reclaimable space. Run 'docker system prune' to free disk."
    fi

update-config:
    #!/usr/bin/env bash
    set -e
    echo "📝 Updating Justfile and docker-compose.yml from latest Docker image..."
    MICROMECH_IMAGE=\$(sed -n 's/.*image: *\(dvilela\/micromech[^:]*:latest\).*/\1/p' docker-compose.yml | head -1)
    MICROMECH_IMAGE=\${MICROMECH_IMAGE:-$MICROMECH_IMAGE}
    docker run --rm --entrypoint cat "\$MICROMECH_IMAGE" /app/scripts/quickstart.sh > /tmp/micromech-qs.sh
    UPDATE_CONFIG=1 bash /tmp/micromech-qs.sh
    rm -f /tmp/micromech-qs.sh
    echo "✅ Config files updated!"

status:
    docker compose ps

init:
    @echo "🔧 Running Micromech setup wizard..."
    @docker run --rm -it \\
        --entrypoint "" \\
        --user "$user_uid:$user_gid" \\
        --volume "\$(pwd)/data:/app/data" \\
        --env-file "\$(pwd)/secrets.env" \\
        $MICROMECH_IMAGE \\
        python -m micromech init

doctor:
    @echo "🔍 Running health check..."
    @docker run --rm -it \\
        --entrypoint "" \\
        --user "$user_uid:$user_gid" \\
        --volume "\$(pwd)/data:/app/data" \\
        --env-file "\$(pwd)/secrets.env" \\
        $MICROMECH_IMAGE \\
        python -m micromech doctor
JUSTEOF
}

# --- Updater sidecar script generator ---
generate_updater_script() {
    cat << 'UPDATEREOF' > updater.sh
#!/bin/sh
# Micromech Updater Sidecar
# Watches for update requests and handles image pull + config regen + restart

# Install bash (required by quickstart.sh config generator)
if ! command -v bash >/dev/null 2>&1; then
    apk add --no-cache bash >/dev/null 2>&1
fi

UPDATER_LOG="data/updater.log"

# Simple rotation: move to .1 if >1MB
rotate_log() {
    if [ -f "$UPDATER_LOG" ] && [ "$(wc -c < "$UPDATER_LOG")" -gt 1048576 ]; then
        mv "$UPDATER_LOG" "${UPDATER_LOG}.1"
    fi
}

log() {
    msg="$(date -u '+%Y-%m-%d %H:%M:%S') - [updater] $1"
    echo "$msg"
    rotate_log
    echo "$msg" >> "$UPDATER_LOG"
}

cd /host

# Detect absolute project directory on host (from our own mount)
PROJECT_DIR=$(docker inspect $(hostname) --format '{{range .Mounts}}{{if eq .Destination "/host"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)
if [ -z "$PROJECT_DIR" ]; then
    log "ERROR: Cannot detect project directory"
    exit 1
fi

# Detect Micromech image from docker-compose.yml
MICROMECH_IMAGE=$(sed -n 's/.*image: *\(dvilela\/micromech[^:]*:latest\).*/\1/p' docker-compose.yml | head -1)
MICROMECH_IMAGE=${MICROMECH_IMAGE:-dvilela/micromech:latest}

log "Project directory: $PROJECT_DIR"
log "Micromech image: $MICROMECH_IMAGE"
log "Ready — watching for update requests"

while true; do
    if [ -f data/.update-request ]; then
        ACTION=$(cat data/.update-request)
        rm -f data/.update-request
        log "Received request: $ACTION"

        if [ "$ACTION" = "restart" ]; then
            log "Restarting micromech..."
            docker compose restart micromech
        else
            # Full update — pull, config regen, restart
            OLD=$(docker inspect --format '{{index .Config.Labels "version"}}' "$MICROMECH_IMAGE" 2>/dev/null || echo "unknown")
            OLD_DIGEST=$(docker inspect --format '{{.Id}}' "$MICROMECH_IMAGE" 2>/dev/null || echo "none")

            if docker compose pull micromech 2>&1; then
                NEW=$(docker inspect --format '{{index .Config.Labels "version"}}' "$MICROMECH_IMAGE" 2>/dev/null || echo "unknown")
                NEW_DIGEST=$(docker inspect --format '{{.Id}}' "$MICROMECH_IMAGE" 2>/dev/null || echo "none")

                if [ "$OLD" != "$NEW" ] || [ "$OLD_DIGEST" != "$NEW_DIGEST" ]; then
                    # Only regenerate configs if not using symlinks (managed installation)
                    if [ ! -L "Justfile" ] && [ ! -L "docker-compose.yml" ]; then
                        log "Regenerating config files from new image..."
                        docker run --rm --entrypoint cat "$MICROMECH_IMAGE" /app/scripts/quickstart.sh > /tmp/qs.sh
                        UPDATE_CONFIG=1 bash /tmp/qs.sh
                        rm -f /tmp/qs.sh
                    else
                        log "Skipping config regeneration (symlinks detected - managed installation)"
                    fi

                    echo "updated:$OLD:$NEW" > data/.update-result

                    # Wait for Micromech to acknowledge (delete result file)
                    for i in $(seq 1 12); do [ ! -f data/.update-result ] && break; sleep 5; done

                    # Graceful shutdown before restart to prevent file lock issues
                    log "Stopping micromech gracefully..."
                    docker compose stop micromech

                    # Wait for complete shutdown
                    sleep 2

                    # Ensure data directory has correct permissions for container user (1000:1000)
                    chown -R 1000:1000 data/ 2>/dev/null || true

                    # Generate temporary docker-compose.yml with absolute path for data volume
                    sed "s|- \\./data|- $PROJECT_DIR/data|g" docker-compose.yml > /tmp/docker-compose-abs.yml

                    # Start Micromech with new image using absolute paths
                    log "Starting micromech with new image..."
                    docker compose -f /tmp/docker-compose-abs.yml --project-directory . up -d micromech

                    # Cleanup
                    rm -f /tmp/docker-compose-abs.yml

                    # Re-exec to pick up any script changes (only if updater.sh is not a symlink)
                    [ ! -L "./updater.sh" ] && exec sh ./updater.sh
                else
                    echo "current:$OLD" > data/.update-result
                fi
            else
                echo "error:pull_failed" > data/.update-result
            fi

            # Warn if Docker has significant reclaimable space
            waste_gb=$(docker system df 2>/dev/null | tail -n +2 | awk '/GB \(/ {gsub(/[^0-9.]/, "", $4); t+=$4} END {printf "%.0f", t}')
            if [ "${waste_gb:-0}" -ge 5 ]; then
                echo "$waste_gb" > data/.disk-warning
                log "WARNING: Docker has ~${waste_gb}GB of reclaimable space"
            else
                rm -f data/.disk-warning
            fi
        fi
    fi
    sleep 10
done
UPDATEREOF
    chmod +x updater.sh
}

# Self-update mode: regenerate config files, then exit
if [ "${UPDATE_CONFIG:-}" = "1" ]; then
    # Managed installation: Justfile is a symlink → all configs are repo-managed
    if [ -L "Justfile" ]; then
        echo "📝 Managed installation detected — skipping config regeneration"
        exit 0
    fi
    generate_compose
    generate_justfile
    generate_updater_script
    exit 0
fi

echo -e "${BLUE}🔧 Micromech QuickStart Setup${NC}"
echo "This script will set up a production-ready Micromech environment."
echo

# 1. Check prerequisites
echo -e "${BLUE}🔍 Checking prerequisites...${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

if ! docker compose version &> /dev/null; then
    echo -e "${RED}Docker Compose plugin is not installed.${NC}"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo -e "${RED}Docker is installed but not running. Please start Docker Desktop.${NC}"
    exit 1
fi

echo -e "${GREEN}Docker is ready.${NC}"
echo

# 2. Setup Directory
DIR_NAME="micromech"
echo -e "${BLUE}📂 Setting up directory '$DIR_NAME'...${NC}"

if [ -d "$DIR_NAME" ]; then
    echo -e "Directory '$DIR_NAME' already exists. Updating config files..."
    echo -e "(Your data/ and secrets.env are preserved.)"
    cd "$DIR_NAME"
else
    mkdir -p "$DIR_NAME/data"
    cd "$DIR_NAME"
fi

# Extract artifacts from Docker image
IMAGE="$MICROMECH_IMAGE"
echo -e "${BLUE}📦 Pulling latest image and extracting configuration...${NC}"
docker pull "$IMAGE"

# Create a temporary container
CONTAINER_ID=$(docker create "$IMAGE")

# Function to cleanup on exit
cleanup() {
    docker rm "$CONTAINER_ID" > /dev/null 2>&1
}
trap cleanup EXIT

# 1. secrets.env
if [ ! -f secrets.env ]; then
    echo -e "   - secrets.env..."
    docker cp "$CONTAINER_ID:/app/secrets.env.example" ./secrets.env
else
    echo -e "   - secrets.env (skipped, exists)"
fi

# 2. docker-compose.yml (Transformed from repo version)
echo -e "   - docker-compose.yml..."
generate_compose

# 3. Justfile (Generated minimal version for end-users)
echo -e "   - Justfile..."
generate_justfile

# 4. updater.sh (Sidecar script for remote updates)
echo -e "   - updater.sh..."
generate_updater_script

# Explicit cleanup for non-error exit
cleanup
trap - EXIT

echo
echo -e "${GREEN}🎉 Setup Complete!${NC}"
echo
echo -e "Next steps:"
echo -e "1. Edit secrets (optional):  ${BLUE}nano $DIR_NAME/secrets.env${NC}"
echo -e "2. Start micromech:          ${BLUE}cd $DIR_NAME && docker compose up -d${NC}"
echo -e "3. Open the dashboard:       ${BLUE}http://localhost:8000${NC}"
echo
