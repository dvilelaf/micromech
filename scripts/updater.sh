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

        # Validate action — only allow exact known values
        if [ "$ACTION" != "restart" ] && [ "$ACTION" != "update" ]; then
            log "WARNING: Ignoring unknown action: $ACTION"
            continue
        fi

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
