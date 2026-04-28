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

# H1 fix: atomic write of .update-result so bot never reads truncated marker.
# write to .tmp + mv (POSIX rename is atomic for same-filesystem).
_write_result() {
    printf '%s\n' "$1" > data/.update-result.tmp 2>/dev/null \
        && mv data/.update-result.tmp data/.update-result 2>/dev/null || true
}

# Wait for micromech container to become healthy after restart.
# Returns 0 if healthy within max seconds, 1 if timeout.
wait_healthy() {
    max=120
    i=0
    while [ "$i" -lt "$max" ]; do
        status=$(docker inspect --format='{{.State.Status}}' micromech 2>/dev/null || echo "gone")
        health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' micromech 2>/dev/null || echo "none")
        if [ "$status" = "running" ] && { [ "$health" = "healthy" ] || [ "$health" = "none" ]; }; then
            log "Micromech is up (status=$status health=$health)"
            return 0
        fi
        sleep 1
        i=$((i+1))
    done
    log "ERROR: Micromech failed to reach healthy within ${max}s (status=${status:-unknown} health=${health:-unknown})"
    return 1
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
# S2: Reject anything that does not match the expected form exactly (supply-chain guard)
case "$MICROMECH_IMAGE" in
    dvilela/micromech:latest|dvilela/micromech-testing:latest) ;;
    *) log "ERROR: Unexpected image '$MICROMECH_IMAGE' — aborting"; exit 1 ;;
esac

log "Project directory: $PROJECT_DIR"
log "Micromech image: $MICROMECH_IMAGE"
log "Ready — watching for update requests"

while true; do
    if [ -f data/.update-request ]; then
        ACTION=$(cat data/.update-request)
        rm -f data/.update-request
        # Sanitize before logging to block log-injection via crafted .update-request
        ACTION=$(printf '%s' "$ACTION" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)
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
            # H3 fix: sanitize OLD immediately after capture — image labels can carry
            # `\n`+`error:wallet_compromised` for Telegram phishing (verified empirically).
            OLD=$(printf '%s' "$OLD" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)

            # Tag current :latest as :rollback-prev so we can restore it on health failure.
            if [ "$OLD_DIGEST" != "none" ]; then
                docker tag "$MICROMECH_IMAGE" "${MICROMECH_IMAGE%:latest}:rollback-prev" \
                    || log "WARNING: rollback-prev retag failed; rollback may not restore image"
            fi

            if docker compose pull micromech 2>&1; then
                NEW=$(docker inspect --format '{{index .Config.Labels "version"}}' "$MICROMECH_IMAGE" 2>/dev/null || echo "unknown")
                NEW_DIGEST=$(docker inspect --format '{{.Id}}' "$MICROMECH_IMAGE" 2>/dev/null || echo "none")
                NEW=$(printf '%s' "$NEW" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)

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

                    # Pre-update snapshot of config + wallet (cp -p preserves mode/owner).
                    # SECURITY: never log size/hash/content of wallet.json — side channel.
                    _pre_ts=$(date -u '+%Y%m%dT%H%M%SZ')
                    _pre_dir="data/backup/pre-update"
                    mkdir -p "$_pre_dir" 2>/dev/null || true
                    # H2 fix: tighten perms even if dir was created with looser umask
                    chmod 700 "$_pre_dir" 2>/dev/null || true
                    cp -p data/config.yaml "$_pre_dir/config.yaml.${_pre_ts}.bak" 2>/dev/null \
                        && log "Pre-update config snapshot saved" || true
                    cp -p data/wallet.json "$_pre_dir/wallet.json.${_pre_ts}.bak" 2>/dev/null \
                        && log "Pre-update wallet snapshot saved" || true

                    _write_result "updated:$OLD:$NEW"

                    # Wait for Micromech to acknowledge (delete result file); 300s matches triton
                    # to avoid interrupting wallet.json writes on slow cold-starts.
                    for i in $(seq 1 60); do [ ! -f data/.update-result ] && break; sleep 5; done

                    # Graceful shutdown before restart to prevent file lock issues
                    log "Stopping micromech gracefully..."
                    docker compose stop micromech

                    # Wait for complete shutdown
                    sleep 2

                    # Ensure data directory has correct permissions for container user (1000:1000)
                    chown -R 1000:1000 data/ 2>/dev/null || true

                    # Backup compose for rollback before modifying anything.
                    cp docker-compose.yml docker-compose.yml.bak \
                        || { _write_result "error:compose_backup_failed"; sleep 60; continue; }
                    # Generate temporary docker-compose.yml with absolute path for data volume
                    sed "s|- \\./data|- $PROJECT_DIR/data|g" docker-compose.yml > /tmp/docker-compose-abs.yml \
                        || { _write_result "error:compose_backup_failed"; sleep 60; continue; }

                    # Start Micromech with new image using absolute paths
                    log "Starting micromech with new image..."
                    docker compose -f /tmp/docker-compose-abs.yml --project-directory . up -d micromech

                    # Cleanup
                    rm -f /tmp/docker-compose-abs.yml

                    # Health check + rollback on failure.
                    # NOTE: bot poll budget is 120s; full rollback may take 5-10 min.
                    # Bot will likely show "Timeout" before this marker is read.
                    if ! wait_healthy; then
                        log "Health check failed — rolling back data and image..."
                        docker compose stop micromech 2>/dev/null || true
                        # C1 fix: sort by filename (ISO timestamp embedded), NOT mtime.
                        # `cp -p` preserves source mtime so mtime order may diverge from chronological.
                        snap_cfg=$(ls -1 data/backup/pre-update/config.yaml.[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.bak 2>/dev/null | sort -r | head -1 || true)
                        if [ -n "$snap_cfg" ] && [ -f "$snap_cfg" ]; then
                            cp -p "$snap_cfg" data/config.yaml.tmp && mv data/config.yaml.tmp data/config.yaml \
                                && log "Restored config.yaml from snapshot"
                        fi
                        snap_wal=$(ls -1 data/backup/pre-update/wallet.json.[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.bak 2>/dev/null | sort -r | head -1 || true)
                        if [ -n "$snap_wal" ] && [ -f "$snap_wal" ]; then
                            cp -p "$snap_wal" data/wallet.json.tmp && mv data/wallet.json.tmp data/wallet.json \
                                && log "Restored wallet.json from snapshot"
                        fi
                        # C2 fix: micromech updater runs as root → cp leaves files root-owned;
                        # bot is uid 1000 → without chown, bot crashes with EACCES.
                        chown 1000:1000 data/config.yaml data/wallet.json 2>/dev/null || true
                        if docker image inspect "${MICROMECH_IMAGE%:latest}:rollback-prev" >/dev/null 2>&1; then
                            docker tag "${MICROMECH_IMAGE%:latest}:rollback-prev" "$MICROMECH_IMAGE" \
                                && log "Image rolled back to previous (:rollback-prev → :latest)"
                        else
                            log "WARNING: :rollback-prev not found; restart will use current :latest (broken)"
                        fi
                        if [ -f docker-compose.yml.bak ]; then
                            cp docker-compose.yml.bak docker-compose.yml
                        fi
                        log "Restarting micromech with rolled-back image..."
                        docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                        # Reuse existing `error:` prefix; bot displays as "Update failed: rolled_back_to_v0.5.1".
                        [ -n "$OLD" ] || OLD="unknown"
                        _write_result "error:rolled_back_to_v$OLD"
                    else
                        # Success: clean up the backup file so it doesn't go stale.
                        rm -f docker-compose.yml.bak
                    fi
                else
                    _write_result "current:$OLD"
                fi
            else
                _write_result "error:pull_failed"
            fi

            # Warn if Docker has significant reclaimable space
            waste_gb=$(if command -v timeout >/dev/null 2>&1; then timeout 5 docker system df 2>/dev/null || true; fi | tail -n +2 | awk '/GB \(/ {gsub(/[^0-9.]/, "", $4); t+=$4} END {printf "%.0f", t}')
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
