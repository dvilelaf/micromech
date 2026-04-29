#!/bin/sh
# Micromech Updater Sidecar
# Watches for update requests and handles image pull + host artifact refresh + restart.

set -eu

UPDATER_LOG="/host/data/updater.log"

rotate_log() {
    if [ -f "$UPDATER_LOG" ] && [ "$(wc -c < "$UPDATER_LOG")" -gt 1048576 ]; then
        mv "$UPDATER_LOG" "${UPDATER_LOG}.1" 2>/dev/null || true
    fi
}

log() {
    msg="$(date -u '+%Y-%m-%d %H:%M:%S') - [updater] $1"
    echo "$msg"
    log_dir=$(dirname "$UPDATER_LOG")
    if [ -d "$log_dir" ] && [ ! -L "$log_dir" ] && [ ! -L "$UPDATER_LOG" ]; then
        rotate_log
        echo "$msg" >> "$UPDATER_LOG" 2>/dev/null || true
    fi
}

data_safe() {
    [ -d data ] && [ ! -L data ]
}

backup_safe() {
    data_safe \
        && [ ! -L data/backup ] \
        && [ ! -L data/backup/pre-update ] \
        && [ -d data/backup/pre-update ]
}

write_data_file() {
    data_safe || return 0
    rel="$1"
    value="$2"
    dest="data/$rel"
    [ -L "$dest" ] && return 0
    tmp=$(mktemp "data/.$rel.XXXXXX" 2>/dev/null) || return 0
    if printf '%s\n' "$value" > "$tmp" 2>/dev/null && mv "$tmp" "$dest" 2>/dev/null; then
        return 0
    fi
    rm -f "$tmp" 2>/dev/null || true
    return 0
}

_write_result() {
    write_data_file ".update-result" "$1"
}

restore_host_artifacts() {
    backup_dir="${1:-}"
    [ -n "$backup_dir" ] || return 0
    for artifact in docker-compose.yml Justfile updater.sh; do
        [ ! -L "$artifact" ] && [ -f "$backup_dir/$artifact" ] && cp -p "$backup_dir/$artifact" "$artifact"
    done
    [ ! -L updater.sh ] && [ -f updater.sh ] && chmod +x updater.sh
}

rollback_image() {
    if docker image inspect "${MICROMECH_IMAGE%:latest}:rollback-prev" >/dev/null 2>&1; then
        docker tag "${MICROMECH_IMAGE%:latest}:rollback-prev" "$MICROMECH_IMAGE" \
            && log "Image rolled back to previous (:rollback-prev → :latest)" \
            || log "WARNING: rollback image retag failed"
    else
        log "WARNING: :rollback-prev not found; restart will use current :latest (broken)"
    fi
    return 0
}

wait_healthy() {
    max=120
    i=0
    while [ "$i" -lt "$max" ]; do
        container_id=$(docker compose ps -q micromech 2>/dev/null || true)
        if [ -n "$container_id" ]; then
            status=$(docker inspect --format='{{.State.Status}}' "$container_id" 2>/dev/null || echo "gone")
            health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id" 2>/dev/null || echo "none")
        else
            status="gone"
            health="none"
        fi
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

if [ -L data ]; then
    echo "$(date -u '+%Y-%m-%d %H:%M:%S') - [updater] CRITICAL: data/ is a symlink — refusing to update."
    exit 1
fi

MICROMECH_IMAGE=$(docker compose config --images micromech 2>/dev/null | head -1 || true)
if [ -z "$MICROMECH_IMAGE" ]; then
    MICROMECH_IMAGE=$(docker compose config micromech 2>/dev/null | sed -n 's/^[[:space:]]*image:[[:space:]]*\([^[:space:]]*\)[[:space:]]*$/\1/p' | head -1)
fi
if [ -z "$MICROMECH_IMAGE" ]; then
    log "ERROR: Could not detect micromech image from docker-compose.yml — aborting"
    exit 1
fi
case "$MICROMECH_IMAGE" in
    dvilela/micromech:latest|dvilela/micromech-testing:latest) ;;
    *) log "ERROR: Unexpected image '$MICROMECH_IMAGE' — aborting"; exit 1 ;;
esac

PROJECT_DIR="${HOST_PROJECT_DIR:-}"
if [ -z "$PROJECT_DIR" ]; then
    PROJECT_DIR=$(docker inspect "$(hostname)" --format '{{range .Mounts}}{{if eq .Destination "/host"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)
fi
if [ -z "$PROJECT_DIR" ]; then
    PROJECT_DIR=$(docker inspect "$(hostname)" --format '{{range .Mounts}}{{if eq .Destination "/host/data"}}{{.Source}}{{end}}{{end}}' 2>/dev/null | sed 's|/data$||')
fi
if [ -z "$PROJECT_DIR" ]; then
    log "ERROR: Cannot detect project directory from /host or /host/data mount"
    exit 1
fi

RUN_AS="${UPDATER_RUN_AS:-}"
if [ -z "$RUN_AS" ]; then
    RUN_AS=$(sed -n 's/^[[:space:]]*user:[[:space:]]*//p' docker-compose.yml | head -1)
    RUN_AS=$(printf '%s' "$RUN_AS" | sed 's/[[:space:]]*$//; s/^"//; s/"$//')
fi
RUN_AS="${RUN_AS:-1000:1000}"
if ! printf '%s' "$RUN_AS" | grep -Eq '^[0-9]+:[0-9]+$'; then
    log "ERROR: Invalid RUN_AS '$RUN_AS' — aborting"
    exit 1
fi

log "Project directory: $PROJECT_DIR"
log "Micromech image: $MICROMECH_IMAGE"
log "Ready — watching for update requests"

while true; do
    if [ -L data ]; then
        echo "$(date -u '+%Y-%m-%d %H:%M:%S') - [updater] CRITICAL: data/ is a symlink — refusing to process update requests."
        sleep 60
        continue
    fi
    if [ -f data/.update-request ]; then
        ACTION=$(cat data/.update-request)
        rm -f data/.update-request
        ACTION=$(printf '%s' "$ACTION" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)
        case "$ACTION" in
            restart|update) ;;
            *) log "WARNING: Ignoring unknown action: $ACTION"; sleep 10; continue ;;
        esac
        log "Received request: $ACTION"

        if [ "$ACTION" = "restart" ]; then
            log "Restarting micromech..."
            docker compose restart micromech
            if ! wait_healthy; then
                _write_result "error:crashloop"
            fi
        else
            OLD=$(docker inspect --format '{{index .Config.Labels "version"}}' "$MICROMECH_IMAGE" 2>/dev/null || echo "unknown")
            OLD_DIGEST=$(docker inspect --format '{{.Id}}' "$MICROMECH_IMAGE" 2>/dev/null || echo "none")
            OLD=$(printf '%s' "$OLD" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)

            if [ "$OLD_DIGEST" != "none" ]; then
                docker tag "$MICROMECH_IMAGE" "${MICROMECH_IMAGE%:latest}:rollback-prev" \
                    || log "WARNING: rollback-prev retag failed; rollback may not restore image"
            fi

            if docker compose pull micromech 2>&1; then
                NEW=$(docker inspect --format '{{index .Config.Labels "version"}}' "$MICROMECH_IMAGE" 2>/dev/null || echo "unknown")
                NEW_DIGEST=$(docker inspect --format '{{.Id}}' "$MICROMECH_IMAGE" 2>/dev/null || echo "none")
                NEW=$(printf '%s' "$NEW" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)

                if [ "$OLD" != "$NEW" ] || [ "$OLD_DIGEST" != "$NEW_DIGEST" ]; then
                    updater_changed=0
                    compose_changed=0
                    host_backup_dir=""
                    if [ ! -L "Justfile" ] && [ ! -L "docker-compose.yml" ] && [ ! -L "updater.sh" ]; then
                        log "Regenerating host artifacts from new image..."
                        host_backup_dir=$(mktemp -d /tmp/micromech-host-artifacts-XXXXXX)
                        for artifact in docker-compose.yml Justfile updater.sh; do
                            [ -f "$artifact" ] && cp -p "$artifact" "$host_backup_dir/$artifact"
                        done
                        qs_tmp=$(mktemp /tmp/qs-XXXXXX)
                        if ! docker run --rm --entrypoint cat "$MICROMECH_IMAGE" /app/scripts/quickstart.sh > "$qs_tmp" \
                            || [ ! -s "$qs_tmp" ] \
                            || ! bash -n "$qs_tmp"; then
                            log "ERROR: Could not extract a valid quickstart.sh from new image"
                            restore_host_artifacts "$host_backup_dir"
                            rollback_image
                            _write_result "error:config_regen_failed"
                            rm -f "$qs_tmp"
                            rm -rf "$host_backup_dir"
                            sleep 60
                            continue
                        fi
                        if HOST_PROJECT_DIR="$PROJECT_DIR" UPDATE_CONFIG=1 bash "$qs_tmp"; then
                            if bash -n updater.sh; then
                                if docker compose -f docker-compose.yml config -q \
                                    && docker compose -f docker-compose.yml config --services | grep -qx micromech \
                                    && docker compose -f docker-compose.yml config --services | grep -qx dockerproxy \
                                    && docker compose -f docker-compose.yml config --services | grep -qx updater; then
                                    cmp -s "$host_backup_dir/updater.sh" updater.sh || updater_changed=1
                                    cmp -s "$host_backup_dir/docker-compose.yml" docker-compose.yml || compose_changed=1
                                    log "Host artifacts refreshed"
                                else
                                    log "ERROR: Regenerated compose is invalid — restoring previous host artifacts"
                                    restore_host_artifacts "$host_backup_dir"
                                    rollback_image
                                    _write_result "error:config_regen_failed"
                                    rm -f "$qs_tmp"
                                    rm -rf "$host_backup_dir"
                                    sleep 60
                                    continue
                                fi
                            else
                                log "ERROR: New updater.sh failed syntax check — restoring previous host artifacts"
                                restore_host_artifacts "$host_backup_dir"
                                rollback_image
                                _write_result "error:updater_invalid"
                                rm -f "$qs_tmp"
                                rm -rf "$host_backup_dir"
                                sleep 60
                                continue
                            fi
                        else
                            log "ERROR: Host artifact regeneration failed — restoring previous host artifacts"
                            restore_host_artifacts "$host_backup_dir"
                            rollback_image
                            _write_result "error:config_regen_failed"
                            rm -f "$qs_tmp"
                            rm -rf "$host_backup_dir"
                            sleep 60
                            continue
                        fi
                        rm -f "$qs_tmp"
                    else
                        log "Skipping config regeneration (symlinks detected - managed installation)"
                    fi

                    pre_ts=$(date -u '+%Y%m%dT%H%M%SZ')
                    pre_dir="data/backup/pre-update"
                    if ! data_safe || [ -L data/backup ] || [ -L "$pre_dir" ]; then
                        log "CRITICAL: data backup path is a symlink — aborting update."
                        restore_host_artifacts "$host_backup_dir"
                        rollback_image
                        [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                        _write_result "error:unsafe_backup_dir"
                        sleep 60
                        continue
                    fi
                    mkdir -p "$pre_dir" 2>/dev/null || true
                    chmod 700 "$pre_dir" 2>/dev/null || true
                    if [ -L data/config.yaml ] || [ -L data/wallet.json ]; then
                        log "CRITICAL: data file is a symlink — aborting update."
                        restore_host_artifacts "$host_backup_dir"
                        rollback_image
                        [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                        _write_result "error:unsafe_backup_dir"
                        sleep 60
                        continue
                    fi
                    snap_cfg_dest="$pre_dir/config.yaml.${pre_ts}.bak"
                    snap_wal_dest="$pre_dir/wallet.json.${pre_ts}.bak"
                    [ ! -e "$snap_cfg_dest" ] && [ ! -L "$snap_cfg_dest" ] \
                        && cp -p data/config.yaml "$snap_cfg_dest" 2>/dev/null \
                        && log "Pre-update config snapshot saved" || true
                    # SECURITY: never log size/hash/content of wallet.json or its snapshot.
                    [ ! -e "$snap_wal_dest" ] && [ ! -L "$snap_wal_dest" ] \
                        && cp -p data/wallet.json "$snap_wal_dest" 2>/dev/null \
                        && log "Pre-update wallet snapshot saved" || true

                    _write_result "updated:$OLD:$NEW"

                    ack=0
                    for i in $(seq 1 60); do
                        if ! data_safe; then
                            ack=-1
                            break
                        fi
                        [ ! -f data/.update-result ] && ack=1 && break
                        [ "$i" -eq 12 ] && log "INFO: Still waiting for Micromech ack (${i}x5s)..."
                        sleep 5
                    done
                    if [ "$ack" -eq -1 ]; then
                        echo "$(date -u '+%Y-%m-%d %H:%M:%S') - [updater] CRITICAL: data/ directory unsafe during ack wait — aborting update cycle."
                        restore_host_artifacts "$host_backup_dir"
                        rollback_image
                        docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                        [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                        sleep 60
                        continue
                    fi
                    [ "$ack" -eq 0 ] && log "WARNING: Micromech did not acknowledge update within 5 min — proceeding anyway"

                    log "Stopping micromech gracefully..."
                    if ! docker compose stop micromech; then
                        log "ERROR: Failed to stop micromech cleanly — restoring previous host artifacts"
                        restore_host_artifacts "$host_backup_dir"
                        rollback_image
                        docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                        [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                        _write_result "error:stop_failed"
                        sleep 60
                        continue
                    fi
                    sleep 2

                    if ! data_safe; then
                        echo "$(date -u '+%Y-%m-%d %H:%M:%S') - [updater] CRITICAL: data/ directory unsafe — cannot safely update. Aborting update cycle."
                        errdir=$(mktemp -d /tmp/micromech-updater-XXXXXX 2>/dev/null || true)
                        if [ -n "$errdir" ]; then
                            echo "error:data_missing" > "$errdir/error" 2>/dev/null || true
                            echo "$(date -u '+%Y-%m-%d %H:%M:%S') - [updater] Error marker written to $errdir/error"
                            rm -rf "$errdir" 2>/dev/null || true
                        fi
                        restore_host_artifacts "$host_backup_dir"
                        rollback_image
                        docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                        [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                        sleep 60
                        continue
                    fi
                    if printf '%s' "$PROJECT_DIR" | grep -q '[[:cntrl:]:]'; then
                        log "CRITICAL: PROJECT_DIR contains unsupported characters — aborting."
                        _write_result "error:unsafe_project_dir"
                        restore_host_artifacts "$host_backup_dir"
                        rollback_image
                        docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                        [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                        sleep 60
                        continue
                    fi
                    PROJECT_DIR_COMPOSE=$(printf '%s' "$PROJECT_DIR" | sed 's/\$/$$/g')
                    PROJECT_DIR_SED=$(printf '%s' "$PROJECT_DIR_COMPOSE" | sed 's/[&|\\]/\\&/g')

                    chown -R -- "$RUN_AS" data/ 2>/dev/null || true
                    if [ -L docker-compose.yml.bak ]; then
                        log "CRITICAL: docker-compose backup path is unsafe — aborting update."
                        restore_host_artifacts "$host_backup_dir"
                        rollback_image
                        docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                        [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                        _write_result "error:compose_backup_failed"
                        sleep 60
                        continue
                    fi
                    cp docker-compose.yml docker-compose.yml.bak \
                        || { restore_host_artifacts "$host_backup_dir"; rollback_image; docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"; [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"; _write_result "error:compose_backup_failed"; sleep 60; continue; }
                    abs_compose=$(mktemp /tmp/micromech-compose-abs-XXXXXX) \
                        || { restore_host_artifacts "$host_backup_dir"; rollback_image; docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"; [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"; _write_result "error:compose_backup_failed"; sleep 60; continue; }
                    sed "s|- \./data|- $PROJECT_DIR_SED/data|g; s|- \./secrets\.env|- $PROJECT_DIR_SED/secrets.env|g" docker-compose.yml > "$abs_compose" \
                        || { rm -f "$abs_compose"; restore_host_artifacts "$host_backup_dir"; rollback_image; docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"; [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"; _write_result "error:compose_backup_failed"; sleep 60; continue; }

                    log "Starting micromech with new image..."
                    if ! docker compose -f "$abs_compose" --project-directory . up -d micromech; then
                        log "ERROR: Failed to start micromech with refreshed compose — restoring previous host artifacts"
                        restore_host_artifacts "$host_backup_dir"
                        rollback_image
                        docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                        rm -f "$abs_compose"
                        [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                        _write_result "error:start_failed"
                        sleep 60
                        continue
                    fi
                    rm -f "$abs_compose"

                    if ! wait_healthy; then
                        if ! data_safe; then
                            echo "$(date -u '+%Y-%m-%d %H:%M:%S') - [updater] CRITICAL: data/ directory unsafe during health rollback — skipping data restore."
                            docker compose stop micromech 2>/dev/null || true
                            rollback_image
                            [ ! -L docker-compose.yml ] && [ -f docker-compose.yml.bak ] && cp docker-compose.yml.bak docker-compose.yml
                            restore_host_artifacts "$host_backup_dir"
                            [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                            docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                            sleep 60
                            continue
                        fi
                        log "Health check failed — rolling back data and image..."
                        docker compose stop micromech 2>/dev/null || true
                        if ! backup_safe; then
                            log "CRITICAL: pre-update backup path unsafe during health rollback — skipping data restore."
                            rollback_image
                            [ ! -L docker-compose.yml ] && [ -f docker-compose.yml.bak ] && cp docker-compose.yml.bak docker-compose.yml
                            restore_host_artifacts "$host_backup_dir"
                            [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                            docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                            _write_result "error:unsafe_backup_dir"
                            sleep 60
                            continue
                        fi
                        snap_cfg=$(ls -1 data/backup/pre-update/config.yaml.[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.bak 2>/dev/null | sort -r | head -1 || true)
                        snap_wal=$(ls -1 data/backup/pre-update/wallet.json.[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.bak 2>/dev/null | sort -r | head -1 || true)
                        if { [ -n "$snap_cfg" ] && [ -L "$snap_cfg" ]; } || { [ -n "$snap_wal" ] && [ -L "$snap_wal" ]; }; then
                            log "CRITICAL: pre-update snapshot is a symlink — skipping data restore."
                            rollback_image
                            [ ! -L docker-compose.yml ] && [ -f docker-compose.yml.bak ] && cp docker-compose.yml.bak docker-compose.yml
                            restore_host_artifacts "$host_backup_dir"
                            [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                            docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                            _write_result "error:unsafe_backup_dir"
                            sleep 60
                            continue
                        fi
                        if [ -n "$snap_cfg" ] && [ -f "$snap_cfg" ]; then
                            cfg_tmp=$(mktemp data/config.yaml.XXXXXX) \
                                && cp -p "$snap_cfg" "$cfg_tmp" \
                                && mv "$cfg_tmp" data/config.yaml \
                                && log "Restored config.yaml from snapshot"
                            rm -f "${cfg_tmp:-}" 2>/dev/null || true
                        fi
                        if [ -n "$snap_wal" ] && [ -f "$snap_wal" ]; then
                            wal_tmp=$(mktemp data/wallet.json.XXXXXX) \
                                && cp -p "$snap_wal" "$wal_tmp" \
                                && mv "$wal_tmp" data/wallet.json \
                                && log "Restored wallet.json from snapshot"
                            rm -f "${wal_tmp:-}" 2>/dev/null || true
                        fi
                        for restored_file in data/config.yaml data/wallet.json; do
                            [ ! -L "$restored_file" ] && [ -f "$restored_file" ] \
                                && chown -- "$RUN_AS" "$restored_file" 2>/dev/null || true
                        done
                        rollback_image
                        [ ! -L docker-compose.yml ] && [ -f docker-compose.yml.bak ] && cp docker-compose.yml.bak docker-compose.yml
                        restore_host_artifacts "$host_backup_dir"
                        [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                        log "Restarting micromech with rolled-back image..."
                        docker compose up -d micromech || log "WARNING: rollback restart failed — manual intervention required"
                        [ -n "$OLD" ] || OLD="unknown"
                        data_safe && _write_result "error:rolled_back_to_v$OLD"
                    else
                        rm -f docker-compose.yml.bak
                        [ -n "$host_backup_dir" ] && rm -rf "$host_backup_dir"
                        if [ "$compose_changed" -eq 1 ]; then
                            log "Recreating updater sidecar from refreshed compose"
                            updater_compose=$(mktemp /tmp/micromech-compose-updater-XXXXXX) || updater_compose=""
                            if [ -z "$updater_compose" ]; then
                                log "WARNING: could not create updater sidecar compose; reloading script in current container"
                            else
                                if ! sed "s|- \./:/host|- $PROJECT_DIR_SED:/host|g" docker-compose.yml > "$updater_compose"; then
                                    log "WARNING: could not rewrite updater sidecar compose; reloading script in current container"
                                    rm -f "$updater_compose"
                                    updater_compose=""
                                fi
                            fi
                            if [ -z "$updater_compose" ] \
                                || grep -Fq -- "- ./:/host" "$updater_compose" \
                                || ! grep -Fq -- "- $PROJECT_DIR_COMPOSE:/host" "$updater_compose"; then
                                log "WARNING: updater sidecar compose still has unsafe /host bind; reloading script in current container"
                            elif docker compose -f "$updater_compose" --project-directory . up -d --force-recreate dockerproxy updater 2>&1; then
                                rm -f "$updater_compose"
                                exit 0
                            fi
                            [ -n "$updater_compose" ] && rm -f "$updater_compose"
                            log "WARNING: updater sidecar recreation failed; reloading script in current container"
                        fi
                        if [ "$updater_changed" -eq 1 ]; then
                            log "Reloading updated updater.sh"
                            exec bash ./updater.sh
                        fi
                    fi
                else
                    _write_result "current:$OLD"
                fi
            else
                _write_result "error:pull_failed"
            fi

            waste_gb=$(if command -v timeout >/dev/null 2>&1; then timeout 5 docker system df 2>/dev/null || true; fi | tail -n +2 | awk '{v=$NF; if (v ~ /^\(/) v=$(NF-1); n=v; gsub(/[^0-9.]/, "", n); u=toupper(v); gsub(/[0-9.]/, "", u); if (u ~ /^TB/) t+=n*1024; else if (u ~ /^GB/) t+=n; else if (u ~ /^MB/) t+=n/1024; else if (u ~ /^KB/) t+=n/1048576; else if (u ~ /^B/) t+=n/1073741824} END {printf "%.0f", t}')
            if data_safe && [ "${waste_gb:-0}" -ge 5 ]; then
                write_data_file ".disk-warning" "$waste_gb"
                log "WARNING: Docker has ~${waste_gb}GB of reclaimable space"
            elif data_safe; then
                rm -f data/.disk-warning
            fi
        fi
    fi
    sleep 10
done
