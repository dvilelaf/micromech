#!/usr/bin/env bash
# Micromech updater sidecar.
# Normal update scope: pull image, recreate Micromech, verify health, rollback if needed.

set -eu

SERVICE_NAME="micromech"
IMAGE_ALLOWLIST="dvilela/micromech:latest dvilela/micromech-testing:latest"
VERSION_LABEL="org.dvilela.micromech.version"
HOST_ROOT="${UPDATER_HOST_ROOT:-/host}"
UPDATER_LOG="${UPDATER_LOG:-$HOST_ROOT/data/updater.log}"
POLL_SECONDS="${UPDATER_POLL_SECONDS:-10}"
HEALTH_TIMEOUT_SECONDS="${UPDATER_HEALTH_TIMEOUT_SECONDS:-120}"

rotate_log() {
    if [ -f "$UPDATER_LOG" ] && [ "$(wc -c < "$UPDATER_LOG")" -gt 1048576 ]; then
        mv "$UPDATER_LOG" "${UPDATER_LOG}.1" 2>/dev/null || true
    fi
}

log() {
    local msg log_dir
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

write_data_file() {
    data_safe || return 0
    local rel value dest tmp
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

write_result() {
    write_data_file ".update-result" "$1"
}

normalize_version() {
    local value
    value=$(printf '%s' "$1" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)
    case "$value" in
        ""|novalue|unknown) echo "unknown" ;;
        *) echo "$value" ;;
    esac
}

image_version() {
    local value
    value=$(docker inspect --format "{{index .Config.Labels \"$VERSION_LABEL\"}}" "$1" 2>/dev/null || true)
    normalize_version "$value"
}

image_id() {
    docker inspect --format '{{.Id}}' "$1" 2>/dev/null || echo "none"
}

running_service_image_id() {
    local cid
    cid=$(docker compose ps -q "$SERVICE_NAME" 2>/dev/null | head -1 || true)
    if [ -n "$cid" ]; then
        docker inspect --format '{{.Image}}' "$cid" 2>/dev/null || true
    fi
}

detect_image() {
    local image
    image=$(docker compose config --images "$SERVICE_NAME" 2>/dev/null | head -1 || true)
    if [ -z "$image" ]; then
        image=$(docker compose config "$SERVICE_NAME" 2>/dev/null | sed -n 's/^[[:space:]]*image:[[:space:]]*\([^[:space:]]*\)[[:space:]]*$/\1/p' | head -1)
    fi
    for allowed in $IMAGE_ALLOWLIST; do
        [ "$image" = "$allowed" ] && printf '%s\n' "$image" && return 0
    done
    log "ERROR: Unexpected or missing image '$image'"
    return 1
}

detect_project_dir() {
    local project_dir
    project_dir="${HOST_PROJECT_DIR:-}"
    if [ -z "$project_dir" ]; then
        project_dir=$(docker inspect "$(hostname)" --format '{{range .Mounts}}{{if eq .Destination "/host"}}{{.Source}}{{end}}{{end}}' 2>/dev/null || true)
    fi
    if [ -z "$project_dir" ]; then
        project_dir=$(docker inspect "$(hostname)" --format '{{range .Mounts}}{{if eq .Destination "/host/data"}}{{.Source}}{{end}}{{end}}' 2>/dev/null | sed 's|/data$||')
    fi
    [ -n "$project_dir" ] || return 1
    case "$project_dir" in
        *$'\n'*|*$'\r'*|*:*) return 1 ;;
    esac
    printf '%s\n' "$project_dir"
}

escape_sed_replacement() {
    printf '%s' "$1" | sed 's/\$/$$/g; s/[&|\\]/\\&/g'
}

make_runtime_compose() {
    local project_dir project_dir_sed out
    project_dir="$1"
    out="$2"
    project_dir_sed=$(escape_sed_replacement "$project_dir")
    sed \
        -e "s|- \./data|- $project_dir_sed/data|g" \
        -e "s|- \./secrets\.env|- $project_dir_sed/secrets.env|g" \
        docker-compose.yml > "$out"
}

protected_fingerprint() {
    local out path meta sum target resolved
    out="$1"
    : > "$out"
    for path in secrets.env docker-compose.yml Justfile updater.sh data/config.yaml data/wallet.json; do
        if [ -L "$path" ]; then
            target=$(readlink "$path" 2>/dev/null || true)
            printf '%s|symlink|%s\n' "$path" "$target" >> "$out"
            resolved=$(readlink -f "$path" 2>/dev/null || true)
            if [ -n "$resolved" ] && [ -f "$resolved" ]; then
                meta=$(stat -c '%u:%g:%a:%s' "$resolved" 2>/dev/null || echo "stat_error")
                sum=$(sha256sum "$resolved" 2>/dev/null | awk '{print $1}' || echo "sha256_error")
                printf '%s|symlink-target|%s|%s|%s\n' "$path" "$resolved" "$meta" "$sum" >> "$out"
            else
                printf '%s|symlink-target-missing|%s\n' "$path" "$resolved" >> "$out"
            fi
        elif [ -f "$path" ]; then
            meta=$(stat -c '%u:%g:%a:%s' "$path" 2>/dev/null || echo "stat_error")
            sum=$(sha256sum "$path" 2>/dev/null | awk '{print $1}' || echo "sha256_error")
            printf '%s|file|%s|%s\n' "$path" "$meta" "$sum" >> "$out"
        elif [ -d "$path" ]; then
            meta=$(stat -c '%u:%g:%a' "$path" 2>/dev/null || echo "stat_error")
            printf '%s|dir|%s\n' "$path" "$meta" >> "$out"
        else
            printf '%s|missing\n' "$path" >> "$out"
        fi
    done
}

verify_fingerprint() {
    local before after rc
    before="$1"
    after=$(mktemp /tmp/micromech-fingerprint-XXXXXX)
    protected_fingerprint "$after"
    cmp -s "$before" "$after"
    rc=$?
    if [ "$rc" -ne 0 ]; then
        log "ERROR: protected artifacts changed during update"
        diff -u "$before" "$after" 2>/dev/null || true
    fi
    rm -f "$after"
    return "$rc"
}

wait_healthy() {
    local max i container_id status health
    max="$HEALTH_TIMEOUT_SECONDS"
    i=0
    while [ "$i" -lt "$max" ]; do
        container_id=$(docker compose ps -q "$SERVICE_NAME" 2>/dev/null || true)
        if [ -n "$container_id" ]; then
            status=$(docker inspect --format='{{.State.Status}}' "$container_id" 2>/dev/null || echo "gone")
            health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id" 2>/dev/null || echo "none")
        else
            status="gone"
            health="none"
        fi
        if [ "$status" = "running" ] && [ "$health" = "healthy" ]; then
            log "Micromech is healthy"
            return 0
        fi
        if [ "$status" = "running" ] && [ "$health" = "none" ]; then
            log "ERROR: Micromech has no Docker healthcheck"
            return 2
        fi
        sleep 1
        i=$((i + 1))
    done
    log "ERROR: Micromech failed to become healthy within ${max}s (status=${status:-unknown} health=${health:-unknown})"
    return 1
}

rollback_image() {
    local image
    image="$1"
    if docker image inspect "${image%:latest}:rollback-prev" >/dev/null 2>&1; then
        docker tag "${image%:latest}:rollback-prev" "$image" \
            && log "Image tag restored from rollback-prev" \
            || return 1
    else
        log "ERROR: rollback-prev image not found"
        return 1
    fi
}

compose_up_service() {
    local compose_file
    compose_file="$1"
    docker compose -f "$compose_file" --project-directory . up -d --no-deps "$SERVICE_NAME"
}

rollback_runtime() {
    local image old new compose_file fingerprints
    image="$1"
    old="$2"
    new="$3"
    compose_file="$4"
    fingerprints="$5"

    log "Rolling back to previous image..."
    docker compose stop "$SERVICE_NAME" 2>/dev/null || true
    if ! rollback_image "$image"; then
        write_result "error:rollback_failed"
        return 1
    fi
    if ! compose_up_service "$compose_file"; then
        write_result "error:rollback_failed"
        return 1
    fi
    if ! wait_healthy; then
        write_result "error:rollback_failed"
        return 1
    fi
    if ! verify_fingerprint "$fingerprints"; then
        write_result "error:rollback_failed"
        return 1
    fi
    write_result "rolled_back:$old:$new"
    return 0
}

handle_restart() {
    log "Restarting Micromech..."
    docker compose restart "$SERVICE_NAME"
    if ! wait_healthy; then
        write_result "error:crashloop"
    fi
}

handle_update() {
    local image project_dir compose_file fingerprints old_image old old_id new new_id
    image=$(detect_image) || { write_result "error:image_detection_failed"; return 0; }
    project_dir=$(detect_project_dir) || { write_result "error:project_dir_detection_failed"; return 0; }
    compose_file=$(mktemp /tmp/micromech-compose-runtime-XXXXXX)
    fingerprints=$(mktemp /tmp/micromech-fingerprint-before-XXXXXX)
    trap 'rm -f "${compose_file:-}" "${fingerprints:-}"' RETURN

    make_runtime_compose "$project_dir" "$compose_file" || { write_result "error:compose_prepare_failed"; return 0; }
    protected_fingerprint "$fingerprints"

    old_image=$(running_service_image_id)
    old_image=${old_image:-$image}
    old=$(image_version "$old_image")
    old_id=$(image_id "$old_image")
    if [ "$old_id" = "none" ]; then
        write_result "error:rollback_unavailable"
        return 0
    fi
    if ! docker tag "$old_image" "${image%:latest}:rollback-prev"; then
        write_result "error:rollback_unavailable"
        return 0
    fi

    if ! docker compose pull "$SERVICE_NAME"; then
        write_result "error:pull_failed"
        return 0
    fi

    new=$(image_version "$image")
    new_id=$(image_id "$image")
    if [ "$old" = "$new" ] && [ "$old_id" = "$new_id" ]; then
        if verify_fingerprint "$fingerprints"; then
            write_result "current:$old"
        else
            write_result "error:artifact_mutation"
        fi
        return 0
    fi

    log "Starting Micromech image $new..."
    if ! compose_up_service "$compose_file"; then
        rollback_runtime "$image" "$old" "$new" "$compose_file" "$fingerprints" || true
        return 0
    fi
    if ! wait_healthy; then
        rollback_runtime "$image" "$old" "$new" "$compose_file" "$fingerprints" || true
        return 0
    fi
    if ! verify_fingerprint "$fingerprints"; then
        rollback_runtime "$image" "$old" "$new" "$compose_file" "$fingerprints" || write_result "error:artifact_mutation"
        return 0
    fi
    write_result "updated:$old:$new"
}

process_request() {
    local action
    if [ -f data/.update-state ]; then
        log "ERROR: stale update state found; previous updater run was interrupted"
        rm -f data/.update-state 2>/dev/null || true
        rm -f data/.update-request 2>/dev/null || true
        write_result "error:interrupted_update"
        return 0
    fi
    if [ ! -f data/.update-request ]; then
        return 1
    fi
    mv data/.update-request data/.update-state 2>/dev/null || return 0
    action=$(cat data/.update-state 2>/dev/null || true)
    action=$(printf '%s' "$action" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)
    case "$action" in
        restart) handle_restart ;;
        update) handle_update ;;
        *) log "WARNING: Ignoring unknown action: $action" ;;
    esac
    rm -f data/.update-state 2>/dev/null || true
    return 0
}

cd "$HOST_ROOT"
data_safe || { log "CRITICAL: data/ is missing or unsafe"; exit 1; }

log "Ready - watching for update requests"
while true; do
    if ! data_safe; then
        log "CRITICAL: data/ is missing or unsafe"
        sleep "$POLL_SECONDS"
        continue
    fi
    process_request || true
    [ "${UPDATER_ONCE:-0}" = "1" ] && exit 0
    sleep "$POLL_SECONDS"
done
