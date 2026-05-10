#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_NAME="Micromech"
SERVICE_NAME="micromech"
DEFAULT_IMAGE="dvilela/micromech:latest"
TESTING_IMAGE="dvilela/micromech-testing:latest"
PROJECT_DIR_NAME="micromech"
PROJECT_IMAGE="${MICROMECH_IMAGE:-${IMAGE:-}}"

log() {
    printf '%s\n' "$*"
}

warn() {
    printf 'WARNING: %s\n' "$*" >&2
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

compose() {
    "${DOCKER_COMPOSE[@]}" "$@"
}

compose_has_service() {
    compose config --services 2>/dev/null | grep -qx "$SERVICE_NAME" \
        || grep -Eq "^[[:space:]]+$SERVICE_NAME:" docker-compose.yml 2>/dev/null
}

select_compose() {
    if docker compose version >/dev/null 2>&1; then
        DOCKER_COMPOSE=(docker compose)
    else
        die "Docker Compose v2 is required"
    fi
}

enter_project_dir() {
    if [ -f docker-compose.yml ]; then
        if compose_has_service; then
            return 0
        fi
        if [ -f "$PROJECT_DIR_NAME/docker-compose.yml" ]; then
            cd "$PROJECT_DIR_NAME"
            return 0
        fi
        die "current docker-compose.yml does not look like a $PROJECT_DIR_NAME deployment"
    fi
    if [ -f "$PROJECT_DIR_NAME/docker-compose.yml" ]; then
        cd "$PROJECT_DIR_NAME"
        return 0
    fi
    die "run this from the $PROJECT_DIR_NAME deployment directory"
}

allow_image() {
    case "$1" in
        "$DEFAULT_IMAGE"|"$TESTING_IMAGE") return 0 ;;
        *) return 1 ;;
    esac
}

image_from_compose() {
    local image
    image=$(compose config --images "$SERVICE_NAME" 2>/dev/null | head -1 || true)
    if allow_image "$image"; then
        printf '%s\n' "$image"
        return 0
    fi

    image=$(compose config --images 2>/dev/null | while IFS= read -r candidate; do
        if allow_image "$candidate"; then
            printf '%s\n' "$candidate"
            break
        fi
    done || true)
    if allow_image "$image"; then
        printf '%s\n' "$image"
        return 0
    fi

    image=$(awk '
        /^[[:space:]]*image:[[:space:]]*/ {
            value = $0
            sub(/^[[:space:]]*image:[[:space:]]*/, "", value)
            gsub(/^[\"\047]|[\"\047]$/, "", value)
            if (value == "dvilela/micromech:latest" || value == "dvilela/micromech-testing:latest") {
                print value
                exit
            }
        }
    ' docker-compose.yml 2>/dev/null || true)
    if allow_image "$image"; then
        printf '%s\n' "$image"
        return 0
    fi

    return 1
}

image_from_config() {
    local channel
    channel=$(awk '
        /^[[:space:]]*update_channel:[[:space:]]*/ {
            value = $0
            sub(/#.*/, "", value)
            sub(/^[[:space:]]*update_channel:[[:space:]]*/, "", value)
            gsub(/^[[:space:]\"\047]+|[[:space:]\"\047]+$/, "", value)
            print value
            exit
        }
    ' data/config.yaml 2>/dev/null || true)
    case "$channel" in
        testing) printf '%s\n' "$TESTING_IMAGE"; return 0 ;;
        release|"") ;;
        *) printf '%s\n' "$DEFAULT_IMAGE"; return 0 ;;
    esac
    if [ "$channel" = "release" ]; then
        printf '%s\n' "$DEFAULT_IMAGE"
        return 0
    fi
    return 1
}

resolve_image() {
    local image
    if [ -n "$PROJECT_IMAGE" ]; then
        allow_image "$PROJECT_IMAGE" || die "unsupported image override: $PROJECT_IMAGE"
        printf '%s\n' "$PROJECT_IMAGE"
        return 0
    fi

    image=$(image_from_compose || true)
    if allow_image "$image"; then
        printf '%s\n' "$image"
        return 0
    fi

    image=$(image_from_config || true)
    if allow_image "$image"; then
        printf '%s\n' "$image"
        return 0
    fi

    printf '%s\n' "$DEFAULT_IMAGE"
}

extract_quickstart() {
    local image="$1"
    QUICKSTART_TMP=$(mktemp "/tmp/${SERVICE_NAME}-quickstart.XXXXXX.sh")
    trap 'rm -f "${QUICKSTART_TMP:-}"' EXIT
    docker run --rm --entrypoint cat "$image" /app/scripts/quickstart.sh > "$QUICKSTART_TMP"
    [ -s "$QUICKSTART_TMP" ] || die "could not extract quickstart.sh from $image"
    bash -n "$QUICKSTART_TMP" || die "extracted quickstart.sh is not valid bash"
}

refuse_managed_install() {
    local artifact
    for artifact in docker-compose.yml Justfile updater.sh; do
        if [ -L "$artifact" ]; then
            die "$artifact is a symlink; this managed install cannot be repaired in-place by this script"
        fi
    done
}

verify_generated_files() {
    local expected_image="$1"
    local generated_image
    [ -f docker-compose.yml ] || die "docker-compose.yml was not generated"
    [ -f Justfile ] || die "Justfile was not generated"
    [ -f updater.sh ] || die "updater.sh was not generated"
    bash -n updater.sh || die "generated updater.sh is not valid bash"
    compose config -q || die "generated docker-compose.yml is invalid"
    compose config --services | grep -qx "$SERVICE_NAME" || die "missing $SERVICE_NAME service"
    compose config --services | grep -qx updater || die "missing updater service"
    compose config | grep -Eq 'UPDATER_RUN_AS[[:space:]]*[:=]' || die "missing UPDATER_RUN_AS in updater service"
    generated_image=$(compose config --images "$SERVICE_NAME" 2>/dev/null | head -1 || true)
    [ "$generated_image" = "$expected_image" ] || die "generated image $generated_image does not match expected $expected_image"
}

restart_updater() {
    log "Starting updater sidecar..."
    compose up -d --force-recreate dockerproxy updater
}

update_service() {
    if [ "${REPAIR_ONLY:-0}" = "1" ]; then
        log "Repair complete. REPAIR_ONLY=1, not updating $SERVICE_NAME."
        return 0
    fi
    log "Updating $SERVICE_NAME service..."
    compose pull "$SERVICE_NAME"
    compose up -d --remove-orphans
}

permission_check() {
    [ "${SKIP_PERMISSION_CHECK:-0}" = "1" ] && return 0
    if ! compose ps -q updater >/dev/null 2>&1; then
        warn "could not inspect updater container; skipping permission check"
        return 0
    fi

    local ready=0
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        if compose exec -T updater sh -lc 'command -v su-exec >/dev/null 2>&1' >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 1
    done
    if [ "$ready" -ne 1 ]; then
        warn "updater sidecar is not ready for permission check yet"
        return 0
    fi

    if compose exec -T updater sh -lc 'test -d /host/data && su-exec "$UPDATER_RUN_AS" sh -lc "touch /host/data/.updater-permission-test && rm -f /host/data/.updater-permission-test"' >/dev/null; then
        log "Updater permission check passed."
    else
        warn "updater permission check failed; inspect docker-compose.yml ownership before relying on auto-updates"
    fi
}

main() {
    need_cmd docker
    select_compose
    enter_project_dir

    local image
    image=$(resolve_image)
    log "Repairing $PROJECT_NAME updater in $(pwd)"
    log "Using image: $image"

    refuse_managed_install
    docker pull "$image"
    extract_quickstart "$image"

    HOST_PROJECT_DIR="$(pwd)" MICROMECH_IMAGE="$image" UPDATE_CONFIG=1 bash "$QUICKSTART_TMP"
    verify_generated_files "$image"
    restart_updater
    permission_check
    update_service

    log "$PROJECT_NAME updater repair completed."
}

main "$@"
