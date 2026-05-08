#!/bin/bash

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

MICROMECH_IMAGE="dvilela/micromech:latest"

detect_micromech_image() {
    local image=""
    if [ -f docker-compose.yml ] && docker compose version >/dev/null 2>&1; then
        image=$(docker compose config --images micromech 2>/dev/null | head -1 || true)
        case "$image" in
            dvilela/micromech:latest|dvilela/micromech-testing:latest) echo "$image"; return 0 ;;
            *) image="" ;;
        esac
    fi
    if [ -f docker-compose.yml ]; then
        image=$(sed -n 's/^[[:space:]]*image:[[:space:]]*\(dvilela\/micromech\(-testing\)\?:latest\)[[:space:]]*$/\1/p' docker-compose.yml | head -1)
        case "$image" in
            dvilela/micromech:latest|dvilela/micromech-testing:latest) echo "$image"; return 0 ;;
            *) image="" ;;
        esac
    fi
    echo "${image:-$MICROMECH_IMAGE}"
}

resolve_install_user() {
    local existing_user user_uid user_gid
    if [ -f docker-compose.yml ]; then
        existing_user=$(sed -n 's/^[[:space:]]*user:[[:space:]]*//p' docker-compose.yml | head -1)
        if [ -n "$existing_user" ]; then
            existing_user=$(printf '%s' "$existing_user" | sed 's/[[:space:]]*$//; s/^"//; s/"$//')
            if [[ ! "$existing_user" =~ ^[0-9]+:[0-9]+$ ]]; then
                echo "ERROR: invalid user in docker-compose.yml: $existing_user" >&2
                return 1
            fi
            echo "$existing_user"
            return 0
        fi
    fi

    if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ] && [ "${SUDO_USER:-}" != "root" ]; then
        user_uid=$(id -u "$SUDO_USER" 2>/dev/null || true)
        user_gid=$(id -g "$SUDO_USER" 2>/dev/null || true)
        if [ -n "$user_uid" ] && [ -n "$user_gid" ]; then
            echo "$user_uid:$user_gid"
            return 0
        fi
        echo "ERROR: could not resolve sudo user '$SUDO_USER'" >&2
        return 1
    fi

    echo "$(id -u):$(id -g)"
}

resolve_human_user() {
    local user_uid user_gid
    if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ] && [ "${SUDO_USER:-}" != "root" ]; then
        user_uid=$(id -u "$SUDO_USER" 2>/dev/null || true)
        user_gid=$(id -g "$SUDO_USER" 2>/dev/null || true)
        if [ -n "$user_uid" ] && [ -n "$user_gid" ]; then
            echo "$user_uid:$user_gid"
            return 0
        fi
        echo "ERROR: could not resolve sudo user '$SUDO_USER'" >&2
        return 1
    fi

    echo "$(id -u):$(id -g)"
}

chown_install_dir_if_needed() {
    local install_dir="$1"
    local owner="$2"
    [ "$(id -u)" -eq 0 ] || return 0
    [ -n "$owner" ] || return 0
    if [ -L "$install_dir" ]; then
        echo "ERROR: refusing to chown symlinked install directory: $install_dir" >&2
        return 1
    fi
    chown -R -- "$owner" "$install_dir"
}

chown_generated_artifacts_if_needed() {
    local owner="$1"
    [ "$(id -u)" -eq 0 ] || return 0
    [ -n "$owner" ] || return 0
    for artifact in docker-compose.yml Justfile updater.sh; do
        if [ -e "$artifact" ] && [ ! -L "$artifact" ]; then
            chown -- "$owner" "$artifact" || return 1
        fi
    done
}

migrate_compose_secrets_mount() {
    [ -f docker-compose.yml ] || return 0
    if [ -L docker-compose.yml ]; then
        echo "📝 docker-compose.yml is managed externally — skipping local compose migration"
        return 0
    fi
    if grep -Eq '^[[:space:]]*-[[:space:]]+.*:/app/secrets\.env:ro[[:space:]]*$' docker-compose.yml; then
        local tmp status
        tmp=$(mktemp docker-compose.yml.XXXXXX) || return 1
        awk '
            /^[[:space:]]*-[[:space:]]+.*:\/app\/secrets\.env:ro[[:space:]]*$/ {
                sub(/:\/app\/secrets\.env:ro[[:space:]]*$/, ":/app/secrets.env")
                changed = 1
            }
            { print }
            END {
                if (!changed) {
                    exit 42
                }
            }
        ' docker-compose.yml > "$tmp"
        status=$?
        if [ "$status" -ne 0 ]; then
            rm -f "$tmp"
            return 1
        fi
        chmod --reference=docker-compose.yml "$tmp" 2>/dev/null || true
        chown --reference=docker-compose.yml "$tmp" 2>/dev/null || true
        mv "$tmp" docker-compose.yml
        echo "📝 Made secrets.env bind mount writable in docker-compose.yml"
        return 0
    fi
    if grep -Eq '^[[:space:]]*-[[:space:]]+.*:/app/secrets\.env(:rw)?[[:space:]]*$' docker-compose.yml; then
        return 0
    fi
    if grep -q '/app/secrets\.env' docker-compose.yml; then
        return 0
    fi

    local tmp status
    tmp=$(mktemp docker-compose.yml.XXXXXX) || return 1
    awk '
        {
            print
            if (!inserted && $0 ~ /^[[:space:]]*-[[:space:]]+.*:\/app\/data(:|[[:space:]]*$)/) {
                match($0, /^[[:space:]]*/)
                print substr($0, RSTART, RLENGTH) "- ./secrets.env:/app/secrets.env"
                inserted = 1
            }
        }
        END {
            if (!inserted) {
                exit 42
            }
        }
    ' docker-compose.yml > "$tmp"
    status=$?
    if [ "$status" -ne 0 ]; then
        rm -f "$tmp"
        if [ "$status" -eq 42 ]; then
            echo "ERROR: could not find /app/data volume in docker-compose.yml; refusing unsafe secrets.env migration" >&2
        fi
        return 1
    fi
    chmod --reference=docker-compose.yml "$tmp" 2>/dev/null || true
    chown --reference=docker-compose.yml "$tmp" 2>/dev/null || true
    mv "$tmp" docker-compose.yml
    echo "📝 Added secrets.env bind mount to docker-compose.yml"
}

# --- docker-compose.yml generator (also used by 'just update-config') ---
generate_compose() {
    local user_uid user_gid user_ids
    if [ -n "${1:-}" ]; then
        user_ids="$1"
    else
        user_ids=$(resolve_install_user) || return 1
    fi
    user_uid=$(echo "$user_ids" | cut -d: -f1)
    user_gid=$(echo "$user_ids" | cut -d: -f2)

    # Preserve existing host volume paths before overwriting
    local _vol_preserve=""
    if [ -f docker-compose.yml ]; then
        _vol_preserve=$(grep -E '^\s*-\s+.+:/' docker-compose.yml || true)
    fi

    local image
    image=$(detect_micromech_image)
    if ! docker run --rm --entrypoint cat "$image" /app/docker-compose.yml > docker-compose.yml.tmp || [ ! -s docker-compose.yml.tmp ]; then
        rm -f docker-compose.yml.tmp
        echo "ERROR: Could not extract a valid docker-compose.yml from $image" >&2
        return 1
    fi

    # Portable sed -i (works on both GNU and BSD/macOS)
    _sed_i() { if [ "$(uname)" = "Darwin" ]; then sed -i '' "$@"; else sed -i "$@"; fi; }

    # Transform for end-user deployment:
    _sed_i "s|    build:|    image: $image|" docker-compose.yml.tmp
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

    local host_project_dir host_project_dir_compose host_project_dir_yaml host_project_dir_sed
    host_project_dir="${HOST_PROJECT_DIR:-$(pwd)}"
    case "$host_project_dir" in
        *$'\n'*|*$'\r'*)
            echo "ERROR: Project directory contains a newline; cannot generate docker-compose.yml safely." >&2
            exit 1
            ;;
    esac
    host_project_dir_compose=$(printf '%s' "$host_project_dir" | sed 's/\$/$$/g')
    host_project_dir_yaml=$(printf '%s' "$host_project_dir_compose" | sed "s/'/''/g")
    host_project_dir_sed=$(printf '%s' "$host_project_dir_yaml" | sed 's/[&|\\]/\\&/g')

    # Add updater sidecar service (if not already present)
    if ! grep -q 'updater:' docker-compose.yml.tmp; then
    cat >> docker-compose.yml.tmp << 'UPDATER_COMPOSE'
  dockerproxy:
    image: tecnativa/docker-socket-proxy
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - PING=1
      - VERSION=1
      - INFO=1
      - DELETE=1
      - CONTAINERS=1
      - POST=1
      - ALLOW_START=1
      - ALLOW_STOP=1
      - ALLOW_RESTARTS=1
      - IMAGES=1
      - NETWORKS=1
      - VOLUMES=1
    restart: unless-stopped
    networks:
      - updater_net

  updater:
    image: docker:cli
    environment:
      - TZ=${TZ:-Europe/Madrid}
      - DOCKER_HOST=tcp://dockerproxy:2375
      - 'HOST_PROJECT_DIR=__HOST_PROJECT_DIR__'
      - "UPDATER_RUN_AS=__USER_UID_GID__"
    volumes:
      - ./:/host
    working_dir: /host
    cap_drop:
      - ALL
    cap_add:
      - SETUID
      - SETGID
    security_opt:
      - no-new-privileges:true
    restart: unless-stopped
    command: ["sh", "-c", "while [ ! -f ./updater.sh ]; do sleep 5; done; apk add --no-cache bash su-exec >/dev/null && exec su-exec \"$${UPDATER_RUN_AS}\" bash ./updater.sh"]
    depends_on:
      - dockerproxy
    networks:
      - updater_net

networks:
  updater_net:
    driver: bridge
UPDATER_COMPOSE
    fi
    _sed_i "s|__USER_UID_GID__|${user_uid}:${user_gid}|" docker-compose.yml.tmp
    _sed_i "s|__HOST_PROJECT_DIR__|${host_project_dir_sed}|" docker-compose.yml.tmp

    # Set fixed project name if not already present
    if ! grep -q '^name:' docker-compose.yml.tmp; then
        { printf 'name: micromech\n'; cat docker-compose.yml.tmp; } > docker-compose.yml.tmp2
        mv docker-compose.yml.tmp2 docker-compose.yml.tmp
    fi

    if ! docker compose -f docker-compose.yml.tmp config -q \
        || ! docker compose -f docker-compose.yml.tmp config --services | grep -qx micromech \
        || ! docker compose -f docker-compose.yml.tmp config --services | grep -qx dockerproxy \
        || ! docker compose -f docker-compose.yml.tmp config --services | grep -qx updater; then
        rm -f docker-compose.yml.tmp
        echo "ERROR: generated docker-compose.yml is invalid" >&2
        return 1
    fi

    mv docker-compose.yml.tmp docker-compose.yml
}

# --- Justfile generator (also used by 'just update-config') ---
generate_justfile() {
    local user_uid user_gid user_ids
    if [ -n "${1:-}" ]; then
        user_ids="$1"
    else
        user_ids=$(resolve_install_user) || return 1
    fi
    user_uid=$(echo "$user_ids" | cut -d: -f1)
    user_gid=$(echo "$user_ids" | cut -d: -f2)
    local image
    image=$(detect_micromech_image)

    cat <<JUSTEOF > Justfile
set shell := ["bash", "-uc"]

up:
    docker compose up -d --remove-orphans
    @echo "🚀 Micromech is running!"
    @echo "🌐 Dashboard: http://localhost:8090"
    @echo "📜 View logs with: just logs"

down:
    docker compose down --remove-orphans

logs:
    docker compose logs -f

update:
    #!/usr/bin/env bash
    set -e
    normalize_version() {
        local value="\$1"
        value=\$(printf '%s' "\$value" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)
        case "\$value" in ""|novalue|unknown) echo "unknown" ;; *) echo "\$value" ;; esac
    }
    image_version() {
        local value
        value=\$(docker inspect --format '{{{{index .Config.Labels "org.dvilela.micromech.version"}}' "\$1" 2>/dev/null || true)
        normalize_version "\$value"
    }
    image_id() { docker inspect --format '{{{{.Id}}' "\$1" 2>/dev/null || echo "none"; }
    running_image_id() {
        local cid
        cid=\$(docker compose ps -q micromech 2>/dev/null | head -1 || true)
        [ -n "\$cid" ] && docker inspect --format '{{{{.Image}}' "\$cid" 2>/dev/null || true
    }
    resolve_image() {
        local image
        image=\$(docker compose config --images micromech 2>/dev/null | head -1 || true)
        case "\$image" in
            dvilela/micromech:latest|dvilela/micromech-testing:latest) echo "\$image" ;;
            *) echo "$image" ;;
        esac
    }
    MICROMECH_IMAGE=\$(resolve_image)
    current_image=\$(running_image_id)
    current_image=\${current_image:-\$MICROMECH_IMAGE}
    current=\$(image_version "\$current_image")
    current_digest=\$(image_id "\$current_image")
    docker compose pull micromech
    new=\$(image_version "\$MICROMECH_IMAGE")
    new_digest=\$(image_id "\$MICROMECH_IMAGE")
    if [ "\$current" != "\$new" ] || [ "\$current_digest" != "\$new_digest" ]; then
        echo "🔄 Updated v\$current -> v\$new"
        echo "📝 Updating config files..."
        qs_tmp=\$(mktemp /tmp/micromech-qs-XXXXXX)
        trap 'rm -f "\$qs_tmp"' EXIT
        docker run --rm --entrypoint cat "\$MICROMECH_IMAGE" /app/scripts/quickstart.sh > "\$qs_tmp"
        bash -n "\$qs_tmp"
        UPDATE_CONFIG=1 bash "\$qs_tmp"
        rm -f "\$qs_tmp"
        trap - EXIT
        docker compose up -d
    else
        echo "✅ Already at latest Micromech image (v\$current)"
    fi
    # Warn if Docker has significant reclaimable space
    waste_gb=\$(if command -v timeout >/dev/null 2>&1; then timeout 5 docker system df 2>/dev/null || true; fi | tail -n +2 | awk '{v=\$NF; if (v ~ /^\\(/) v=\$(NF-1); n=v; gsub(/[^0-9.]/, "", n); u=toupper(v); gsub(/[0-9.]/, "", u); if (u ~ /^TB/) t+=n*1024; else if (u ~ /^GB/) t+=n; else if (u ~ /^MB/) t+=n/1024; else if (u ~ /^KB/) t+=n/1048576; else if (u ~ /^B/) t+=n/1073741824} END {printf "%.0f", t}')
    if [ "\${waste_gb:-0}" -ge 5 ]; then
        echo "⚠️  Docker has ~\${waste_gb}GB of reclaimable space. Run 'docker system prune' to free disk."
    fi

update-config:
    #!/usr/bin/env bash
    set -e
    echo "📝 Updating Justfile and docker-compose.yml from latest Docker image..."
    MICROMECH_IMAGE=\$(docker compose config --images micromech 2>/dev/null | head -1 || true)
    case "\$MICROMECH_IMAGE" in
        dvilela/micromech:latest|dvilela/micromech-testing:latest) ;;
        *) MICROMECH_IMAGE="$image" ;;
    esac
    qs_tmp=\$(mktemp /tmp/micromech-qs-XXXXXX)
    trap 'rm -f "\$qs_tmp"' EXIT
    docker run --rm --entrypoint cat "\$MICROMECH_IMAGE" /app/scripts/quickstart.sh > "\$qs_tmp"
    bash -n "\$qs_tmp"
    UPDATE_CONFIG=1 bash "\$qs_tmp"
    rm -f "\$qs_tmp"
    trap - EXIT
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
        $image \\
        python -m micromech init

doctor:
    @echo "🔍 Running health check..."
    @docker run --rm -it \\
        --entrypoint "" \\
        --user "$user_uid:$user_gid" \\
        --volume "\$(pwd)/data:/app/data" \\
        --env-file "\$(pwd)/secrets.env" \\
        $image \\
        python -m micromech doctor
JUSTEOF
}

# --- Updater sidecar script generator ---
generate_updater_script() {
    local image tmp
    image=$(detect_micromech_image)
    tmp=$(mktemp updater.XXXXXX)
    if ! docker run --rm --entrypoint cat "$image" /app/scripts/updater.sh > "$tmp" || [ ! -s "$tmp" ] || ! bash -n "$tmp"; then
        rm -f "$tmp"
        echo "ERROR: Could not extract a valid updater.sh from $image" >&2
        return 1
    fi
    mv "$tmp" updater.sh
    chmod +x updater.sh
}

is_managed_install() {
    [ -L "Justfile" ] || [ -L "docker-compose.yml" ] || [ -L "updater.sh" ]
}

# Self-update mode: regenerate config files, then exit
if [ "${UPDATE_CONFIG:-}" = "1" ]; then
    # Managed installation: generated artifacts are symlinked and repo-managed.
    if is_managed_install; then
        echo "📝 Managed installation detected — applying safe local migrations only"
        migrate_compose_secrets_mount || exit 1
        exit 0
    fi
    UPDATE_BACKUP_DIR=$(mktemp -d /tmp/micromech-update-config-XXXXXX) || exit 1
    for artifact in docker-compose.yml Justfile updater.sh; do
        if [ -e "$artifact" ] && ! cp -p "$artifact" "$UPDATE_BACKUP_DIR/$artifact"; then
            rm -rf "$UPDATE_BACKUP_DIR"
            exit 1
        fi
    done
    restore_update_config_artifacts() {
        local artifact
        for artifact in docker-compose.yml Justfile updater.sh; do
            if [ -e "$UPDATE_BACKUP_DIR/$artifact" ]; then
                cp -p "$UPDATE_BACKUP_DIR/$artifact" "$artifact"
            else
                rm -f "$artifact"
            fi
        done
        [ ! -L updater.sh ] && [ -f updater.sh ] && chmod +x updater.sh
        rm -rf "$UPDATE_BACKUP_DIR"
    }
    if ! generate_compose || ! generate_justfile || ! generate_updater_script; then
        restore_update_config_artifacts
        exit 1
    fi
    if ! INSTALL_OWNER=$(resolve_install_user); then
        restore_update_config_artifacts
        exit 1
    fi
    if ! chown_generated_artifacts_if_needed "$INSTALL_OWNER"; then
        restore_update_config_artifacts
        exit 1
    fi
    rm -rf "$UPDATE_BACKUP_DIR"
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
INSTALL_DIR="$(pwd)/micromech"

if [ -L "$INSTALL_DIR" ]; then
    echo -e "${RED}❌ Refusing to use symlinked install directory '$INSTALL_DIR'.${NC}"
    exit 1
fi

if [ -d "$INSTALL_DIR" ] \
    && { [ -L "$INSTALL_DIR/Justfile" ] || [ -L "$INSTALL_DIR/docker-compose.yml" ] || [ -L "$INSTALL_DIR/updater.sh" ]; }; then
    echo -e "${RED}❌ Managed installation detected — refusing to overwrite symlinked artifacts.${NC}"
    exit 1
fi

if ! mkdir -p "$INSTALL_DIR/data" 2>/dev/null; then
    echo -e "${RED}❌ Cannot create directory '$INSTALL_DIR'. Permission denied.${NC}"
    if [ "$(id -u)" -ne 0 ]; then
        echo -e "   To install here, run with sudo:"
        echo -e "   ${BLUE}curl -sSL https://raw.githubusercontent.com/dvilelaf/micromech/main/scripts/quickstart.sh | sudo bash${NC}"
        echo -e "   Or install in your home directory instead:"
        echo -e "   ${BLUE}cd ~ && bash <(curl -sSL https://raw.githubusercontent.com/dvilelaf/micromech/main/scripts/quickstart.sh)${NC}"
    fi
    exit 1
fi

echo -e "${BLUE}📂 Setting up directory '$INSTALL_DIR'...${NC}"

if [ -d "$INSTALL_DIR" ] && [ "$(ls -A "$INSTALL_DIR" 2>/dev/null)" ]; then
    echo -e "Directory already exists. Updating config files..."
    echo -e "(Your data/ and secrets.env are preserved.)"
fi
cd "$INSTALL_DIR"

if is_managed_install; then
    echo -e "${RED}❌ Managed installation detected — refusing to overwrite symlinked artifacts.${NC}"
    exit 1
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
INSTALL_OWNER=$(resolve_human_user) || exit 1
generate_compose "$INSTALL_OWNER" || exit 1

# 3. Justfile (Generated minimal version for end-users)
echo -e "   - Justfile..."
generate_justfile "$INSTALL_OWNER" || exit 1

# 4. updater.sh (Sidecar script for remote updates)
echo -e "   - updater.sh..."
generate_updater_script || exit 1

# Explicit cleanup for non-error exit
cleanup
trap - EXIT

chown_install_dir_if_needed "$INSTALL_DIR" "$INSTALL_OWNER" || exit 1

# Start micromech
echo
echo -e "${BLUE}🚀 Starting Micromech...${NC}"
docker compose up -d

echo
echo -e "${GREEN}🎉 Micromech is running!${NC}"
echo
echo -e "  Open: ${BLUE}http://localhost:8090${NC}"
echo
