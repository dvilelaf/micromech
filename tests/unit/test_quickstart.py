"""Tests for quickstart ownership and managed-install guards."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

QUICKSTART_SH = Path(__file__).parent.parent.parent / "scripts" / "quickstart.sh"
REPAIR_UPDATER_SH = Path(__file__).parent.parent.parent / "scripts" / "repair-updater.sh"
UPDATER_SH = Path(__file__).parent.parent.parent / "scripts" / "updater.sh"

IMAGE_COMPOSE_TEMPLATE = """\
services:
  micromech:
    build:
      context: .
      dockerfile: Dockerfile
    user: "1000:1000"
    volumes:
      - ./data:/app/data
      - ./secrets.env:/app/secrets.env
    restart: unless-stopped
    command: ["python", "-m", "micromech"]
"""


def _write_fake_docker(fake_bin: Path) -> None:
    docker = fake_bin / "docker"
    docker.write_text(
        """#!/bin/sh
[ -n "$DOCKER_LOG" ] && echo "$@" >> "$DOCKER_LOG"
case "$*" in
  "compose -f docker-compose.yml.tmp config -q")
    exit 0
    ;;
  "compose -f docker-compose.yml.tmp config --services")
    printf 'micromech\ndockerproxy\nupdater\n'
    ;;
  "compose version")
    exit 0
    ;;
  "compose config --images micromech")
    printf 'dvilela/micromech:latest\\n'
    ;;
  "compose ps -q micromech")
    echo cid-micromech
    ;;
  *cid-micromech*)
    case "$*" in
      *.Image*) echo sha256:old; exit 0 ;;
    esac
    ;;
  *sha256:old*)
    case "$*" in
      *"org.dvilela.micromech.version"*) echo 0.1.0; exit 0 ;;
      *.Id*) echo sha256:old; exit 0 ;;
    esac
    ;;
  "info")
    exit 0
    ;;
  "compose pull micromech")
    exit 0
    ;;
  "pull dvilela/micromech:latest")
    exit 0
    ;;
  *inspect*"dvilela/micromech:latest"*)
    case "$*" in
      *"org.dvilela.micromech.version"*) echo 0.1.2; exit 0 ;;
      *.Id*) echo sha256:new; exit 0 ;;
    esac
    ;;
  "create dvilela/micromech:latest")
    echo fake-container
    ;;
  "rm fake-container")
    exit 0
    ;;
  "cp fake-container:/app/secrets.env.example ./secrets.env")
    printf 'wallet_password=test\\n' > ./secrets.env
    ;;
  "compose up -d")
    exit 0
    ;;
  *"/app/scripts/quickstart.sh"*)
    cat "__QUICKSTART_SH__"
    ;;
  *"/app/docker-compose.yml"*)
    cat <<'EOF'
services:
  micromech:
    build:
      context: .
      dockerfile: Dockerfile
    user: "1000:1000"
    volumes:
      - ./data:/app/data
      - ./secrets.env:/app/secrets.env
    restart: unless-stopped
    command: [ "python", "-m", "micromech" ]
EOF
    ;;
  *"/app/scripts/updater.sh"*)
    printf '#!/bin/sh\\necho refreshed\\n'
    ;;
  *)
    echo "unexpected docker invocation: $*" >&2
    exit 1
    ;;
esac
"""
        .replace("__QUICKSTART_SH__", str(QUICKSTART_SH))
    )
    docker.chmod(0o755)


def _write_fake_id(fake_bin: Path) -> None:
    fake_id = fake_bin / "id"
    fake_id.write_text(
        """#!/bin/sh
case "$1 $2" in
  "-u alice") echo 2001 ;;
  "-g alice") echo 3001 ;;
  "-u ") [ -n "$SUDO_USER" ] && echo 0 || echo 2001 ;;
  "-g ") echo 3001 ;;
  *) exit 1 ;;
esac
"""
    )
    fake_id.chmod(0o755)


def _make_deploy_dir(tmp_path: Path) -> tuple[Path, Path]:
    deploy_dir = tmp_path / "deploy"
    fake_bin = tmp_path / "bin"
    deploy_dir.mkdir()
    fake_bin.mkdir()
    (deploy_dir / "data").mkdir()
    (deploy_dir / "secrets.env").write_text("wallet_password=test\n")
    (deploy_dir / "updater.sh").write_text("#!/bin/sh\necho old\n")
    _write_fake_docker(fake_bin)
    fake_chown = fake_bin / "chown"
    fake_chown.write_text("#!/bin/sh\nexit 0\n")
    fake_chown.chmod(0o755)
    return deploy_dir, fake_bin


def _run_update_config(deploy_dir: Path, fake_bin: Path, sudo_user: str | None = None) -> None:
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["UPDATE_CONFIG"] = "1"
    if sudo_user is None:
        env.pop("SUDO_USER", None)
    else:
        env["SUDO_USER"] = sudo_user
    subprocess.run(["bash", str(QUICKSTART_SH)], cwd=deploy_dir, env=env, check=True)


@pytest.mark.parametrize("sudo_user", [None, "alice"])
def test_update_config_uses_installer_owner(tmp_path: Path, sudo_user: str | None) -> None:
    deploy_dir, fake_bin = _make_deploy_dir(tmp_path)
    _write_fake_id(fake_bin)
    chown_log = tmp_path / "chown.log"
    fake_chown = fake_bin / "chown"
    fake_chown.write_text(f'#!/bin/sh\necho "$@" >> {chown_log}\n')
    fake_chown.chmod(0o755)

    _run_update_config(deploy_dir, fake_bin, sudo_user=sudo_user)

    compose = (deploy_dir / "docker-compose.yml").read_text()
    justfile = (deploy_dir / "Justfile").read_text()
    assert 'user: "2001:3001"' in compose
    assert "UPDATER_RUN_AS=2001:3001" in compose
    assert '--user "2001:3001"' in justfile
    assert "org.dvilela.micromech.version" in justfile
    assert 'index .Config.Labels "version"' not in justfile
    assert "docker compose ps -q micromech" in justfile
    assert "docker compose pull micromech" in justfile
    assert "bash -n \"$qs_tmp\"" in justfile
    if sudo_user is not None:
        assert chown_log.read_text().splitlines() == [
            "-- 2001:3001 docker-compose.yml",
            "-- 2001:3001 Justfile",
            "-- 2001:3001 updater.sh",
        ]
    else:
        assert not chown_log.exists()


def test_update_config_preserves_existing_compose_user(tmp_path: Path) -> None:
    deploy_dir, fake_bin = _make_deploy_dir(tmp_path)
    (deploy_dir / "docker-compose.yml").write_text("services:\n  micromech:\n    user: 4444:5555\n")

    _run_update_config(deploy_dir, fake_bin)

    compose = (deploy_dir / "docker-compose.yml").read_text()
    justfile = (deploy_dir / "Justfile").read_text()
    assert 'user: "4444:5555"' in compose
    assert "UPDATER_RUN_AS=4444:5555" in compose
    assert '--user "4444:5555"' in justfile


def test_generated_just_update_recreates_when_container_lags_local_latest(tmp_path: Path) -> None:
    if not shutil.which("just"):
        pytest.skip("just is not installed")

    deploy_dir, fake_bin = _make_deploy_dir(tmp_path)
    _write_fake_id(fake_bin)
    docker_log = tmp_path / "docker.log"

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["UPDATE_CONFIG"] = "1"
    env["DOCKER_LOG"] = str(docker_log)
    subprocess.run(["bash", str(QUICKSTART_SH)], cwd=deploy_dir, env=env, check=True)
    env.pop("UPDATE_CONFIG")

    result = subprocess.run(
        ["just", "--justfile", str(deploy_dir / "Justfile"), "update"],
        cwd=deploy_dir,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Updated v0.1.0 -> v0.1.2" in result.stdout
    log = docker_log.read_text()
    assert "inspect --format {{.Image}} cid-micromech" in log
    assert (
        "inspect --format {{index .Config.Labels \"org.dvilela.micromech.version\"}} sha256:old"
        in log
    )


def test_update_config_rejects_invalid_existing_compose_user(tmp_path: Path) -> None:
    deploy_dir, fake_bin = _make_deploy_dir(tmp_path)
    (deploy_dir / "docker-compose.yml").write_text("services:\n  micromech:\n    user: --bad\n")

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["UPDATE_CONFIG"] = "1"
    result = subprocess.run(
        ["bash", str(QUICKSTART_SH)],
        cwd=deploy_dir,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "invalid user in docker-compose.yml" in result.stderr


def test_update_config_rejects_empty_generated_compose(tmp_path: Path) -> None:
    deploy_dir, fake_bin = _make_deploy_dir(tmp_path)
    (deploy_dir / "docker-compose.yml").write_text("old-compose\n")

    docker = fake_bin / "docker"
    docker.write_text(
        """#!/bin/sh
case "$*" in
  *"/app/docker-compose.yml"*) exit 0 ;;
  *) echo "unexpected docker invocation: $*" >&2; exit 1 ;;
esac
"""
    )
    docker.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["UPDATE_CONFIG"] = "1"
    result = subprocess.run(
        ["bash", str(QUICKSTART_SH)],
        cwd=deploy_dir,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "Could not extract a valid docker-compose.yml" in result.stderr
    assert (deploy_dir / "docker-compose.yml").read_text() == "old-compose\n"


def test_update_config_restores_artifacts_when_updater_extract_fails(tmp_path: Path) -> None:
    deploy_dir, fake_bin = _make_deploy_dir(tmp_path)
    (deploy_dir / "docker-compose.yml").write_text("old-compose\n")
    (deploy_dir / "Justfile").write_text("old-justfile\n")
    (deploy_dir / "updater.sh").write_text("#!/bin/sh\necho old\n")

    docker = fake_bin / "docker"
    docker.write_text(
        """#!/bin/sh
case "$*" in
  "compose -f docker-compose.yml.tmp config -q")
    exit 0
    ;;
  "compose -f docker-compose.yml.tmp config --services")
    printf 'micromech\ndockerproxy\nupdater\n'
    ;;
  *"/app/docker-compose.yml"*)
    cat <<'EOF'
services:
  micromech:
    build:
      context: .
      dockerfile: Dockerfile
    user: "1000:1000"
    volumes:
      - ./data:/app/data
      - ./secrets.env:/app/secrets.env
    restart: unless-stopped
    command: [ "python", "-m", "micromech" ]
EOF
    ;;
  *"/app/scripts/updater.sh"*)
    exit 0
    ;;
  *)
    echo "unexpected docker invocation: $*" >&2
    exit 1
    ;;
esac
"""
    )
    docker.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["UPDATE_CONFIG"] = "1"
    result = subprocess.run(
        ["bash", str(QUICKSTART_SH)],
        cwd=deploy_dir,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert (deploy_dir / "docker-compose.yml").read_text() == "old-compose\n"
    assert (deploy_dir / "Justfile").read_text() == "old-justfile\n"
    assert (deploy_dir / "updater.sh").read_text() == "#!/bin/sh\necho old\n"


def test_update_config_rejects_unresolvable_sudo_user(tmp_path: Path) -> None:
    deploy_dir, fake_bin = _make_deploy_dir(tmp_path)
    _write_fake_id(fake_bin)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["UPDATE_CONFIG"] = "1"
    env["SUDO_USER"] = "ghost"
    result = subprocess.run(
        ["bash", str(QUICKSTART_SH)],
        cwd=deploy_dir,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "could not resolve sudo user 'ghost'" in result.stderr


def test_full_quickstart_uses_human_owner_over_stale_root_compose(tmp_path: Path) -> None:
    parent = tmp_path / "parent"
    deploy_dir = parent / "micromech"
    fake_bin = tmp_path / "bin"
    parent.mkdir()
    deploy_dir.mkdir()
    fake_bin.mkdir()
    (deploy_dir / "data").mkdir()
    (deploy_dir / "docker-compose.yml").write_text('services:\n  micromech:\n    user: "0:0"\n')
    _write_fake_docker(fake_bin)
    _write_fake_id(fake_bin)

    chown_log = tmp_path / "chown.log"
    fake_chown = fake_bin / "chown"
    fake_chown.write_text(f'#!/bin/sh\necho "$@" >> {chown_log}\n')
    fake_chown.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["SUDO_USER"] = "alice"
    subprocess.run(["bash", str(QUICKSTART_SH)], cwd=parent, env=env, check=True)

    compose = (deploy_dir / "docker-compose.yml").read_text()
    justfile = (deploy_dir / "Justfile").read_text()
    assert 'user: "2001:3001"' in compose
    assert "UPDATER_RUN_AS=2001:3001" in compose
    assert '--user "2001:3001"' in justfile
    assert chown_log.read_text().strip() == f"-R -- 2001:3001 {deploy_dir}"


def test_full_quickstart_normal_install_uses_current_user(tmp_path: Path) -> None:
    parent = tmp_path / "parent"
    deploy_dir = parent / "micromech"
    fake_bin = tmp_path / "bin"
    parent.mkdir()
    fake_bin.mkdir()
    _write_fake_docker(fake_bin)
    _write_fake_id(fake_bin)

    chown_log = tmp_path / "chown.log"
    fake_chown = fake_bin / "chown"
    fake_chown.write_text(f'#!/bin/sh\necho "$@" >> {chown_log}\n')
    fake_chown.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env.pop("SUDO_USER", None)
    subprocess.run(["bash", str(QUICKSTART_SH)], cwd=parent, env=env, check=True)

    compose = (deploy_dir / "docker-compose.yml").read_text()
    justfile = (deploy_dir / "Justfile").read_text()
    assert 'user: "2001:3001"' in compose
    assert "UPDATER_RUN_AS=2001:3001" in compose
    assert '--user "2001:3001"' in justfile
    assert not chown_log.exists()


@pytest.mark.parametrize("artifact", ["Justfile", "docker-compose.yml", "updater.sh"])
def test_update_config_skips_symlink_managed_artifacts(tmp_path: Path, artifact: str) -> None:
    deploy_dir = tmp_path / "deploy"
    fake_bin = tmp_path / "bin"
    target = tmp_path / f"managed-{artifact}"
    deploy_dir.mkdir()
    fake_bin.mkdir()
    (deploy_dir / "data").mkdir()
    target.write_text("managed\n")
    (deploy_dir / artifact).symlink_to(target)

    docker = fake_bin / "docker"
    docker.write_text("#!/bin/sh\necho should-not-run >&2\nexit 1\n")
    docker.chmod(0o755)

    _run_update_config(deploy_dir, fake_bin)

    assert (deploy_dir / artifact).is_symlink()
    assert target.read_text() == "managed\n"


def test_update_config_migrates_managed_compose_secret_mount(tmp_path: Path) -> None:
    deploy_dir = tmp_path / "deploy"
    fake_bin = tmp_path / "bin"
    target = tmp_path / "managed-updater.sh"
    deploy_dir.mkdir()
    fake_bin.mkdir()
    (deploy_dir / "data").mkdir()
    (deploy_dir / "secrets.env").write_text("wallet_password=test\n")
    (deploy_dir / "docker-compose.yml").write_text(
        """name: micromech
services:
  micromech:
    image: dvilela/micromech:latest
    user: "1000:100"
    volumes:
      - ./data:/app/data
    env_file:
      - path: ./secrets.env
        required: false
  updater:
    image: docker:cli
"""
    )
    target.write_text("#!/bin/sh\necho managed\n")
    (deploy_dir / "updater.sh").symlink_to(target)

    docker = fake_bin / "docker"
    docker.write_text("#!/bin/sh\necho should-not-run >&2\nexit 1\n")
    docker.chmod(0o755)

    secrets_stat = (deploy_dir / "secrets.env").stat()
    _run_update_config(deploy_dir, fake_bin)

    compose = (deploy_dir / "docker-compose.yml").read_text()
    assert "- ./data:/app/data\n      - ./secrets.env:/app/secrets.env" in compose
    assert ":/app/secrets.env:ro" not in compose
    assert (deploy_dir / "secrets.env").read_text() == "wallet_password=test\n"
    assert (deploy_dir / "secrets.env").stat().st_ino == secrets_stat.st_ino
    assert (deploy_dir / "secrets.env").stat().st_mtime_ns == secrets_stat.st_mtime_ns
    assert (deploy_dir / "updater.sh").is_symlink()
    assert target.read_text() == "#!/bin/sh\necho managed\n"


def test_update_config_makes_managed_micromech_secret_mount_writable(tmp_path: Path) -> None:
    deploy_dir = tmp_path / "deploy"
    fake_bin = tmp_path / "bin"
    target = tmp_path / "managed-updater.sh"
    deploy_dir.mkdir()
    fake_bin.mkdir()
    (deploy_dir / "data").mkdir()
    (deploy_dir / "secrets.env").write_text("wallet_password=test\n")
    (deploy_dir / "docker-compose.yml").write_text(
        """name: micromech
services:
  micromech:
    image: dvilela/micromech:latest
    volumes:
      - ./data:/app/data
      - ./secrets.env:/app/secrets.env:ro
  updater:
    image: docker:cli
"""
    )
    target.write_text("#!/bin/sh\necho managed\n")
    (deploy_dir / "updater.sh").symlink_to(target)

    docker = fake_bin / "docker"
    docker.write_text("#!/bin/sh\necho should-not-run >&2\nexit 1\n")
    docker.chmod(0o755)

    _run_update_config(deploy_dir, fake_bin)

    compose = (deploy_dir / "docker-compose.yml").read_text()
    assert "- ./secrets.env:/app/secrets.env\n" in compose
    assert ":/app/secrets.env:ro" not in compose


def test_update_config_fails_when_managed_compose_cannot_be_migrated(tmp_path: Path) -> None:
    deploy_dir = tmp_path / "deploy"
    fake_bin = tmp_path / "bin"
    target = tmp_path / "managed-updater.sh"
    deploy_dir.mkdir()
    fake_bin.mkdir()
    (deploy_dir / "docker-compose.yml").write_text(
        "name: micromech\nservices:\n  micromech:\n    image: dvilela/micromech:latest\n"
    )
    target.write_text("#!/bin/sh\necho managed\n")
    (deploy_dir / "updater.sh").symlink_to(target)

    docker = fake_bin / "docker"
    docker.write_text("#!/bin/sh\necho should-not-run >&2\nexit 1\n")
    docker.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["UPDATE_CONFIG"] = "1"
    result = subprocess.run(
        ["bash", str(QUICKSTART_SH)],
        cwd=deploy_dir,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "could not find /app/data volume" in result.stderr


@pytest.mark.parametrize("artifact", ["Justfile", "docker-compose.yml", "updater.sh"])
def test_full_quickstart_does_not_touch_symlink_managed_install(
    tmp_path: Path, artifact: str
) -> None:
    parent = tmp_path / "parent"
    deploy_dir = parent / "micromech"
    fake_bin = tmp_path / "bin"
    target = tmp_path / f"managed-{artifact}"
    parent.mkdir()
    deploy_dir.mkdir()
    fake_bin.mkdir()
    target.write_text("managed\n")
    (deploy_dir / artifact).symlink_to(target)

    docker = fake_bin / "docker"
    docker.write_text(
        """#!/bin/sh
case "$*" in
  "compose version"|"info") exit 0 ;;
  *) echo "unexpected docker invocation: $*" >&2; exit 1 ;;
esac
"""
    )
    docker.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    result = subprocess.run(
        ["bash", str(QUICKSTART_SH)],
        cwd=parent,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert (deploy_dir / artifact).is_symlink()
    assert not (deploy_dir / "data").exists()
    assert target.read_text() == "managed\n"


def test_updater_managed_legacy_update_migrates_secret_mount_and_recreates_updater_only(
    tmp_path: Path,
) -> None:
    """Exercise updater.sh -> extracted quickstart.sh -> legacy sidecar recreate."""
    repo_root = Path(__file__).parent.parent.parent
    deploy_dir = tmp_path / "deploy"
    managed_dir = tmp_path / "managed"
    fake_bin = tmp_path / "bin"
    deploy_dir.mkdir()
    managed_dir.mkdir()
    fake_bin.mkdir()
    (deploy_dir / "data" / "backup" / "pre-update").mkdir(parents=True)
    (deploy_dir / "data" / "config.yaml").write_text("chains: []\n")
    (deploy_dir / "data" / "wallet.json").write_text('{"wallet": true}\n')
    (deploy_dir / "secrets.env").write_text("wallet_password=test\n")
    secrets_stat = (deploy_dir / "secrets.env").stat()
    (managed_dir / "Justfile").write_text("managed just\n")
    (managed_dir / "updater.sh").write_text("#!/bin/sh\necho managed updater\n")
    (managed_dir / "updater.sh").chmod(0o755)
    (deploy_dir / "Justfile").symlink_to(managed_dir / "Justfile")
    (deploy_dir / "updater.sh").symlink_to(managed_dir / "updater.sh")
    (deploy_dir / "docker-compose.yml").write_text(
        """name: micromech
services:
  micromech:
    image: dvilela/micromech:latest
    user: "1000:1000"
    volumes:
      - ./data:/app/data
    env_file:
      - ./secrets.env
  updater:
    image: docker:cli
    volumes:
      - ./:/host
"""
    )
    (deploy_dir / "data" / ".update-request").write_text("update")

    fake_sleep = fake_bin / "sleep"
    fake_sleep.write_text(f"#!/bin/sh\nrm -f {deploy_dir}/data/.update-result\nexit 0\n")
    fake_sleep.chmod(0o755)
    fake_chown = fake_bin / "chown"
    fake_chown.write_text("#!/bin/sh\nexit 0\n")
    fake_chown.chmod(0o755)
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        f"""#!/bin/sh
echo "$@" >> "{tmp_path}/docker.log"
case "$*" in
  *cid-micromech*)
    case "$*" in
      *State.Status*) echo running ;;
      *State.Health*) echo healthy ;;
      *) echo unknown ;;
    esac
    exit 0
    ;;
  *"dvilela/micromech:latest"*)
    case "$*" in
      *Labels*) [ -f "{tmp_path}/pulled" ] && echo 0.0.47 || echo 0.0.46; exit 0 ;;
      *.Id*) [ -f "{tmp_path}/pulled" ] && echo sha256:new || echo sha256:old; exit 0 ;;
    esac
    ;;
esac
case "$*" in
  "compose config --images micromech") echo "dvilela/micromech:latest"; exit 0 ;;
  "compose pull micromech") touch "{tmp_path}/pulled"; exit 0 ;;
  "run --rm --entrypoint cat dvilela/micromech:latest /app/scripts/quickstart.sh")
    cat "{repo_root}/scripts/quickstart.sh"; exit 0 ;;
  "compose -f docker-compose.yml config -q") exit 0 ;;
  "compose -f docker-compose.yml config --services") printf 'micromech\\nupdater\\n'; exit 0 ;;
  "compose stop micromech") exit 0 ;;
  "compose up -d micromech") exit 0 ;;
  "compose ps -q micromech") echo cid-micromech; exit 0 ;;
  *" up -d micromech") exit 0 ;;
  *"config --services") printf 'updater\\n'; exit 0 ;;
  *"up -d --force-recreate updater") exit 0 ;;
  *"up -d --force-recreate dockerproxy updater") exit 1 ;;
esac
if [ "$1" = "tag" ] || [ "$1 $2" = "image inspect" ]; then exit 0; fi
echo "unexpected docker invocation: $*" >&2
exit 1
"""
    )
    fake_docker.chmod(0o755)

    updater = tmp_path / "updater.sh"
    updater.write_text(
        (repo_root / "scripts" / "updater.sh")
        .read_text()
        .replace('UPDATER_LOG="/host/data/updater.log"', f'UPDATER_LOG="{tmp_path}/updater.log"')
        .replace("cd /host", f'cd "{deploy_dir}"')
    )
    updater.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["HOST_PROJECT_DIR"] = str(deploy_dir)
    result = subprocess.run(["bash", str(updater)], env=env, capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    compose = (deploy_dir / "docker-compose.yml").read_text()
    assert "- ./secrets.env:/app/secrets.env" in compose
    assert ":/app/secrets.env:ro" not in compose
    assert (deploy_dir / "secrets.env").stat().st_ino == secrets_stat.st_ino
    assert (deploy_dir / "secrets.env").stat().st_mtime_ns == secrets_stat.st_mtime_ns
    assert (deploy_dir / "Justfile").is_symlink()
    assert (deploy_dir / "updater.sh").is_symlink()
    assert (managed_dir / "Justfile").read_text() == "managed just\n"
    assert (managed_dir / "updater.sh").read_text() == "#!/bin/sh\necho managed updater\n"
    docker_log = (tmp_path / "docker.log").read_text()
    assert "up -d --force-recreate updater" in docker_log
    assert "up -d --force-recreate dockerproxy updater" not in docker_log


class TestRepairUpdaterScript:
    """Regression tests for the public one-command updater repair path."""

    def _write_fake_docker(self, fake_bin: Path, image_compose: Path) -> Path:
        log_file = fake_bin.parent / "docker.log"
        fake_docker = fake_bin / "docker"
        fake_docker.write_text(
            f"""#!/usr/bin/env bash
set -e
printf '%s\\n' "$*" >> "{log_file}"
if [ "$1" = "compose" ]; then
    shift
    if [ "${{1:-}}" = "version" ]; then exit 0; fi
    file_config=0
    if [ "${{1:-}}" = "-f" ]; then file_config=1; shift 2; fi
    case "${{1:-}}" in
        config)
            case "${{2:-}}" in
                --images)
                    if [ "${{FAKE_CONFIG_IMAGES_FAIL_UNTIL_GENERATED:-0}}" = "1" ] && ! grep -q '^name: micromech' docker-compose.yml 2>/dev/null; then
                        exit 1
                    fi
                    echo "${{FAKE_IMAGE:-${{FAKE_GENERATED_IMAGE:-dvilela/micromech:latest}}}}"
                    exit 0
                    ;;
                --services)
                    if [ "$file_config" = "1" ] || grep -Eq '^[[:space:]]+micromech:' docker-compose.yml 2>/dev/null; then
                        printf 'micromech\\ndockerproxy\\nupdater\\n'
                    else
                        printf 'other\\n'
                    fi
                    exit 0
                    ;;
                -q) exit 0 ;;
                *) cat docker-compose.yml 2>/dev/null || true; exit 0 ;;
            esac
            ;;
        pull|up|ps|exec)
            if [ "${{1:-}}" = "ps" ]; then echo updater-container; fi
            exit 0
            ;;
    esac
fi
if [ "$1" = "info" ]; then exit 0; fi
if [ "$1" = "pull" ]; then exit 0; fi
if [ "$1" = "run" ]; then
    last="${{@: -1}}"
    case "$last" in
        /app/scripts/quickstart.sh) cat "{QUICKSTART_SH}"; exit 0 ;;
        /app/scripts/updater.sh) cat "{UPDATER_SH}"; exit 0 ;;
        /app/docker-compose.yml) cat "{image_compose}"; exit 0 ;;
    esac
fi
echo "unexpected docker invocation: $*" >&2
exit 1
"""
        )
        fake_docker.chmod(0o755)
        return log_file

    def test_repair_updater_repairs_testing_install_without_just(self, tmp_path: Path) -> None:
        deploy_dir = tmp_path / "micromech"
        deploy_dir.mkdir()
        (deploy_dir / "data").mkdir()
        (deploy_dir / "secrets.env").write_text("wallet_password=test\n")
        (deploy_dir / "docker-compose.yml").write_text(
            """\
services:
  micromech:
    image: dvilela/micromech-testing:latest
    user: "1000:1000"
    volumes:
      - ./data:/app/data
      - ./secrets.env:/app/secrets.env
"""
        )
        (deploy_dir / "Justfile").write_text("update:\n\t@echo broken\n")
        (deploy_dir / "updater.sh").write_text("#!/bin/sh\necho old\n")

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        image_compose = tmp_path / "image-compose.yml"
        image_compose.write_text(IMAGE_COMPOSE_TEMPLATE)
        log_file = self._write_fake_docker(fake_bin, image_compose)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["FAKE_IMAGE"] = "dvilela/micromech-testing:latest"

        result = subprocess.run(
            ["bash", str(REPAIR_UPDATER_SH)],
            cwd=deploy_dir,
            env=env,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr + result.stdout
        compose = (deploy_dir / "docker-compose.yml").read_text()
        assert "image: dvilela/micromech-testing:latest" in compose
        assert "UPDATER_RUN_AS=1000:1000" in compose
        assert "- ./:/host" in compose
        assert "su-exec" in compose
        assert "org.dvilela.micromech.version" in (deploy_dir / "Justfile").read_text()
        assert "PROJECT_DIR=" in (deploy_dir / "updater.sh").read_text()

        docker_log = log_file.read_text()
        assert "pull dvilela/micromech-testing:latest" in docker_log
        assert "compose pull micromech" in docker_log
        assert "compose up -d --force-recreate dockerproxy updater" in docker_log
        assert "just" not in docker_log

    def test_repair_updater_can_be_run_from_parent_directory(self, tmp_path: Path) -> None:
        deploy_dir = tmp_path / "micromech"
        deploy_dir.mkdir()
        (deploy_dir / "data").mkdir()
        (deploy_dir / "secrets.env").write_text("wallet_password=test\n")
        (deploy_dir / "docker-compose.yml").write_text(
            "services:\n  micromech:\n    image: dvilela/micromech:latest\n    user: \"1000:1000\"\n"
        )
        (deploy_dir / "Justfile").write_text("update:\n\t@echo broken\n")
        (deploy_dir / "updater.sh").write_text("#!/bin/sh\necho old\n")

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        image_compose = tmp_path / "image-compose.yml"
        image_compose.write_text(IMAGE_COMPOSE_TEMPLATE)
        self._write_fake_docker(fake_bin, image_compose)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"

        result = subprocess.run(
            ["bash", str(REPAIR_UPDATER_SH)],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr + result.stdout
        assert "Micromech updater repair completed" in result.stdout
        assert (deploy_dir / "docker-compose.yml").read_text().startswith("name: micromech")

    def test_repair_updater_preserves_testing_from_config_when_compose_detection_fails(self, tmp_path: Path) -> None:
        deploy_dir = tmp_path / "micromech"
        deploy_dir.mkdir()
        (deploy_dir / "data").mkdir()
        (deploy_dir / "data" / "config.yaml").write_text("update_channel: testing # keep testing\n")
        (deploy_dir / "secrets.env").write_text("wallet_password=test\n")
        (deploy_dir / "docker-compose.yml").write_text(
            "services:\n  micromech:\n    image: invalid\n    user: \"1000:1000\"\n"
        )
        (deploy_dir / "Justfile").write_text("update:\n\t@echo broken\n")
        (deploy_dir / "updater.sh").write_text("#!/bin/sh\necho old\n")

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        image_compose = tmp_path / "image-compose.yml"
        image_compose.write_text(IMAGE_COMPOSE_TEMPLATE)
        self._write_fake_docker(fake_bin, image_compose)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["FAKE_CONFIG_IMAGES_FAIL_UNTIL_GENERATED"] = "1"
        env["FAKE_GENERATED_IMAGE"] = "dvilela/micromech-testing:latest"

        result = subprocess.run(
            ["bash", str(REPAIR_UPDATER_SH)],
            cwd=deploy_dir,
            env=env,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr + result.stdout
        assert "image: dvilela/micromech-testing:latest" in (deploy_dir / "docker-compose.yml").read_text()

    def test_repair_updater_detects_quoted_image_when_compose_detection_fails(self, tmp_path: Path) -> None:
        deploy_dir = tmp_path / "micromech"
        deploy_dir.mkdir()
        (deploy_dir / "data").mkdir()
        (deploy_dir / "secrets.env").write_text("wallet_password=test\n")
        (deploy_dir / "docker-compose.yml").write_text(
            "services:\n  micromech:\n    image: \"dvilela/micromech-testing:latest\"\n    user: \"1000:1000\"\n"
        )
        (deploy_dir / "Justfile").write_text("update:\n\t@echo broken\n")
        (deploy_dir / "updater.sh").write_text("#!/bin/sh\necho old\n")

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        image_compose = tmp_path / "image-compose.yml"
        image_compose.write_text(IMAGE_COMPOSE_TEMPLATE)
        self._write_fake_docker(fake_bin, image_compose)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["FAKE_CONFIG_IMAGES_FAIL_UNTIL_GENERATED"] = "1"
        env["FAKE_GENERATED_IMAGE"] = "dvilela/micromech-testing:latest"

        result = subprocess.run(
            ["bash", str(REPAIR_UPDATER_SH)],
            cwd=deploy_dir,
            env=env,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr + result.stdout
        assert "image: dvilela/micromech-testing:latest" in (deploy_dir / "docker-compose.yml").read_text()

    def test_repair_updater_refuses_managed_symlink_install(self, tmp_path: Path) -> None:
        deploy_dir = tmp_path / "micromech"
        managed_dir = tmp_path / "managed"
        deploy_dir.mkdir()
        managed_dir.mkdir()
        (deploy_dir / "data").mkdir()
        (deploy_dir / "secrets.env").write_text("wallet_password=test\n")
        (deploy_dir / "docker-compose.yml").write_text(
            "services:\n  micromech:\n    image: dvilela/micromech:latest\n    user: \"1000:1000\"\n"
        )
        (deploy_dir / "Justfile").write_text("update:\n\t@echo broken\n")
        (managed_dir / "updater.sh").write_text("#!/bin/sh\necho old but valid\n")
        (deploy_dir / "updater.sh").symlink_to(managed_dir / "updater.sh")

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        image_compose = tmp_path / "image-compose.yml"
        image_compose.write_text(IMAGE_COMPOSE_TEMPLATE)
        self._write_fake_docker(fake_bin, image_compose)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"

        result = subprocess.run(
            ["bash", str(REPAIR_UPDATER_SH)],
            cwd=deploy_dir,
            env=env,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "is a symlink" in result.stderr

    def test_repair_updater_uses_subdir_when_parent_has_unrelated_compose(self, tmp_path: Path) -> None:
        (tmp_path / "docker-compose.yml").write_text("services:\n  other:\n    image: alpine\n")
        deploy_dir = tmp_path / "micromech"
        deploy_dir.mkdir()
        (deploy_dir / "data").mkdir()
        (deploy_dir / "secrets.env").write_text("wallet_password=test\n")
        (deploy_dir / "docker-compose.yml").write_text(
            "services:\n  micromech:\n    image: dvilela/micromech:latest\n    user: \"1000:1000\"\n"
        )
        (deploy_dir / "Justfile").write_text("update:\n\t@echo broken\n")
        (deploy_dir / "updater.sh").write_text("#!/bin/sh\necho old\n")

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        image_compose = tmp_path / "image-compose.yml"
        image_compose.write_text(IMAGE_COMPOSE_TEMPLATE)
        self._write_fake_docker(fake_bin, image_compose)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"

        result = subprocess.run(
            ["bash", str(REPAIR_UPDATER_SH)],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr + result.stdout
        assert (tmp_path / "docker-compose.yml").read_text() == "services:\n  other:\n    image: alpine\n"
        assert (deploy_dir / "docker-compose.yml").read_text().startswith("name: micromech")

    def test_full_quickstart_rejects_invalid_image_override_before_pull(self, tmp_path: Path) -> None:
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        image_compose = tmp_path / "image-compose.yml"
        image_compose.write_text(IMAGE_COMPOSE_TEMPLATE)
        log_file = self._write_fake_docker(fake_bin, image_compose)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["MICROMECH_IMAGE"] = "attacker/example:latest"

        result = subprocess.run(
            ["bash", str(QUICKSTART_SH)],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "unsupported MICROMECH_IMAGE" in result.stderr
        assert "pull attacker/example:latest" not in log_file.read_text()
