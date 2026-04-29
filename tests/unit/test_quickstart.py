"""Tests for quickstart ownership and managed-install guards."""

import os
import subprocess
from pathlib import Path

import pytest

QUICKSTART_SH = Path(__file__).parent.parent.parent / "scripts" / "quickstart.sh"


def _write_fake_docker(fake_bin: Path) -> None:
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
  "compose version")
    exit 0
    ;;
  "info")
    exit 0
    ;;
  "pull dvilela/micromech:latest")
    exit 0
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
    fake_chown.write_text(f"#!/bin/sh\necho \"$@\" >> {chown_log}\n")
    fake_chown.chmod(0o755)

    _run_update_config(deploy_dir, fake_bin, sudo_user=sudo_user)

    compose = (deploy_dir / "docker-compose.yml").read_text()
    justfile = (deploy_dir / "Justfile").read_text()
    assert 'user: "2001:3001"' in compose
    assert "UPDATER_RUN_AS=2001:3001" in compose
    assert '--user "2001:3001"' in justfile
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
    (deploy_dir / "docker-compose.yml").write_text(
        "services:\n  micromech:\n    user: 4444:5555\n"
    )

    _run_update_config(deploy_dir, fake_bin)

    compose = (deploy_dir / "docker-compose.yml").read_text()
    justfile = (deploy_dir / "Justfile").read_text()
    assert 'user: "4444:5555"' in compose
    assert "UPDATER_RUN_AS=4444:5555" in compose
    assert '--user "4444:5555"' in justfile


def test_update_config_rejects_invalid_existing_compose_user(tmp_path: Path) -> None:
    deploy_dir, fake_bin = _make_deploy_dir(tmp_path)
    (deploy_dir / "docker-compose.yml").write_text(
        "services:\n  micromech:\n    user: --bad\n"
    )

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
    fake_chown.write_text(f"#!/bin/sh\necho \"$@\" >> {chown_log}\n")
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
    fake_chown.write_text(f"#!/bin/sh\necho \"$@\" >> {chown_log}\n")
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
def test_update_config_skips_symlink_managed_artifacts(
    tmp_path: Path, artifact: str
) -> None:
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
