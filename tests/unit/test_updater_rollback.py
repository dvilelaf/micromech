"""Contract and behavioral smoke tests for the Micromech updater sidecar."""

import os
import subprocess
import textwrap
from pathlib import Path

UPDATER = Path("scripts/updater.sh")
QUICKSTART = Path("scripts/quickstart.sh")


def _updater() -> str:
    return UPDATER.read_text()


def _quickstart() -> str:
    return QUICKSTART.read_text()


def test_normal_update_does_not_execute_new_image_control_plane() -> None:
    src = _updater()
    assert "/app/scripts/quickstart.sh" not in src
    assert "UPDATE_CONFIG=1" not in src
    assert "docker run --rm --entrypoint cat" not in src


def test_normal_update_does_not_mutate_host_artifacts_or_data() -> None:
    src = _updater()
    forbidden = [
        "restore_host_artifacts",
        "chown -R",
        "data/backup/pre-update",
        "docker-compose.yml.bak",
        "cp -p data/config.yaml",
        "cp -p data/wallet.json",
        'mv "$cfg_tmp" data/config.yaml',
        'mv "$wal_tmp" data/wallet.json',
        "exec bash ./updater.sh",
        "--force-recreate dockerproxy updater",
    ]
    for token in forbidden:
        assert token not in src


def test_update_has_strict_healthcheck_and_no_health_none_success() -> None:
    src = _updater()
    assert '[ "$health" = "healthy" ]' in src
    assert '[ "$health" = "none" ]' in src
    assert '{ [ "$health" = "healthy" ] || [ "$health" = "none" ]; }' not in src


def test_update_fingerprints_protected_artifacts_before_success_or_rollback() -> None:
    src = _updater()
    for artifact in (
        "secrets.env",
        "docker-compose.yml",
        "Justfile",
        "updater.sh",
        "data/config.yaml",
        "data/wallet.json",
    ):
        assert artifact in src
    assert "protected_fingerprint" in src
    assert "verify_fingerprint" in src
    assert "sha256sum" in src
    assert "readlink -f" in src
    assert "symlink-target" in src
    assert src.index("protected_fingerprint") < src.index("compose_up_service")
    assert 'write_result "updated:$old:$new"' in src
    assert src.index('verify_fingerprint "$fingerprints"') < src.index(
        'write_result "updated:$old:$new"'
    )


def test_update_rollback_is_final_state_not_error_alias() -> None:
    src = _updater()
    assert 'write_result "rolled_back:$old:$new"' in src
    assert "rolled_back_to_v" not in src


def test_generated_just_update_uses_sidecar_contract() -> None:
    src = _quickstart()
    update_block = src.split("\nupdate:\n", 1)[1].split("\nupdate-config:\n", 1)[0]
    assert "printf 'update\\n' > data/.update-request" in update_block
    assert "data/.update-result" in update_block
    assert "docker compose pull" not in update_block
    assert "/app/scripts/quickstart.sh" not in update_block
    assert "UPDATE_CONFIG=1" not in update_block


def _write_deployment(tmp_path: Path) -> None:
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "config.yaml").write_text("update_channel: release\n")
    (tmp_path / "data" / "wallet.json").write_text('{"wallet": "fake"}\n')
    (tmp_path / "secrets.env").write_text("telegram_token=fake\n")
    (tmp_path / "Justfile").write_text("update:\n\t@echo fake\n")
    (tmp_path / "updater.sh").write_text("#!/usr/bin/env bash\n")
    (tmp_path / "docker-compose.yml").write_text(
        textwrap.dedent(
            """\
            services:
              micromech:
                image: dvilela/micromech:latest
                volumes:
                  - ./data:/app/data
                  - ./secrets.env:/app/secrets.env
            """
        )
    )


def _write_fake_docker(
    tmp_path: Path,
    *,
    health: str = "healthy",
    same_image: bool = False,
    mutate_path: Path | None = None,
    rollback_recovers: bool = False,
) -> None:
    mutation = f'printf "mutated\\n" > "{mutate_path}"' if mutate_path else ":"
    health_probe = (
        f'if [ -f "{tmp_path / "rollback-restored"}" ]; then echo "healthy"; else echo "unhealthy"; fi'
        if rollback_recovers
        else f'echo "{health}"'
    )
    docker = tmp_path / "docker"
    docker.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env bash
            set -eu
            echo "$@" >> "{tmp_path / "docker.log"}"
            if [ "$1 $2 $3" = "compose config --images" ]; then echo "dvilela/micromech:latest"; exit 0; fi
            if [ "$1 $2 $3" = "compose ps -q" ]; then echo "micromech-cid"; exit 0; fi
            if [ "$1" = "inspect" ]; then
              if [[ "${{2:-}}" == --format=* ]]; then fmt="${{2#--format=}}"; target="${{3:-}}"; else fmt="$3"; target="${{4:-}}"; fi
              case "$fmt|$target" in
                *".Image"*\\|micromech-cid) echo "sha256:old"; exit 0 ;;
                *".Id"*\\|sha256:old) echo "sha256:old"; exit 0 ;;
                *".Id"*\\|dvilela/micromech:latest) echo "{"sha256:old" if same_image else "sha256:new"}"; exit 0 ;;
                *"org.dvilela.micromech.version"*\\|sha256:old) echo "0.1.0"; exit 0 ;;
                *"org.dvilela.micromech.version"*\\|dvilela/micromech:latest) echo "{"0.1.0" if same_image else "0.1.1"}"; exit 0 ;;
                *".State.Status"*\\|micromech-cid) echo "running"; exit 0 ;;
                *".State.Health"*\\|micromech-cid) {health_probe}; exit 0 ;;
              esac
            fi
            if [ "$1 $2" = "image inspect" ]; then exit 0; fi
            if [ "$1" = "tag" ]; then
              if [ "${{2:-}}" = "dvilela/micromech:rollback-prev" ] && [ "${{3:-}}" = "dvilela/micromech:latest" ]; then
                touch "{tmp_path / "rollback-restored"}"
              fi
              exit 0
            fi
            if [ "$1 $2 $3" = "compose pull micromech" ]; then exit 0; fi
            if [ "$1 $2" = "compose -f" ]; then {mutation}; exit 0; fi
            if [ "$1 $2 $3" = "compose stop micromech" ]; then exit 0; fi
            if [ "$1 $2 $3" = "compose restart micromech" ]; then exit 0; fi
            exit 0
            """
        )
    )
    docker.chmod(0o755)


def _run_once(tmp_path: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{tmp_path}:{env['PATH']}",
            "UPDATER_HOST_ROOT": str(tmp_path),
            "HOST_PROJECT_DIR": str(tmp_path),
            "UPDATER_ONCE": "1",
            "UPDATER_POLL_SECONDS": "0",
            "UPDATER_HEALTH_TIMEOUT_SECONDS": "2",
        }
    )
    return subprocess.run(["bash", str(UPDATER)], env=env, capture_output=True, text=True)


def test_successful_update_reports_updated_and_preserves_artifacts(tmp_path: Path) -> None:
    _write_deployment(tmp_path)
    _write_fake_docker(tmp_path)
    before = {
        p: (tmp_path / p).read_bytes()
        for p in (
            "secrets.env",
            "docker-compose.yml",
            "Justfile",
            "updater.sh",
            "data/config.yaml",
            "data/wallet.json",
        )
    }
    (tmp_path / "data" / ".update-request").write_text("update\n")

    result = _run_once(tmp_path)

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "data" / ".update-result").read_text().strip() == "updated:0.1.0:0.1.1"
    for path, content in before.items():
        assert (tmp_path / path).read_bytes() == content
    docker_log = (tmp_path / "docker.log").read_text()
    assert "/app/scripts/quickstart.sh" not in docker_log
    assert "compose pull micromech" in docker_log
    assert "up -d --no-deps micromech" in docker_log


def test_health_none_does_not_report_success(tmp_path: Path) -> None:
    _write_deployment(tmp_path)
    _write_fake_docker(tmp_path, health="none")
    (tmp_path / "data" / ".update-request").write_text("update\n")

    result = _run_once(tmp_path)

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "data" / ".update-result").read_text().strip() == "error:rollback_failed"


def test_unhealthy_new_image_rolls_back_to_healthy_previous_image(tmp_path: Path) -> None:
    _write_deployment(tmp_path)
    _write_fake_docker(tmp_path, rollback_recovers=True)
    before = {
        p: (tmp_path / p).read_bytes()
        for p in (
            "secrets.env",
            "docker-compose.yml",
            "Justfile",
            "updater.sh",
            "data/config.yaml",
            "data/wallet.json",
        )
    }
    (tmp_path / "data" / ".update-request").write_text("update\n")

    result = _run_once(tmp_path)

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "data" / ".update-result").read_text().strip() == "rolled_back:0.1.0:0.1.1"
    for path, content in before.items():
        assert (tmp_path / path).read_bytes() == content
    docker_log = (tmp_path / "docker.log").read_text()
    assert "tag sha256:old dvilela/micromech:rollback-prev" in docker_log
    assert "compose stop micromech" in docker_log
    assert "tag dvilela/micromech:rollback-prev dvilela/micromech:latest" in docker_log
    assert docker_log.count("up -d --no-deps micromech") == 2


def test_current_image_noop_does_not_recreate_service(tmp_path: Path) -> None:
    _write_deployment(tmp_path)
    _write_fake_docker(tmp_path, same_image=True)
    (tmp_path / "data" / ".update-request").write_text("update\n")

    result = _run_once(tmp_path)

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "data" / ".update-result").read_text().strip() == "current:0.1.0"
    docker_log = (tmp_path / "docker.log").read_text()
    assert "up -d --no-deps micromech" not in docker_log


def test_symlink_target_mutation_is_detected(tmp_path: Path) -> None:
    _write_deployment(tmp_path)
    managed = tmp_path / "managed-updater.sh"
    managed.write_text("#!/usr/bin/env bash\n")
    (tmp_path / "updater.sh").unlink()
    (tmp_path / "updater.sh").symlink_to(managed)
    _write_fake_docker(tmp_path, mutate_path=managed)
    (tmp_path / "data" / ".update-request").write_text("update\n")

    result = _run_once(tmp_path)

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "data" / ".update-result").read_text().strip() == "error:artifact_mutation"


def test_stale_update_state_is_terminal(tmp_path: Path) -> None:
    _write_deployment(tmp_path)
    _write_fake_docker(tmp_path)
    (tmp_path / "data" / ".update-state").write_text("update\n")
    (tmp_path / "data" / ".update-request").write_text("update\n")

    result = _run_once(tmp_path)

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "data" / ".update-result").read_text().strip() == "error:interrupted_update"
    assert not (tmp_path / "data" / ".update-state").exists()
    assert not (tmp_path / "data" / ".update-request").exists()
