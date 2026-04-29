"""Behavioral tests for micromech/scripts/updater.sh rollback path.

Mirrors triton's tests/test_updater_rollback.py but for micromech.
Executes the rollback bash block in a sandbox with a mocked `docker` binary.
"""

import os
import subprocess
import textwrap
from pathlib import Path

import pytest

UPDATER_SH = Path(__file__).parent.parent.parent / "scripts" / "updater.sh"
QUICKSTART_SH = Path(__file__).parent.parent.parent / "scripts" / "quickstart.sh"


def _docker_mock(tmp_bin: Path, scenario: str) -> Path:
    """Mock docker binary: 'rollback_prev_exists' or 'rollback_prev_missing'."""
    inspect_exit = 0 if scenario == "rollback_prev_exists" else 1
    docker_path = tmp_bin / "docker"
    docker_path.write_text(textwrap.dedent(f"""\
        #!/bin/bash
        echo "$@" >> "${{TMP_DOCKER_LOG:-/tmp/docker-calls.log}}"
        case "$1 $2" in
            "image inspect"*)  exit {inspect_exit} ;;
            "tag "*)           exit 0 ;;
            "compose stop"|"compose up"*) exit 0 ;;
            "inspect "*)       echo "running"; exit 0 ;;
            *)                 exit 0 ;;
        esac
    """))
    docker_path.chmod(0o755)
    return docker_path


def _extract_rollback_block(script: Path) -> str:
    """Extract the rollback `if ! wait_healthy; then ... else ... fi` block.

    micromech only has ONE wait_healthy invocation (no restart action with
    health check), so we anchor on the unique 'rolling back data and image'
    log line.
    """
    src = script.read_text()
    # Anchor on the unique log line inside the rollback block
    anchor = "Health check failed — rolling back data and image"
    anchor_idx = src.find(anchor)
    assert anchor_idx != -1, "rollback log anchor not found — script may have been edited"
    start = src.rfind("if ! wait_healthy; then", 0, anchor_idx)
    assert start != -1, "rollback block start not found"
    # End just before the success branch cleanup.
    end = src.find("else\n                        rm -f docker-compose.yml.bak", start)
    assert end != -1, "rollback block end (else success) not found"
    return src[start:end] + "\nfi\n"


def _write_runner(tmp_path: Path, rollback_block: str, vars_setup: str) -> Path:
    runner = tmp_path / "run_rollback.sh"
    header = textwrap.dedent(f"""\
        #!/bin/bash
        set -u
        export PATH="{tmp_path / "bin"}:$PATH"
        export TMP_DOCKER_LOG="{tmp_path}/docker-calls.log"
        : > "$TMP_DOCKER_LOG"
        cd "{tmp_path}"
        sleep() {{ :; }}
        log() {{ echo "[updater] $1" >&2; }}
        data_safe() {{ [ -d data ] && [ ! -L data ]; }}
        backup_safe() {{
            data_safe \\
                && [ ! -L data/backup ] \\
                && [ ! -L data/backup/pre-update ] \\
                && [ -d data/backup/pre-update ]
        }}
        _write_result() {{
            data_safe || return 0
            printf '%s\\n' "$1" > data/.update-result.tmp 2>/dev/null \\
                && mv data/.update-result.tmp data/.update-result 2>/dev/null || true
        }}
        restore_host_artifacts() {{ return 0; }}
        rollback_image() {{
            if docker image inspect "${{MICROMECH_IMAGE%:latest}}:rollback-prev" >/dev/null 2>&1; then
                docker tag "${{MICROMECH_IMAGE%:latest}}:rollback-prev" "$MICROMECH_IMAGE" \\
                    || true
            fi
            return 0
        }}
        wait_healthy() {{ return 1; }}
        MICROMECH_IMAGE="dvilela/micromech:latest"
        RUN_AS="1000:1000"
        host_backup_dir=""
        {vars_setup}
        # === injected rollback block follows ===
        for _ in 1; do
    """)
    footer = textwrap.dedent("""\

        done
        if [ -f data/.update-result ]; then
            echo "MARKER:$(cat data/.update-result)"
        fi
    """)
    runner.write_text(header + rollback_block + footer)
    runner.chmod(0o755)
    return runner


@pytest.fixture
def sandbox(tmp_path):
    (tmp_path / "bin").mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "backup" / "pre-update").mkdir(parents=True)
    return tmp_path


class TestRollbackBehavior:

    def test_marker_uses_error_rolled_back_to_v_prefix(self, sandbox):
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="20260424T120000Z"; '
            '_pre_dir="data/backup/pre-update"'
        )
        result = subprocess.run([str(runner)], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert "MARKER:error:rolled_back_to_v0.5.1" in result.stdout

    def test_marker_falls_back_to_unknown_when_old_empty(self, sandbox):
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            'OLD=""; NEW="0.5.2"; _pre_ts="20260424T120000Z"; '
            '_pre_dir="data/backup/pre-update"'
        )
        result = subprocess.run([str(runner)], capture_output=True, text=True)
        assert "MARKER:error:rolled_back_to_vunknown" in result.stdout

    def test_image_inspect_called_before_retag(self, sandbox):
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="20260424T120000Z"; '
            '_pre_dir="data/backup/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        log = (sandbox / "docker-calls.log").read_text()
        idx_inspect = log.find("image inspect")
        idx_tag = log.find("tag dvilela/micromech:rollback-prev")
        assert idx_inspect != -1, log
        assert idx_tag != -1, log
        assert idx_inspect < idx_tag

    def test_skips_retag_if_rollback_prev_missing(self, sandbox):
        _docker_mock(sandbox / "bin", "rollback_prev_missing")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="20260424T120000Z"; '
            '_pre_dir="data/backup/pre-update"'
        )
        result = subprocess.run([str(runner)], capture_output=True, text=True)
        log = (sandbox / "docker-calls.log").read_text()
        assert "tag dvilela/micromech:rollback-prev" not in log
        assert "MARKER:error:rolled_back_to_v0.5.1" in result.stdout

    def test_restores_config_from_snapshot(self, sandbox):
        ts = "20260424T120000Z"
        snap = sandbox / "data" / "backup" / "pre-update" / f"config.yaml.{ts}.bak"
        snap.write_text("snapshot-cfg\n")
        (sandbox / "data" / "config.yaml").write_text("broken-new\n")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{ts}"; '
            f'_pre_dir="data/backup/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        assert (sandbox / "data" / "config.yaml").read_text() == "snapshot-cfg\n"

    def test_rejects_symlinked_snapshot_source(self, sandbox):
        ts = "20260424T120000Z"
        outside = sandbox / "outside-config"
        outside.write_text("outside-content\n")
        snap = sandbox / "data" / "backup" / "pre-update" / f"config.yaml.{ts}.bak"
        snap.symlink_to(outside)
        (sandbox / "data" / "config.yaml").write_text("broken-new\n")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{ts}"; '
            f'_pre_dir="data/backup/pre-update"'
        )
        result = subprocess.run([str(runner)], capture_output=True, text=True)

        assert result.returncode == 0, result.stderr
        assert (sandbox / "data" / "config.yaml").read_text() == "broken-new\n"
        assert "MARKER:error:unsafe_backup_dir" in result.stdout

    def test_restores_wallet_from_snapshot(self, sandbox):
        ts = "20260424T120000Z"
        snap = sandbox / "data" / "backup" / "pre-update" / f"wallet.json.{ts}.bak"
        snap.write_text('{"snapshot": true}\n')
        (sandbox / "data" / "wallet.json").write_text('{"broken": true}\n')
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{ts}"; '
            f'_pre_dir="data/backup/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        assert (sandbox / "data" / "wallet.json").read_text() == '{"snapshot": true}\n'

    def test_no_temp_files_left(self, sandbox):
        ts = "20260424T120000Z"
        for name in ("config.yaml", "wallet.json"):
            (sandbox / "data" / "backup" / "pre-update" / f"{name}.{ts}.bak").write_text("x")
            (sandbox / "data" / name).write_text("orig")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{ts}"; '
            f'_pre_dir="data/backup/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        assert list((sandbox / "data").glob("*.tmp")) == []

    def test_skips_restore_when_no_snapshot(self, sandbox):
        (sandbox / "data" / "config.yaml").write_text("new-broken\n")
        (sandbox / "data" / "wallet.json").write_text("new-broken\n")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="20260424T120000Z"; '
            '_pre_dir="data/backup/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        assert (sandbox / "data" / "config.yaml").read_text() == "new-broken\n"
        assert (sandbox / "data" / "wallet.json").read_text() == "new-broken\n"

    def test_glob_strict_rejects_planted_filename(self, sandbox):
        (sandbox / "data" / "backup" / "pre-update" / "config.yaml.malicious.bak").write_text(
            "PLANTED\n"
        )
        (sandbox / "data" / "config.yaml").write_text("real\n")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="20260424T120000Z"; '
            '_pre_dir="data/backup/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        assert (sandbox / "data" / "config.yaml").read_text() != "PLANTED\n"

    def test_compose_stop_called_before_up(self, sandbox):
        ts = "20260424T120000Z"
        (sandbox / "data" / "backup" / "pre-update" / f"config.yaml.{ts}.bak").write_text("x")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{ts}"; '
            f'_pre_dir="data/backup/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        log = (sandbox / "docker-calls.log").read_text()
        idx_stop = log.find("compose stop")
        idx_up = log.find("compose up")
        assert idx_stop != -1
        assert idx_up != -1
        assert idx_stop < idx_up


class TestUpdaterStructuralGuards:

    def test_uses_cp_dash_p_for_snapshots(self):
        src = UPDATER_SH.read_text()
        assert "cp -p data/config.yaml" in src
        assert "cp -p data/wallet.json" in src
        assert 'cp -p "$snap_cfg"' in src
        assert 'cp -p "$snap_wal"' in src

    def test_security_comment_present(self):
        src = UPDATER_SH.read_text()
        assert "SECURITY: never log" in src

    def test_retag_uses_image_inspect_guard(self):
        src = UPDATER_SH.read_text()
        assert 'docker image inspect "${MICROMECH_IMAGE%:latest}:rollback-prev"' in src

    def test_glob_uses_iso_timestamp_pattern(self):
        src = UPDATER_SH.read_text()
        assert "config.yaml.[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.bak" in src
        assert "wallet.json.[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.bak" in src

    def test_atomic_restore_uses_tmp_plus_mv(self):
        src = UPDATER_SH.read_text()
        assert "mktemp data/config.yaml.XXXXXX" in src
        assert "mktemp data/wallet.json.XXXXXX" in src
        assert 'mv "$cfg_tmp" data/config.yaml' in src
        assert 'mv "$wal_tmp" data/wallet.json' in src

    def test_old_version_fallback_guard(self):
        src = UPDATER_SH.read_text()
        assert '[ -n "$OLD" ] || OLD="unknown"' in src

    def test_pre_pull_retag_with_guard(self):
        src = UPDATER_SH.read_text()
        assert 'docker tag "$MICROMECH_IMAGE" "${MICROMECH_IMAGE%:latest}:rollback-prev"' in src

    def test_wait_healthy_function_defined(self):
        """micromech did NOT have wait_healthy before; must be defined now."""
        src = UPDATER_SH.read_text()
        assert "wait_healthy()" in src
        assert "docker compose ps -q micromech" in src
        assert "docker inspect --format='{{.State.Status}}' \"$container_id\"" in src

    def test_tmp_compose_files_use_private_mktemp_names(self):
        src = UPDATER_SH.read_text()
        assert "/tmp/docker-compose-abs.yml" not in src
        assert "/tmp/docker-compose-updater.yml" not in src
        assert "mktemp /tmp/micromech-compose-abs-XXXXXX" in src
        assert "mktemp /tmp/micromech-compose-updater-XXXXXX" in src

    def test_h1_atomic_write_helper_used(self):
        """All .update-result writes must go through _write_result (atomic tmp+mv)."""
        src = UPDATER_SH.read_text()
        assert "_write_result()" in src
        assert 'write_data_file ".update-result"' in src
        assert 'mktemp "data/.$rel.XXXXXX"' in src
        for line in src.split("\n"):
            if "> data/.update-result" in line and ".tmp" not in line:
                raise AssertionError(f"bare redirect to .update-result found: {line!r}")

    def test_h2_backup_dir_chmod_700(self):
        """Backup dir must be locked to 700 even if created with a loose umask."""
        src = UPDATER_SH.read_text()
        assert 'chmod 700 "$pre_dir"' in src

    def test_backup_symlink_refusal_precedes_snapshot_creation(self):
        src = UPDATER_SH.read_text()
        guard = src.index('if ! data_safe || [ -L data/backup ] || [ -L "$pre_dir" ]; then')
        mkdir = src.index('mkdir -p "$pre_dir"')
        assert guard < mkdir
        assert '_write_result "error:unsafe_backup_dir"' in src

    def test_backup_path_rechecked_before_health_rollback_restore(self):
        src = UPDATER_SH.read_text()
        assert "backup_safe()" in src
        guard = src.index("if ! backup_safe; then")
        restore = src.index("snap_cfg=$(ls -1 data/backup/pre-update", guard)
        assert guard < restore

    def test_leaf_symlink_writes_are_guarded_or_atomic(self):
        src = UPDATER_SH.read_text()
        assert '[ ! -L "$UPDATER_LOG" ]' in src
        assert 'write_data_file ".disk-warning" "$waste_gb"' in src
        assert "if [ -L data/config.yaml ] || [ -L data/wallet.json ]; then" in src
        assert '[ ! -e "$snap_cfg_dest" ] && [ ! -L "$snap_cfg_dest" ]' in src
        assert '[ ! -e "$snap_wal_dest" ] && [ ! -L "$snap_wal_dest" ]' in src

    def test_compose_backup_symlink_refusal_precedes_backup_creation(self):
        src = UPDATER_SH.read_text()
        guard = src.index("if [ -L docker-compose.yml.bak ]; then")
        backup = src.index("cp docker-compose.yml docker-compose.yml.bak")
        assert guard < backup
        assert '_write_result "error:compose_backup_failed"' in src

    def test_managed_compose_symlink_can_be_used_as_update_source(self, tmp_path):
        src = UPDATER_SH.read_text()
        start = src.index("if [ -L docker-compose.yml.bak ]; then")
        end = src.index("                    if ! wait_healthy; then", start)
        block = src[start:end]
        docker_log = tmp_path / "docker-calls.log"
        docker = tmp_path / "docker"
        docker.write_text(textwrap.dedent(f"""\
            #!/bin/sh
            echo "$@" >> "{docker_log}"
            exit 0
        """))
        docker.chmod(0o755)
        managed_compose = tmp_path / "managed-compose.yml"
        managed_content = (
            "services:\n"
            "  micromech:\n"
            "    volumes:\n"
            "      - ./data:/app/data\n"
            "      - ./secrets.env:/app/secrets.env\n"
        )
        managed_compose.write_text(managed_content)
        (tmp_path / "docker-compose.yml").symlink_to(managed_compose)
        (tmp_path / "data").mkdir()

        runner = tmp_path / "run-managed-compose.sh"
        runner.write_text(textwrap.dedent(f"""\
            #!/usr/bin/env bash
            set -u
            export PATH="{tmp_path}:$PATH"
            cd "{tmp_path}"
            sleep() {{ :; }}
            log() {{ echo "[updater] $1" >&2; }}
            data_safe() {{ [ -d data ] && [ ! -L data ]; }}
            _write_result() {{ printf '%s\\n' "$1" > data/.update-result; }}
            restore_host_artifacts() {{ :; }}
            rollback_image() {{ :; }}
            MICROMECH_IMAGE="dvilela/micromech:latest"
            PROJECT_DIR="{tmp_path}"
            PROJECT_DIR_COMPOSE=$(printf '%s' "$PROJECT_DIR" | sed 's/\\$/$$/g')
            PROJECT_DIR_SED=$(printf '%s' "$PROJECT_DIR_COMPOSE" | sed 's/[&|\\\\]/\\\\&/g')
            host_backup_dir=""
            RUN_AS="$(id -u):$(id -g)"
            for _ in 1; do
            {block}
            break
            done
        """))
        runner.chmod(0o755)

        result = subprocess.run([str(runner)], capture_output=True, text=True)

        assert result.returncode == 0, result.stderr
        assert managed_compose.read_text() == managed_content
        assert not (tmp_path / "docker-compose.yml.bak").is_symlink()
        assert "compose -f /tmp/micromech-compose-abs-" in docker_log.read_text()

    def test_h3_old_sanitized_after_capture(self):
        """OLD must be sanitized immediately after docker inspect to block label injection."""
        src = UPDATER_SH.read_text()
        assert "OLD=$(printf '%s' \"$OLD\" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)" in src

    def test_h3_new_sanitized_after_capture(self):
        """NEW must be sanitized immediately after docker inspect."""
        src = UPDATER_SH.read_text()
        assert "NEW=$(printf '%s' \"$NEW\" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)" in src

    def test_c2_chown_called_after_restore(self):
        """Restored files must be chowned to the configured service uid/gid."""
        src = UPDATER_SH.read_text()
        assert "for restored_file in data/config.yaml data/wallet.json; do" in src
        assert '[ ! -L "$restored_file" ] && [ -f "$restored_file" ]' in src
        assert 'chown -- "$RUN_AS" "$restored_file"' in src

    def test_data_symlink_refusal_precedes_recursive_chown(self):
        """A symlinked data path must be rejected before recursive chown."""
        src = UPDATER_SH.read_text()
        guard = src.index("if [ -L data ]; then")
        chown = src.index('chown -R -- "$RUN_AS" data/')
        assert guard < chown

    def test_restore_does_not_chmod_symlinked_updater(self):
        src = UPDATER_SH.read_text()
        assert "[ ! -L updater.sh ] && [ -f updater.sh ] && chmod +x updater.sh" in src

    def test_data_symlink_is_rechecked_inside_update_loop(self):
        """A long-running updater must re-check data/ before each request."""
        src = UPDATER_SH.read_text()
        loop = src.index("while true; do")
        loop_guard = src.index("if [ -L data ]; then", loop)
        request_check = src.index("if [ -f data/.update-request ]; then", loop)
        post_stop_guard = src.index("if ! data_safe; then")
        chown = src.index('chown -R -- "$RUN_AS" data/')
        assert loop_guard < request_check
        assert post_stop_guard < chown

    def test_run_as_validation_precedes_chown(self):
        """RUN_AS must be numeric UID:GID before it can reach chown."""
        src = UPDATER_SH.read_text()
        validation = src.index("grep -Eq '^[0-9]+:[0-9]+$'")
        chown = src.index('chown -R -- "$RUN_AS" data/')
        assert validation < chown

    def test_updater_remains_sh_compatible_for_existing_sidecars(self):
        src = UPDATER_SH.read_text()
        assert src.startswith("#!/bin/sh\n")
        assert "local " not in src
        assert "$'\\n'" not in src
        assert "$'\\r'" not in src
        assert "exec bash ./updater.sh" in src

    def test_recreates_updater_sidecar_when_compose_changes(self):
        src = UPDATER_SH.read_text()
        assert "compose_changed=0" in src
        assert 'cmp -s "$host_backup_dir/docker-compose.yml" docker-compose.yml || compose_changed=1' in src
        assert 'if [ "$compose_changed" -eq 1 ]; then' in src
        assert "up -d --force-recreate dockerproxy updater" in src

    def _extract_success_tail(self) -> str:
        src = UPDATER_SH.read_text()
        start = src.index("else\n                        rm -f docker-compose.yml.bak")
        end = src.index("else\n                    _write_result", start)
        return src[start + len("else\n") : end] + "\n"

    def _write_success_runner(
        self, tmp_path: Path, *, docker_recreate_succeeds: bool, updater_changed: bool
    ) -> Path:
        captured_compose = tmp_path / "captured-updater-compose.yml"
        docker_log = tmp_path / "docker-calls.log"
        docker = tmp_path / "docker"
        docker.write_text(textwrap.dedent(f"""\
            #!/bin/sh
            echo "$@" >> "{docker_log}"
            if [ "$1 $2" = "compose -f" ]; then
                cp "$3" "{captured_compose}"
                {'exit 0' if docker_recreate_succeeds else 'exit 1'}
            fi
            exit 0
        """))
        docker.chmod(0o755)
        (tmp_path / "docker-compose.yml").write_text(
            "services:\n"
            "  updater:\n"
            "    volumes:\n"
            "      - ./:/host\n"
            "  dockerproxy:\n"
            "    image: tecnativa/docker-socket-proxy\n"
        )
        (tmp_path / "updater.sh").write_text(
            "#!/usr/bin/env bash\n"
            "echo reloaded > reload-marker\n"
            "exit 42\n"
        )
        (tmp_path / "updater.sh").chmod(0o755)
        runner = tmp_path / "run-success-tail.sh"
        runner.write_text(textwrap.dedent(f"""\
            #!/usr/bin/env bash
            set -u
            export PATH="{tmp_path}:$PATH"
            cd "{tmp_path}"
            log() {{ echo "[updater] $1" >&2; }}
            PROJECT_DIR="{tmp_path}"
            PROJECT_DIR_COMPOSE=$(printf '%s' "$PROJECT_DIR" | sed 's/\\$/$$/g')
            PROJECT_DIR_SED=$(printf '%s' "$PROJECT_DIR_COMPOSE" | sed 's/[&|\\\\]/\\\\&/g')
            compose_changed=1
            updater_changed={1 if updater_changed else 0}
            host_backup_dir=""
            {self._extract_success_tail()}
        """))
        runner.chmod(0o755)
        return runner

    def test_updater_sidecar_recreate_success_path_rewrites_host_bind(self, tmp_path):
        runner = self._write_success_runner(
            tmp_path, docker_recreate_succeeds=True, updater_changed=False
        )
        result = subprocess.run([str(runner)], capture_output=True, text=True)

        assert result.returncode == 0, result.stderr
        docker_log = (tmp_path / "docker-calls.log").read_text()
        assert "compose -f /tmp/micromech-compose-updater-" in docker_log
        assert " --project-directory . up -d --force-recreate dockerproxy updater" in docker_log
        captured = (tmp_path / "captured-updater-compose.yml").read_text()
        assert f"- {tmp_path}:/host" in captured
        assert "- ./:/host" not in captured

    def test_updater_sidecar_recreate_fallback_reloads_updated_script(self, tmp_path):
        runner = self._write_success_runner(
            tmp_path, docker_recreate_succeeds=False, updater_changed=True
        )
        result = subprocess.run([str(runner)], capture_output=True, text=True)

        assert result.returncode == 42
        assert (tmp_path / "reload-marker").read_text() == "reloaded\n"

    def test_stop_failure_restores_artifacts_and_rolls_back(self):
        src = UPDATER_SH.read_text()
        stop = src.index("if ! docker compose stop micromech; then")
        block = src[stop:src.index("sleep 2", stop)]
        assert 'restore_host_artifacts "$host_backup_dir"' in block
        assert "rollback_image" in block
        assert "docker compose up -d micromech" in block
        assert '_write_result "error:stop_failed"' in block

    def test_early_abort_paths_restore_artifacts_and_roll_back(self):
        src = UPDATER_SH.read_text()
        for marker in (
            "data/ directory unsafe",
            "PROJECT_DIR contains unsupported characters",
        ):
            start = src.index(marker)
            block = src[start:src.index("continue", start)]
            assert 'restore_host_artifacts "$host_backup_dir"' in block
            assert "rollback_image" in block
            assert "docker compose up -d micromech" in block

    def _write_failure_runner(
        self, tmp_path: Path, block: str, *, project_dir: str | None = None
    ) -> Path:
        docker_log = tmp_path / "docker-calls.log"
        docker = tmp_path / "docker"
        docker.write_text(textwrap.dedent(f"""\
            #!/bin/sh
            echo "$@" >> "{docker_log}"
            case "$1 $2" in
                "compose stop") exit 1 ;;
                "compose up") exit 0 ;;
                "image inspect") exit 0 ;;
                "tag "*) exit 0 ;;
                *) exit 0 ;;
            esac
        """))
        docker.chmod(0o755)
        backup = tmp_path / "backup"
        backup.mkdir()
        for artifact in ("docker-compose.yml", "Justfile", "updater.sh"):
            (backup / artifact).write_text(f"old {artifact}\n")
            (tmp_path / artifact).write_text(f"new {artifact}\n")
        (tmp_path / "data").mkdir()
        runner = tmp_path / "run-failure.sh"
        runner.write_text(textwrap.dedent(f"""\
            #!/usr/bin/env bash
            set -u
            export PATH="{tmp_path}:$PATH"
            cd "{tmp_path}"
            sleep() {{ :; }}
            log() {{ echo "[updater] $1" >&2; }}
            data_safe() {{ [ -d data ] && [ ! -L data ]; }}
            _write_result() {{
                data_safe || return 0
                printf '%s\\n' "$1" > data/.update-result.tmp 2>/dev/null \\
                    && mv data/.update-result.tmp data/.update-result 2>/dev/null || true
            }}
            restore_host_artifacts() {{
                backup_dir="${{1:-}}"
                [ -n "$backup_dir" ] || return 0
                for artifact in docker-compose.yml Justfile updater.sh; do
                    [ -f "$backup_dir/$artifact" ] && cp -p "$backup_dir/$artifact" "$artifact"
                done
            }}
            rollback_image() {{
                docker image inspect "${{MICROMECH_IMAGE%:latest}}:rollback-prev" >/dev/null 2>&1 \\
                    && docker tag "${{MICROMECH_IMAGE%:latest}}:rollback-prev" "$MICROMECH_IMAGE"
            }}
            MICROMECH_IMAGE="dvilela/micromech:latest"
            host_backup_dir="{backup}"
            PROJECT_DIR="{project_dir or tmp_path}"
            for _ in 1; do
            {block}
            break
            done
        """))
        runner.chmod(0o755)
        return runner

    def test_stop_failure_restores_artifacts_behaviorally(self, tmp_path):
        src = UPDATER_SH.read_text()
        start = src.index("if ! docker compose stop micromech; then")
        end = src.index("sleep 2", start)
        runner = self._write_failure_runner(tmp_path, src[start:end])

        result = subprocess.run([str(runner)], capture_output=True, text=True)

        assert result.returncode == 0, result.stderr
        for artifact in ("docker-compose.yml", "Justfile", "updater.sh"):
            assert (tmp_path / artifact).read_text() == f"old {artifact}\n"
        log = (tmp_path / "docker-calls.log").read_text()
        assert "tag dvilela/micromech:rollback-prev dvilela/micromech:latest" in log
        assert "compose up -d micromech" in log
        assert (tmp_path / "data" / ".update-result").read_text() == "error:stop_failed\n"
        assert not (tmp_path / "backup").exists()

    def test_data_missing_restores_artifacts_behaviorally(self, tmp_path):
        src = UPDATER_SH.read_text()
        marker = src.index("data/ directory unsafe — cannot safely update")
        start = src.rfind("if ! data_safe; then", 0, marker)
        end = src.index("                    if printf", start)
        runner = self._write_failure_runner(tmp_path, src[start:end])
        (tmp_path / "data").rmdir()

        result = subprocess.run([str(runner)], capture_output=True, text=True)

        assert result.returncode == 0, result.stderr
        for artifact in ("docker-compose.yml", "Justfile", "updater.sh"):
            assert (tmp_path / artifact).read_text() == f"old {artifact}\n"
        log = (tmp_path / "docker-calls.log").read_text()
        assert "tag dvilela/micromech:rollback-prev dvilela/micromech:latest" in log
        assert "compose up -d micromech" in log
        assert not (tmp_path / "backup").exists()

    def test_data_symlink_abort_does_not_write_through_target(self, tmp_path):
        src = UPDATER_SH.read_text()
        marker = src.index("data/ directory unsafe — cannot safely update")
        start = src.rfind("if ! data_safe; then", 0, marker)
        end = src.index("                    if printf", start)
        runner = self._write_failure_runner(tmp_path, src[start:end])
        target = tmp_path / "outside-data"
        target.mkdir()
        (tmp_path / "data").rmdir()
        (tmp_path / "data").symlink_to(target, target_is_directory=True)

        result = subprocess.run([str(runner)], capture_output=True, text=True)

        assert result.returncode == 0, result.stderr
        assert not (target / "updater.log").exists()
        assert not (target / ".update-result").exists()

    def test_unsafe_project_dir_restores_artifacts_behaviorally(self, tmp_path):
        src = UPDATER_SH.read_text()
        start = src.index("if printf '%s' \"$PROJECT_DIR\"")
        end = src.index("                    PROJECT_DIR_COMPOSE=", start)
        runner = self._write_failure_runner(
            tmp_path, src[start:end], project_dir=f"{tmp_path}:bad"
        )

        result = subprocess.run([str(runner)], capture_output=True, text=True)

        assert result.returncode == 0, result.stderr
        for artifact in ("docker-compose.yml", "Justfile", "updater.sh"):
            assert (tmp_path / artifact).read_text() == f"old {artifact}\n"
        log = (tmp_path / "docker-calls.log").read_text()
        assert "tag dvilela/micromech:rollback-prev dvilela/micromech:latest" in log
        assert "compose up -d micromech" in log
        assert (tmp_path / "data" / ".update-result").read_text() == "error:unsafe_project_dir\n"
        assert not (tmp_path / "backup").exists()


class TestQuickstartHostArtifacts:
    def test_quickstart_copies_updater_from_image(self):
        src = QUICKSTART_SH.read_text()
        assert "/app/scripts/updater.sh" in src
        assert 'bash -n "$tmp"' in src
        assert 'mv "$tmp" updater.sh' in src
        assert "UPDATEREOF" not in src

    def test_quickstart_adds_dockerproxy_sidecar(self):
        src = QUICKSTART_SH.read_text()
        assert "dockerproxy:" in src
        assert "image: tecnativa/docker-socket-proxy" in src
        assert "DOCKER_HOST=tcp://dockerproxy:2375" in src
        for permission in (
            "PING=1",
            "VERSION=1",
            "INFO=1",
            "DELETE=1",
            "CONTAINERS=1",
            "POST=1",
            "IMAGES=1",
            "NETWORKS=1",
            "VOLUMES=1",
            "ALLOW_START=1",
            "ALLOW_STOP=1",
            "ALLOW_RESTARTS=1",
        ):
            assert permission in src
        assert "depends_on:" in src
        assert "- dockerproxy" in src

    def test_quickstart_mounts_project_for_updater_self_update(self):
        src = QUICKSTART_SH.read_text()
        assert "- ./:/host" in src
        assert "HOST_PROJECT_DIR=__HOST_PROJECT_DIR__" in src
        assert "UPDATER_RUN_AS=__USER_UID_GID__" in src

    def test_generated_justfile_uses_private_quickstart_tempfile(self):
        src = QUICKSTART_SH.read_text()
        assert "/tmp/micromech-qs.sh" not in src
        assert "mktemp /tmp/micromech-qs-XXXXXX" in src
        assert 'trap \'rm -f "\\$qs_tmp"\' EXIT' in src

    def test_quickstart_detects_existing_image_channel(self):
        src = QUICKSTART_SH.read_text()
        assert "detect_micromech_image()" in src
        assert "dvilela\\/micromech[^[:space:]]*:latest" in src


class TestRollbackC1MtimeSort:
    """C1 fix: restore must pick the snapshot with the newest ISO *filename*,
    not the newest mtime — cp -p preserves source mtime so mtime order diverges."""

    def test_sort_by_filename_beats_mtime_order(self, sandbox):
        pre = sandbox / "data" / "backup" / "pre-update"
        older_ts, newer_ts = "20260424T100000Z", "20260424T120000Z"
        snap_older = pre / f"config.yaml.{older_ts}.bak"
        snap_newer = pre / f"config.yaml.{newer_ts}.bak"
        snap_older.write_text("old-snapshot\n")
        snap_newer.write_text("new-snapshot\n")
        # Invert mtimes: older-named file gets a far-future mtime (would win by mtime sort)
        far_future = 9_999_999_999.0
        os.utime(snap_older, (far_future, far_future))
        os.utime(snap_newer, (0.0, 0.0))
        (sandbox / "data" / "config.yaml").write_text("broken\n")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{newer_ts}"; _pre_dir="data/backup/pre-update"'
        )
        result = subprocess.run([str(runner)], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert (sandbox / "data" / "config.yaml").read_text() == "new-snapshot\n"


class TestWaitHealthyStructural:
    """wait_healthy() must use the conditional Go template to avoid the \\nnone bug."""

    def test_uses_conditional_go_template(self):
        src = UPDATER_SH.read_text()
        assert "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}" in src

    def test_bare_health_status_format_absent(self):
        src = UPDATER_SH.read_text()
        for line in src.splitlines():
            stripped = line.strip()
            if "format=" in stripped and "State.Health.Status" in stripped:
                assert "{{if .State.Health}}" in stripped, (
                    f"bare {{{{.State.Health.Status}}}} found without conditional: {line!r}"
                )

    def test_accepts_healthy_and_none_conditions(self):
        src = UPDATER_SH.read_text()
        assert '[ "$health" = "healthy" ]' in src
        assert '[ "$health" = "none" ]' in src


class TestWaitHealthyBehavior:
    """Behavioral tests: run wait_healthy() in a subprocess with a mocked docker."""

    def _make_runner(self, tmp_path: Path, health_val: str) -> Path:
        src = UPDATER_SH.read_text()
        fn_start = src.find("wait_healthy()")
        fn_end = src.find("\n}", fn_start) + 2
        fn_src = src[fn_start:fn_end].replace("max=120", "max=3")

        bin_dir = tmp_path / "bin"
        bin_dir.mkdir(exist_ok=True)
        docker = bin_dir / "docker"
        docker.write_text(textwrap.dedent(f"""\
            #!/bin/sh
            case "$*" in
                "compose ps -q micromech") printf 'micromech-micromech-1\\n' ;;
                *State.Health*) printf '%s\\n' "{health_val}" ;;
                *State.Status*) printf 'running\\n' ;;
                *) exit 0 ;;
            esac
            exit 0
        """))
        docker.chmod(0o755)

        runner = tmp_path / "run_wh.sh"
        header = (
            f"#!/bin/sh\n"
            f"export PATH=\"{bin_dir}:$PATH\"\n"
            "log() { :; }\n"
            "sleep() { :; }\n\n"
        )
        runner.write_text(header + fn_src + "\nwait_healthy\n")
        runner.chmod(0o755)
        return runner

    def test_healthy_returns_zero(self, tmp_path):
        result = subprocess.run([str(self._make_runner(tmp_path, "healthy"))],
                                capture_output=True, text=True)
        assert result.returncode == 0

    def test_none_returns_zero_backward_compat(self, tmp_path):
        """health=none means no HEALTHCHECK on image — must pass for old images."""
        result = subprocess.run([str(self._make_runner(tmp_path, "none"))],
                                capture_output=True, text=True)
        assert result.returncode == 0

    def test_starting_times_out(self, tmp_path):
        result = subprocess.run([str(self._make_runner(tmp_path, "starting"))],
                                capture_output=True, text=True)
        assert result.returncode == 1

    def test_unhealthy_times_out(self, tmp_path):
        result = subprocess.run([str(self._make_runner(tmp_path, "unhealthy"))],
                                capture_output=True, text=True)
        assert result.returncode == 1
