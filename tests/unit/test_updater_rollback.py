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
    # End just before `else\n  # Success: clean up...`
    end = src.find("else\n                        # Success: clean up", start)
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
        log() {{ echo "[updater] $1" >&2; }}
        _write_result() {{
            printf '%s\\n' "$1" > data/.update-result.tmp 2>/dev/null \\
                && mv data/.update-result.tmp data/.update-result 2>/dev/null || true
        }}
        wait_healthy() {{ return 1; }}
        MICROMECH_IMAGE="dvilela/micromech:latest"
        {vars_setup}
        # === injected rollback block follows ===
    """)
    footer = textwrap.dedent("""\

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
    (tmp_path / "data" / "backups" / "pre-update").mkdir(parents=True)
    return tmp_path


class TestRollbackBehavior:

    def test_marker_uses_error_rolled_back_to_v_prefix(self, sandbox):
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="20260424T120000Z"; '
            '_pre_dir="data/backups/pre-update"'
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
            '_pre_dir="data/backups/pre-update"'
        )
        result = subprocess.run([str(runner)], capture_output=True, text=True)
        assert "MARKER:error:rolled_back_to_vunknown" in result.stdout

    def test_image_inspect_called_before_retag(self, sandbox):
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="20260424T120000Z"; '
            '_pre_dir="data/backups/pre-update"'
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
            '_pre_dir="data/backups/pre-update"'
        )
        result = subprocess.run([str(runner)], capture_output=True, text=True)
        log = (sandbox / "docker-calls.log").read_text()
        assert "tag dvilela/micromech:rollback-prev" not in log
        assert "MARKER:error:rolled_back_to_v0.5.1" in result.stdout

    def test_restores_config_from_snapshot(self, sandbox):
        ts = "20260424T120000Z"
        snap = sandbox / "data" / "backups" / "pre-update" / f"config.yaml.{ts}.bak"
        snap.write_text("snapshot-cfg\n")
        (sandbox / "data" / "config.yaml").write_text("broken-new\n")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{ts}"; '
            f'_pre_dir="data/backups/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        assert (sandbox / "data" / "config.yaml").read_text() == "snapshot-cfg\n"

    def test_restores_wallet_from_snapshot(self, sandbox):
        ts = "20260424T120000Z"
        snap = sandbox / "data" / "backups" / "pre-update" / f"wallet.json.{ts}.bak"
        snap.write_text('{"snapshot": true}\n')
        (sandbox / "data" / "wallet.json").write_text('{"broken": true}\n')
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{ts}"; '
            f'_pre_dir="data/backups/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        assert (sandbox / "data" / "wallet.json").read_text() == '{"snapshot": true}\n'

    def test_no_temp_files_left(self, sandbox):
        ts = "20260424T120000Z"
        for name in ("config.yaml", "wallet.json"):
            (sandbox / "data" / "backups" / "pre-update" / f"{name}.{ts}.bak").write_text("x")
            (sandbox / "data" / name).write_text("orig")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{ts}"; '
            f'_pre_dir="data/backups/pre-update"'
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
            '_pre_dir="data/backups/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        assert (sandbox / "data" / "config.yaml").read_text() == "new-broken\n"
        assert (sandbox / "data" / "wallet.json").read_text() == "new-broken\n"

    def test_glob_strict_rejects_planted_filename(self, sandbox):
        (sandbox / "data" / "backups" / "pre-update" / "config.yaml.malicious.bak").write_text(
            "PLANTED\n"
        )
        (sandbox / "data" / "config.yaml").write_text("real\n")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="20260424T120000Z"; '
            '_pre_dir="data/backups/pre-update"'
        )
        subprocess.run([str(runner)], capture_output=True, text=True)
        assert (sandbox / "data" / "config.yaml").read_text() != "PLANTED\n"

    def test_compose_stop_called_before_up(self, sandbox):
        ts = "20260424T120000Z"
        (sandbox / "data" / "backups" / "pre-update" / f"config.yaml.{ts}.bak").write_text("x")
        _docker_mock(sandbox / "bin", "rollback_prev_exists")
        rollback = _extract_rollback_block(UPDATER_SH)
        runner = _write_runner(
            sandbox, rollback,
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{ts}"; '
            f'_pre_dir="data/backups/pre-update"'
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
        assert "data/config.yaml.tmp" in src
        assert "data/wallet.json.tmp" in src
        assert "mv data/config.yaml.tmp data/config.yaml" in src
        assert "mv data/wallet.json.tmp data/wallet.json" in src

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
        assert "docker inspect --format='{{.State.Status}}' micromech" in src

    def test_h1_atomic_write_helper_used(self):
        """All .update-result writes must go through _write_result (atomic tmp+mv)."""
        src = UPDATER_SH.read_text()
        assert "_write_result()" in src
        assert "data/.update-result.tmp" in src
        for line in src.split("\n"):
            if "> data/.update-result" in line and ".tmp" not in line:
                raise AssertionError(f"bare redirect to .update-result found: {line!r}")

    def test_h2_backup_dir_chmod_700(self):
        """Backup dir must be locked to 700 even if created with a loose umask."""
        src = UPDATER_SH.read_text()
        assert 'chmod 700 "$_pre_dir"' in src

    def test_h3_old_sanitized_after_capture(self):
        """OLD must be sanitized immediately after docker inspect to block label injection."""
        src = UPDATER_SH.read_text()
        assert "OLD=$(printf '%s' \"$OLD\" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)" in src

    def test_h3_new_sanitized_after_capture(self):
        """NEW must be sanitized immediately after docker inspect."""
        src = UPDATER_SH.read_text()
        assert "NEW=$(printf '%s' \"$NEW\" | tr -cd 'A-Za-z0-9._-' | cut -c1-32)" in src

    def test_c2_chown_called_after_restore(self):
        """Updater runs as root; restored files must be chowned to 1000:1000 so bot (uid 1000) can read them."""
        src = UPDATER_SH.read_text()
        assert "chown 1000:1000 data/config.yaml data/wallet.json" in src


class TestRollbackC1MtimeSort:
    """C1 fix: restore must pick the snapshot with the newest ISO *filename*,
    not the newest mtime — cp -p preserves source mtime so mtime order diverges."""

    def test_sort_by_filename_beats_mtime_order(self, sandbox):
        pre = sandbox / "data" / "backups" / "pre-update"
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
            f'OLD="0.5.1"; NEW="0.5.2"; _pre_ts="{newer_ts}"; _pre_dir="data/backups/pre-update"'
        )
        result = subprocess.run([str(runner)], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert (sandbox / "data" / "config.yaml").read_text() == "new-snapshot\n"
