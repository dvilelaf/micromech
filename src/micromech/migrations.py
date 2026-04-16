"""Config migration helpers for Micromech.

All functions must be called BEFORE MicromechConfig.load() reads from disk.
"""

import os
import tempfile
from pathlib import Path

from loguru import logger


def migrate_removed_fields(config_path: Path, removed_fields: dict[str, str | None]) -> list[str]:
    """Strip/rename obsolete fields from config.yaml before MicromechConfig loads it.

    Uses ruamel.yaml so YAML comments are preserved. Writes atomically so the
    fix is permanent — the warning only appears once, not on every restart.

    Returns the list of field names that were migrated (empty if nothing done).
    """
    if not removed_fields or not config_path.exists():
        return []

    from ruamel.yaml import YAML as _YAML  # type: ignore[import-not-found]

    ryaml = _YAML()
    ryaml.preserve_quotes = True

    try:
        with config_path.open("r", encoding="utf-8") as f:
            doc = ryaml.load(f) or {}
    except Exception as load_err:
        logger.warning(f"Config migration: cannot parse {config_path} ({load_err}), skipping.")
        return []

    micromech_section = (doc.get("plugins") or {}).get("micromech") or {}
    stale = [k for k in list(micromech_section.keys()) if k in removed_fields]
    if not stale:
        return []

    for old_key in stale:
        new_key = removed_fields[old_key]
        if new_key:
            logger.info(
                f"Config migration: renaming '{old_key}' → '{new_key}' "
                "(field was renamed in this version of Micromech)"
            )
            if new_key not in micromech_section:
                micromech_section[new_key] = micromech_section[old_key]
            del micromech_section[old_key]
        else:
            logger.info(
                f"Config migration: removing obsolete field '{old_key}' "
                "(field no longer exists in this version of Micromech)"
            )
            del micromech_section[old_key]

    fd, tmp_path = tempfile.mkstemp(
        dir=config_path.parent, prefix=".config_migrate_", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            ryaml.dump(doc, out)
        os.replace(tmp_path, config_path)
        logger.info(
            f"Config migration complete: removed {len(stale)} obsolete field(s) "
            f"({', '.join(stale)}). Config saved."
        )
    except Exception as write_err:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        logger.warning(
            f"Config migration: could not persist cleaned config ({write_err}). "
            "Will re-migrate on next restart."
        )

    return stale


def migrate_schema(config_path: Path) -> list[str]:
    """Apply schema version upgrades to config.yaml.

    Each migration bumps schema_version so it only runs once.
    Returns a list of human-readable descriptions of changes made.
    """
    if not config_path.exists():
        return []

    from ruamel.yaml import YAML as _YAML  # type: ignore[import-not-found]

    ryaml = _YAML()
    ryaml.preserve_quotes = True

    try:
        with config_path.open("r", encoding="utf-8") as f:
            doc = ryaml.load(f) or {}
    except Exception as load_err:
        logger.warning(f"Schema migration: cannot parse {config_path} ({load_err}), skipping.")
        return []

    micromech_section = (doc.get("plugins") or {}).get("micromech")
    if micromech_section is None:
        return []

    _version = micromech_section.get("schema_version", 1)
    changes: list[str] = []

    # No schema migrations yet — placeholder for future use.

    if not changes:
        return []

    fd, tmp_path = tempfile.mkstemp(
        dir=config_path.parent, prefix=".config_schema_", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            ryaml.dump(doc, out)
        os.replace(tmp_path, config_path)
        logger.info(f"Schema migration complete: {'; '.join(changes)}")
    except Exception as write_err:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        logger.warning(
            f"Schema migration: could not persist config ({write_err}). "
            "Will re-migrate on next restart."
        )

    return changes
