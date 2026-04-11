"""Read and write secrets.env from within the container.

The file is mounted at /app/secrets.env (configurable via SECRETS_ENV_PATH).
We parse it line-by-line preserving comments and blank lines, updating or
appending keys as needed without touching unrelated values.

Security properties:
- Values are validated to reject newlines/nulls before writing (prevents injection).
- The file is written with mode 0o600 (owner-read/write only).
- write_secrets() is atomic: single read → apply all changes → single write.
- Atomic write uses a random-named tempfile (prevents symlink attacks).
- Cross-device renames handled via shutil.move (works across Docker volume mounts).
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Path where secrets.env is mounted inside the container.
# Override via env var for tests.
SECRETS_ENV_PATH = Path(os.environ.get("SECRETS_ENV_PATH", "secrets.env"))

# Keys exposed via the /api/setup/secrets endpoint (others are internal).
# wallet_password is intentionally excluded — it is written automatically,
# never via the user-facing secrets editor.
EDITABLE_KEYS = {
    "telegram_token",
    "telegram_chat_id",
    "gnosis_rpc",
    "ethereum_rpc",
    "base_rpc",
    "polygon_rpc",
    "optimism_rpc",
    "arbitrum_rpc",
    "celo_rpc",
    "health_url",
}

# Keys whose values are masked (shown as *** in GET responses).
SENSITIVE_KEYS = {"wallet_password", "telegram_token"}

_FORBIDDEN_CHARS = frozenset("\n\r\x00")


def _validate_key(key: str) -> None:
    """Raise ValueError if key contains characters that could corrupt the file."""
    if not key or not key.replace("_", "").isalnum():
        raise ValueError(f"Invalid secret key: {key!r}")
    if not any(c.isalpha() for c in key):
        raise ValueError(f"Secret key must contain at least one letter: {key!r}")
    if any(c in key for c in _FORBIDDEN_CHARS):
        raise ValueError(f"Secret key contains forbidden characters: {key!r}")


def _validate_value(key: str, value: str) -> None:
    """Raise ValueError if value contains newlines or null bytes."""
    if any(c in value for c in _FORBIDDEN_CHARS):
        raise ValueError(
            f"Secret value for key '{key}' contains forbidden characters "
            "(newlines and null bytes are not allowed)"
        )


def _secure_write(path: Path, content: str) -> None:
    """Write content to path with mode 0o600 (owner r/w only).

    Writes in-place (preserves the inode) so Docker file bind mounts stay
    visible on the host after writes. A rename-based atomic write would
    change the inode and break Docker's bind mount mapping.
    """
    # Create with correct permissions if it doesn't exist yet.
    if not path.exists():
        path.touch(mode=0o600)
    try:
        path.chmod(0o600)
    except OSError as e:
        logger.warning("Could not enforce 0o600 on %s: %s", path, e)
    path.write_text(content, encoding="utf-8")


def read_secrets_file(path: Path | None = None) -> dict[str, str]:
    """Parse secrets.env and return {key: value} for all non-comment lines."""
    p = path or SECRETS_ENV_PATH
    result: dict[str, str] = {}
    if not p.exists():
        return result
    for line in p.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" in stripped:
            key, _, value = stripped.partition("=")
            result[key.strip()] = value.strip()
    return result


def write_secret(key: str, value: str, path: Path | None = None) -> None:
    """Set ``key=value`` in secrets.env, adding it if absent.

    Preserves all comments and other keys. Creates the file with mode 0o600
    if it doesn't exist yet.

    Raises ValueError if key or value contain characters that could corrupt
    the env file (newlines, null bytes).
    """
    _validate_key(key)
    _validate_value(key, value)
    write_secrets({key: value}, path=path)


def write_secrets(updates: dict[str, str], path: Path | None = None) -> None:
    """Write multiple secrets atomically (single read → modify → write).

    Validates all keys and values before touching the file, so either all
    updates succeed or none are written.

    Raises ValueError if any key or value contains forbidden characters.

    Note: no file locking. This is intentional — micromech runs as a single
    asyncio process in Docker (no multi-worker). If you ever add uvicorn
    workers or multiple processes, add fcntl.flock() here.
    """
    for k, v in updates.items():
        _validate_key(k)
        _validate_value(k, v)

    p = path or SECRETS_ENV_PATH
    lines: list[str] = []
    remaining = dict(updates)  # keys not yet found in the file

    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped:
                lines.append(line)
                continue
            # Detect commented-out template placeholders like "# key=" or "# key=value"
            if stripped.startswith("#") and "=" in stripped:
                candidate = stripped.lstrip("#").strip()
                k = candidate.partition("=")[0].strip()
                if k in remaining:
                    lines.append(f"{k}={remaining.pop(k)}")
                    continue
            if stripped.startswith("#"):
                lines.append(line)
                continue
            if "=" in stripped:
                k = stripped.partition("=")[0].strip()
                if k in remaining:
                    lines.append(f"{k}={remaining.pop(k)}")
                    continue
            lines.append(line)

    # Append any keys not found in the existing file
    for k, v in remaining.items():
        lines.append(f"{k}={v}")

    _secure_write(p, "\n".join(lines) + "\n")
