"""IPFS client — push and pull data via HTTP.

Push uses iwa's IPFS API (requires an IPFS node with /api/v0/add).
Pull uses any public IPFS gateway (read-only HTTP GET).
CID computation is pure (no network needed).
"""

import base64
import hashlib
import json
from typing import Any, Optional

import aiohttp
from loguru import logger

from micromech.core.constants import IPFS_GATEWAY

# Multihash prefix for sha2-256: function code (0x12) + digest length (0x20)
_SHA256_MULTIHASH_PREFIX = bytes([0x12, 0x20])

# CIDv1 prefix: version(1) + codec(raw=0x55) + multihash(sha256=0x12, len=0x20)
_CIDV1_RAW_SHA256_PREFIX = bytes([0x01, 0x55, 0x12, 0x20])


def compute_cid(data: bytes) -> str:
    """Compute CIDv1 base32 string for raw data.

    Returns a bafkrei... CID (raw codec, sha2-256).
    """
    digest = hashlib.sha256(data).digest()
    cid_bytes = _CIDV1_RAW_SHA256_PREFIX + digest
    b32 = base64.b32encode(cid_bytes).decode().lower().rstrip("=")
    return "b" + b32


def compute_cid_hex(data: bytes) -> str:
    """Compute CIDv1 hex representation (f01551220...).

    This is the format used internally by iwa and the OLAS contracts.
    """
    digest = hashlib.sha256(data).digest()
    cid_bytes = _CIDV1_RAW_SHA256_PREFIX + digest
    return "f" + cid_bytes.hex()


def cid_hex_to_multihash_bytes(cid_hex: str) -> bytes:
    """Extract multihash bytes from CIDv1 hex string.

    f01551220<sha256hex> → bytes(0x12 0x20 <sha256>)
    f(1) + 01(2) + 55(2) = 5 hex chars to skip, leaving 1220<sha256>
    """
    return bytes.fromhex(cid_hex[5:])


def is_ipfs_multihash(data: bytes) -> bool:
    """Check if data looks like a truncated IPFS multihash (sha2-256).

    On-chain requestData contains 34 bytes: 0x12 + 0x20 + 32-byte SHA-256 digest.
    """
    return len(data) == 34 and data[:2] == _SHA256_MULTIHASH_PREFIX


def multihash_to_cid(data: bytes) -> str:
    """Convert raw multihash bytes (0x12 0x20 <digest>) back to a CID string.

    Used to reconstruct the CID from on-chain requestData for IPFS fetch.
    """
    if len(data) < 34 or data[:2] != _SHA256_MULTIHASH_PREFIX:
        msg = f"Invalid multihash: expected 34 bytes starting with 0x1220, got {len(data)} bytes"
        raise ValueError(msg)
    digest = data[2:34]
    cid_bytes = _CIDV1_RAW_SHA256_PREFIX + digest
    b32 = base64.b32encode(cid_bytes).decode().lower().rstrip("=")
    return "b" + b32


async def fetch_from_ipfs(
    cid: str,
    gateway: str = IPFS_GATEWAY,
    timeout: int = 30,
) -> bytes:
    """Fetch data from IPFS via HTTP gateway.

    Args:
        cid: The IPFS CID string (bafkrei... or Qm...).
        gateway: Gateway base URL (must end with /).
        timeout: Request timeout in seconds.

    Returns:
        Raw bytes from IPFS.
    """
    url = f"{gateway}{cid}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            resp.raise_for_status()
            data = await resp.read()
            logger.debug("Fetched {} bytes from IPFS: {}", len(data), cid[:16])
            return data


async def fetch_json_from_ipfs(
    cid: str,
    gateway: str = IPFS_GATEWAY,
    timeout: int = 30,
) -> dict:
    """Fetch and parse JSON from IPFS."""
    data = await fetch_from_ipfs(cid, gateway, timeout)
    return json.loads(data)


async def push_to_ipfs(
    data: bytes,
    api_url: Optional[str] = None,
) -> tuple[str, str]:
    """Push data to IPFS. Returns (CID string, CID hex).

    Uses iwa's IPFS API if available, otherwise computes CID locally.
    """
    try:
        from iwa.core.ipfs import push_to_ipfs_async

        return await push_to_ipfs_async(data, api_url=api_url)
    except ImportError:
        # No iwa — compute CID locally (data won't actually be on IPFS)
        cid_str = compute_cid(data)
        cid_hex = compute_cid_hex(data)
        logger.warning("iwa not available — CID computed locally, data NOT pushed to IPFS")
        return cid_str, cid_hex


async def push_json_to_ipfs(
    obj: dict[str, Any],
    api_url: Optional[str] = None,
) -> tuple[str, str]:
    """Push JSON object to IPFS. Returns (CID string, CID hex)."""
    data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    return await push_to_ipfs(data, api_url=api_url)


def prepare_request_data(metadata: dict[str, Any]) -> bytes:
    """Convert metadata to on-chain request data bytes.

    Pushes to IPFS (sync) and returns the truncated multihash bytes
    that go into marketplace.request(requestData, ...).
    """
    try:
        from iwa.core.ipfs import metadata_to_request_data

        return metadata_to_request_data(metadata)
    except ImportError:
        # Compute locally
        data = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
        cid_hex = compute_cid_hex(data)
        return cid_hex_to_multihash_bytes(cid_hex)
