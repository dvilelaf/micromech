"""Metadata manager — orchestrates tool metadata lifecycle.

Handles: scan tools → build metadata → push IPFS → update on-chain hash.
Tracks state to detect when tools change and metadata needs republishing.
"""

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from loguru import logger

from micromech.core.config import MicromechConfig

# Default tools directory (built-in tools inside the package)
_BUILTIN_TOOLS_DIR = Path(__file__).parent / "tools"


@dataclass
class MetadataResult:
    """Result of a metadata publish operation."""

    metadata: dict = field(default_factory=dict)
    ipfs_cid: str = ""
    onchain_hash: str = ""
    chain_txs: dict[str, str] = field(default_factory=dict)  # chain → tx_hash
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None


@dataclass
class MetadataStatus:
    """Current metadata state for display."""

    tools_fingerprint: dict[str, str] = field(default_factory=dict)
    needs_update: bool = False
    changed_packages: list[str] = field(default_factory=list)
    ipfs_cid: Optional[str] = None
    computed_hash: Optional[str] = None
    stored_hash: Optional[str] = None
    tools: list[dict[str, Any]] = field(default_factory=list)


class MetadataManager:
    """Orchestrates tool metadata publication and change detection."""

    def __init__(
        self,
        config: MicromechConfig,
        tools_dirs: list[Path] | None = None,
    ):
        self.config = config
        self.tools_dirs = tools_dirs or [_BUILTIN_TOOLS_DIR]

    def _scan_all(self) -> list[dict]:
        """Scan all tools directories (builtin + custom), merged.

        Packages listed in ``disabled_tools`` are excluded — the metadata
        pipeline must reflect the same tool set the runtime actually
        serves, otherwise on-chain ALLOWED_TOOLS would advertise tools
        the mech cannot execute.

        The disabled list is re-read from disk on every scan. Multiple
        subsystems hold their own MicromechConfig references (RuntimeManager,
        MechServer, this MetadataManager, the web setup handler), and the
        ``POST /api/setup/tools`` handler saves to disk without touching
        those in-memory copies. Re-reading here guarantees the metadata
        pipeline always reflects what was last persisted, regardless of
        which reference is stale.
        """
        from micromech.ipfs.metadata import scan_tool_packages

        try:
            fresh = MicromechConfig.load()
            disabled_list = fresh.disabled_tools or []
            # Also propagate to the live config so publish() persists the
            # correct value alongside the new metadata hashes.
            self.config.disabled_tools = disabled_list
        except Exception:
            disabled_list = self.config.disabled_tools or []
        disabled = set(disabled_list)

        tools: list[dict] = []
        for d in self.tools_dirs:
            source = "builtin" if d == _BUILTIN_TOOLS_DIR else "custom"
            for t in scan_tool_packages(d, source=source):
                if t["name"] in disabled:
                    continue
                tools.append(t)
        return tools

    def get_status(self) -> MetadataStatus:
        """Get current metadata state: fingerprints, staleness, tools list."""
        from micromech.ipfs.metadata import build_metadata, compute_onchain_hash

        # Single scan — extract fingerprints directly (no triple scan)
        tools = self._scan_all()
        current_fps = {t["name"]: t["package_cid"] for t in tools if t.get("package_cid")}
        metadata = build_metadata(tools)
        current_hash = compute_onchain_hash(metadata)

        # Compare against stored hash
        stored_hash = self.config.metadata_onchain_hash
        needs_update = stored_hash is None or stored_hash != current_hash

        changed = []
        if stored_hash and needs_update:
            stored_fps = self.config.metadata_fingerprints or {}
            for name, cid in current_fps.items():
                if name not in stored_fps or stored_fps[name] != cid:
                    changed.append(name)
            for name in stored_fps:
                if name not in current_fps:
                    changed.append(name)

        return MetadataStatus(
            tools_fingerprint=current_fps,
            needs_update=needs_update,
            changed_packages=changed,
            ipfs_cid=self.config.metadata_ipfs_cid,
            computed_hash=current_hash,
            stored_hash=stored_hash,
            tools=[
                {
                    "name": t["name"],
                    "version": t["version"],
                    "tools": t["allowed_tools"],
                    "package_cid": t["package_cid"],
                    "source": t.get("source", "builtin"),
                }
                for t in tools
            ],
        )

    async def publish(
        self,
        update_onchain: bool = True,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> MetadataResult:
        """Full publish pipeline: scan → build → push IPFS → update on-chain.

        Args:
            update_onchain: If True, call changeHash() on all enabled chains.
            on_progress: Optional callback(step, message) for progress reporting.
        """
        from micromech.ipfs.client import push_json_to_ipfs
        from micromech.ipfs.metadata import build_metadata, compute_onchain_hash

        result = MetadataResult()

        def _progress(step: str, msg: str) -> None:
            logger.info("[metadata] {}: {}", step, msg)
            if on_progress:
                on_progress(step, msg)

        try:
            # Step 1: Scan and build
            _progress("scan", "Scanning tool packages...")
            tools = self._scan_all()
            metadata = build_metadata(tools)
            result.metadata = metadata
            _progress("scan", f"Found {len(tools)} packages, {len(metadata['tools'])} tool IDs")

            # Step 2: Push to IPFS
            _progress("ipfs", "Pushing metadata to IPFS...")
            cid, cid_hex = await push_json_to_ipfs(metadata)
            result.ipfs_cid = cid
            _progress("ipfs", f"Pushed: {cid}")

            # Step 3: Compute on-chain hash
            onchain_hash = compute_onchain_hash(metadata)
            result.onchain_hash = onchain_hash

            # Step 4: Update on-chain (per chain)
            if update_onchain:
                for chain_name in self.config.enabled_chains:
                    chain_cfg = self.config.chains[chain_name]
                    if not chain_cfg.setup_complete:
                        continue

                    # Skip if hash unchanged on this chain
                    _progress("onchain", f"Updating on-chain hash on {chain_name}...")

                    from micromech.core.bridge import get_service_info
                    from micromech.management import MechLifecycle

                    svc_info = get_service_info(chain_name)
                    svc_key = svc_info.get("service_key")
                    if not svc_key:
                        _progress("onchain", f"No service key for {chain_name}, skipping")
                        continue

                    lc = MechLifecycle(self.config, chain_name)
                    tx = await asyncio.to_thread(
                        lc.update_metadata_onchain,
                        svc_key,
                        onchain_hash,
                    )
                    if tx:
                        result.chain_txs[chain_name] = tx
                        _progress("onchain", f"{chain_name}: tx {tx[:18]}...")
                    else:
                        _progress("onchain", f"{chain_name}: update failed (non-fatal)")

            # Step 5: Persist state in config
            fingerprints = {t["name"]: t["package_cid"] for t in tools if t.get("package_cid")}
            self.config.metadata_ipfs_cid = cid
            self.config.metadata_onchain_hash = onchain_hash
            self.config.metadata_fingerprints = fingerprints
            self.config.save()
            _progress("done", "Metadata published successfully")

        except Exception as e:
            logger.error("Metadata publish failed: {}", e)
            result.error = str(e)

        return result

    def publish_sync(
        self,
        service_key: str = "",
        chain_name: str = "",
        on_progress: Callable[[str, str], None] | None = None,
    ) -> MetadataResult:
        """Synchronous publish — for use inside threads (e.g. full_deploy).

        Uses requests.post for IPFS (no asyncio) to avoid event loop conflicts.
        Only updates on-chain for the specified chain (not all enabled chains).
        """
        import json as _json

        import requests as req_lib

        from micromech.core.constants import IPFS_API_URL
        from micromech.ipfs.client import compute_cid
        from micromech.ipfs.metadata import build_metadata, compute_onchain_hash

        result = MetadataResult()

        def _progress(step: str, msg: str) -> None:
            logger.info("[metadata-sync] {}: {}", step, msg)
            if on_progress:
                on_progress(step, msg)

        try:
            _progress("scan", "Scanning tool packages...")
            tools = self._scan_all()
            metadata = build_metadata(tools)
            result.metadata = metadata

            _progress("ipfs", "Pushing metadata to IPFS...")
            metadata_bytes = _json.dumps(
                metadata,
                separators=(",", ":"),
            ).encode("utf-8")
            try:
                resp = req_lib.post(
                    f"{IPFS_API_URL}/api/v0/add",
                    files={"file": ("metadata.json", metadata_bytes, "application/octet-stream")},
                    params={"pin": "true", "cid-version": "1"},
                    timeout=30,
                )
                resp.raise_for_status()
            except Exception as ipfs_err:
                _progress("ipfs", f"IPFS push failed (non-fatal): {ipfs_err}")

            cid = compute_cid(metadata_bytes)
            result.ipfs_cid = cid
            _progress("ipfs", f"CID: {cid}")

            onchain_hash = compute_onchain_hash(metadata)
            result.onchain_hash = onchain_hash

            if service_key and chain_name:
                _progress("onchain", f"Updating on-chain hash on {chain_name}...")
                from micromech.management import MechLifecycle

                lc = MechLifecycle(self.config, chain_name)
                tx = lc.update_metadata_onchain(service_key, onchain_hash)
                if tx:
                    result.chain_txs[chain_name] = tx
                    _progress("onchain", f"tx {tx[:18]}...")
                else:
                    _progress("onchain", f"Skipped (not available on {chain_name})")

            fingerprints = {t["name"]: t["package_cid"] for t in tools if t.get("package_cid")}
            self.config.metadata_ipfs_cid = cid
            self.config.metadata_onchain_hash = onchain_hash
            self.config.metadata_fingerprints = fingerprints
            self.config.save()
            _progress("done", "Metadata published")

        except Exception as e:
            logger.error("Metadata sync publish failed: {}", e)
            result.error = str(e)

        return result
