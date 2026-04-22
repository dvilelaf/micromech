"""Address book for human-readable Ethereum address substitution in logs.

Two layers:
- Static: well-known external contracts (hardcoded here).
- Dynamic: wallet tags loaded at startup via register_address().

The loguru patcher replaces every known address in every log message
automatically, without touching individual log call sites.
"""

import re
from typing import Optional

# Well-known external contracts on Gnosis chain.
# Keys are lowercase (no checksum) for case-insensitive lookup.
_STATIC: dict[str, str] = {
    # MechMarketplace v2
    "0x735faab1c4ec41128c367afb5c3bac73509f70bb": "MECH_MARKETPLACE",
    # Default Valory priority mech (marketplace v2)
    "0xc05e7412439bd7e91730a6880e18d5d5873f632c": "VALORY_MECH",
    # Custom echo mech (PROTEUS) deployed on Gnosis
    "0x33ca1e117c4254b2ee8cd7ef1621739431a37396": "PROTEUS_MECH",
}

# Wallet tags and other runtime-registered addresses (populated at startup).
_DYNAMIC: dict[str, str] = {}

# Matches any 0x-prefixed 40-hex-char Ethereum address.
_ADDR_RE = re.compile(r"0x[0-9a-fA-F]{40}")


def register_address(address: str, name: str) -> None:
    """Register a known address → name mapping (e.g. wallet tags at startup)."""
    _DYNAMIC[address.lower()] = name


def fmt_addr(address: str) -> str:
    """Return human-readable name for address, or the address itself."""
    key = address.lower()
    return _DYNAMIC.get(key) or _STATIC.get(key) or address


def _substitute(text: str) -> str:
    """Replace all known addresses in text with their names."""

    def _replacer(m: re.Match) -> str:
        name = _DYNAMIC.get(m.group(0).lower()) or _STATIC.get(m.group(0).lower())
        return name if name else m.group(0)

    return _ADDR_RE.sub(_replacer, text)


def address_book_patcher(record: dict) -> None:
    """Loguru patcher — replace known addresses in all log messages."""
    record["message"] = _substitute(str(record["message"]))


def load_wallet_tags(wallet: Optional[object]) -> None:
    """Load all wallet account tags into the dynamic address book.

    Call this once after the wallet is initialized at startup.
    """
    if wallet is None:
        return
    try:
        accounts = wallet.account_service.get_account_data()  # type: ignore[union-attr]
        for addr, account in accounts.items():
            tag = getattr(account, "tag", None)
            if tag:
                register_address(str(addr), tag)
    except Exception:
        pass
