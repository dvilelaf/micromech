"""Default constants for micromech."""

import re
from typing import Final, Optional

# Ethereum address validation
ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def validate_eth_address(v: Optional[str]) -> Optional[str]:
    """Validate an Ethereum address. Returns the value or raises ValueError."""
    if v is None:
        return v
    if not ETH_ADDRESS_RE.match(v):
        msg = f"Invalid Ethereum address: {v}"
        raise ValueError(msg)
    return v


# Default chain
DEFAULT_CHAIN = "gnosis"

# Default delivery rate (0.01 native token)
DEFAULT_DELIVERY_RATE = 10_000_000_000_000_000

# Import chain contract addresses from iwa (single source of truth)
try:
    from iwa.plugins.olas.constants import MECH_CONTRACTS as CHAIN_DEFAULTS
except ImportError:
    # Fallback when iwa is not installed — hardcoded Gnosis defaults only
    CHAIN_DEFAULTS: dict[str, dict[str, str]] = {
        "gnosis": {
            "marketplace": "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB",
            "factory": "0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF",
            "staking": "0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44",
        },
    }

# Runtime defaults
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
DEFAULT_MAX_CONCURRENT = 10
DEFAULT_REQUEST_TIMEOUT = 300  # 5 minutes
DEFAULT_EVENT_POLL_INTERVAL = 15  # seconds
DEFAULT_EVENT_LOOKBACK_BLOCKS = 1000
DEFAULT_DELIVERY_BATCH_SIZE = 10
DEFAULT_DELIVERY_INTERVAL = 10  # seconds

# Persistence
DEFAULT_DB_NAME = "micromech.db"
DEFAULT_CLEANUP_DAYS = 30

# LLM defaults
DEFAULT_LLM_MODEL = "Qwen/Qwen2.5-0.5B-Instruct-GGUF"
DEFAULT_LLM_FILE = "qwen2.5-0.5b-instruct-q4_k_m.gguf"
DEFAULT_LLM_MAX_TOKENS = 256
DEFAULT_LLM_CONTEXT_SIZE = 2048

# IPFS
IPFS_GATEWAY = "https://gateway.autonolas.tech/ipfs/"

# Request statuses (Final for mypy Literal compatibility)
STATUS_PENDING: Final = "pending"
STATUS_EXECUTING: Final = "executing"
STATUS_EXECUTED: Final = "executed"
STATUS_DELIVERED: Final = "delivered"
STATUS_FAILED: Final = "failed"

# Delivery methods
DELIVERY_MARKETPLACE: Final = "marketplace"
DELIVERY_LEGACY: Final = "legacy"
