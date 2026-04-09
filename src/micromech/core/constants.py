"""Default constants for micromech."""

import re
from pathlib import Path
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

# Runtime defaults (not user-configurable — CLI args override host/port)
DEFAULT_HOST = "0.0.0.0"  # noqa: S104 — Docker handles external access via port mapping
DEFAULT_PORT = 8090
DEFAULT_MAX_CONCURRENT = 10
DEFAULT_REQUEST_TIMEOUT = 300  # 5 minutes
DEFAULT_EVENT_POLL_INTERVAL = 15  # seconds
DEFAULT_EVENT_LOOKBACK_BLOCKS = 1000
DEFAULT_DELIVERY_BATCH_SIZE = 10
DEFAULT_DELIVERY_INTERVAL = 10  # seconds

# Persistence
DEFAULT_CONFIG_DIR = Path("data")
CUSTOM_TOOLS_DIR = DEFAULT_CONFIG_DIR / "tools"
DB_NAME = "micromech.db"
DB_PATH = DEFAULT_CONFIG_DIR / DB_NAME
CLEANUP_DAYS = 30

# IPFS (Autonolas infrastructure)
IPFS_GATEWAY = "https://gateway.autonolas.tech/ipfs/"
IPFS_API_URL = "https://registry.autonolas.tech"
IPFS_TIMEOUT = 30

# Tasks internals (not user-configurable)
CHECKPOINT_GRACE_PERIOD_SECONDS = 120
HEALTH_INTERVAL_SECONDS = 55
TELEGRAM_RATE_LIMIT_SECONDS = 2

# LLM defaults (used by the llm_tool, not global config)
DEFAULT_LLM_MAX_TOKENS = 256
DEFAULT_LLM_CONTEXT_SIZE = 2048

# Available model presets: {preset_name: (repo_id, gguf_filename)}
LLM_MODEL_PRESETS: dict[str, tuple[str, str]] = {
    "qwen": (
        "Qwen/Qwen2.5-0.5B-Instruct-GGUF",
        "qwen2.5-0.5b-instruct-q4_k_m.gguf",
    ),
    "gemma4": (
        "unsloth/gemma-4-E2B-it-GGUF",
        "gemma-4-E2B-it-Q4_K_M.gguf",
    ),
}

DEFAULT_LLM_PRESET = "qwen"
DEFAULT_LLM_MODEL = LLM_MODEL_PRESETS[DEFAULT_LLM_PRESET][0]
DEFAULT_LLM_FILE = LLM_MODEL_PRESETS[DEFAULT_LLM_PRESET][1]

# Request statuses (Final for mypy Literal compatibility)
STATUS_PENDING: Final = "pending"
STATUS_EXECUTING: Final = "executing"
STATUS_EXECUTED: Final = "executed"
STATUS_DELIVERED: Final = "delivered"
STATUS_FAILED: Final = "failed"

# Delivery methods
DELIVERY_MARKETPLACE: Final = "marketplace"
DELIVERY_LEGACY: Final = "legacy"

# Minimum funding requirements per chain (native token in wei, OLAS in whole units)
# These cover gas for the full lifecycle + some buffer
MIN_NATIVE_WEI: dict[str, int] = {
    "gnosis": 100_000_000_000_000_000,  # 0.1 xDAI
    "base": 1_000_000_000_000_000,  # 0.001 ETH
    "ethereum": 10_000_000_000_000_000,  # 0.01 ETH
    "polygon": 1_000_000_000_000_000_000,  # 1 POL
    "optimism": 1_000_000_000_000_000,  # 0.001 ETH
    "arbitrum": 1_000_000_000_000_000,  # 0.001 ETH
    "celo": 1_000_000_000_000_000_000,  # 1 CELO
}
MIN_OLAS_WHOLE: int = 10_000  # OLAS for staking bond (Supply Alpha)
