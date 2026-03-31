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

# Legacy Gnosis constants (backward compat — prefer CHAIN_DEFAULTS)
MECH_MARKETPLACE_ADDRESS = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"
MECH_FACTORY_FIXED_PRICE_NATIVE = "0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF"
SUPPLY_STAKING_ALPHA = "0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44"

# Per-chain contract addresses (marketplace proxy, factory native, supply staking)
CHAIN_DEFAULTS: dict[str, dict[str, str]] = {
    "gnosis": {
        "marketplace": "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB",
        "factory": "0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF",
        "staking": "0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44",
    },
    "base": {
        "marketplace": "0xf24eE42edA0fc9b33B7D41B06Ee8ccD2Ef7C5020",
        "factory": "0x2E008211f34b25A7d7c102403c6C2C3B665a1abe",
        "staking": "0xB14Cd66c6c601230EA79fa7Cc072E5E0C2F3A756",
    },
    "ethereum": {
        "marketplace": "0x3d6494CE09a9f40c0B5a92BdBD7c7A9b0e3912b1",
        "factory": "0x3515a36AF270070635Fa3E957e006aaF6078e658",
        "staking": "0x5A40e2661b3EE672e945445F885F975a51A6c461",
    },
    "polygon": {
        "marketplace": "0x343F2B005cF6D70bA610CD9F1F1927049414B582",
        "factory": "0x87f89F94033305791B6269AE2F9cF4e09983E56e",
        "staking": "0x3aE11e2dD9a055AF3DA61ae2E36515D1612d7D93",
    },
    "optimism": {
        "marketplace": "0x46C0D07F55d4F9B5Eed2Fc9680B5953e5fd7b461",
        "factory": "0xf76953444C35F1FcE2F6CA1b167173357d3F5C17",
        "staking": "0xBb375c8d8517e6956AF7044FE676f2100505624f",
    },
    "arbitrum": {
        "marketplace": "0xf76953444C35F1FcE2F6CA1b167173357d3F5C17",
        "factory": "0x4Cd816ce806FF1003ee459158A093F02AbF042a8",
        "staking": "0x646ECbe31dF12D17A949d65764187408F6BB095d",
    },
    "celo": {
        "marketplace": "0x17d96ba4532fe91809326092fE4D5606A7B7a0d8",
        "factory": "0xDd1252c5a75be568B5E6e50bA542680b38dbd68f",
        "staking": "0x6CC3A0D25e2Ac7D8ff119ef92D5523259c6Dc821",
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
