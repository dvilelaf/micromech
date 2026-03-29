"""Default constants for micromech."""

import re
from typing import Final

# Ethereum address validation
ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

# Gnosis chain defaults
DEFAULT_CHAIN = "gnosis"
GNOSIS_BLOCK_TIME_SECONDS = 5

# Marketplace contract (Gnosis)
MECH_MARKETPLACE_ADDRESS = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"

# Mech factory (fixed price native, Gnosis)
MECH_FACTORY_FIXED_PRICE_NATIVE = "0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF"

# Supply staking contracts (Gnosis)
SUPPLY_STAKING_ALPHA = "0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44"

# Default delivery rate (0.01 xDAI)
DEFAULT_DELIVERY_RATE = 10_000_000_000_000_000

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
