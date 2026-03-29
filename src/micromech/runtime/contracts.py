"""Contract helpers — ABI loading and common contract interactions."""

import json
from pathlib import Path
from typing import Optional

from loguru import logger

# Try to load ABIs from iwa if available, otherwise use bundled minimal ABIs
_IWA_ABI_PATH: Optional[Path] = None
try:
    from iwa.plugins.olas.contracts.base import OLAS_ABI_PATH

    _IWA_ABI_PATH = OLAS_ABI_PATH
except ImportError:
    pass

# Minimal ABI for deliverToMarketplace (used when iwa ABIs not available)
MECH_DELIVER_ABI = [
    {
        "inputs": [
            {"internalType": "bytes32[]", "name": "requestIds", "type": "bytes32[]"},
            {"internalType": "bytes[]", "name": "datas", "type": "bytes[]"},
        ],
        "name": "deliverToMarketplace",
        "outputs": [
            {"internalType": "bool[]", "name": "deliveredRequests", "type": "bool[]"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

# Minimal ABI for marketplace request
MARKETPLACE_REQUEST_ABI = [
    {
        "inputs": [
            {"internalType": "bytes", "name": "requestData", "type": "bytes"},
            {"internalType": "uint256", "name": "maxDeliveryRate", "type": "uint256"},
            {"internalType": "bytes32", "name": "paymentType", "type": "bytes32"},
            {"internalType": "address", "name": "priorityMech", "type": "address"},
            {"internalType": "uint256", "name": "responseTimeout", "type": "uint256"},
            {"internalType": "bytes", "name": "paymentData", "type": "bytes"},
        ],
        "name": "request",
        "outputs": [
            {"internalType": "bytes32", "name": "requestId", "type": "bytes32"},
        ],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "priorityMech", "type": "address"},
            {"indexed": True, "name": "requester", "type": "address"},
            {"indexed": False, "name": "numRequests", "type": "uint256"},
            {"indexed": False, "name": "requestIds", "type": "bytes32[]"},
            {"indexed": False, "name": "requestDatas", "type": "bytes[]"},
        ],
        "name": "MarketplaceRequest",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "mech", "type": "address"},
            {"indexed": True, "name": "mechServiceMultisig", "type": "address"},
            {"indexed": False, "name": "requestId", "type": "bytes32"},
            {"indexed": False, "name": "deliveryRate", "type": "uint256"},
            {"indexed": False, "name": "requestData", "type": "bytes"},
            {"indexed": False, "name": "deliveryData", "type": "bytes"},
        ],
        "name": "Deliver",
        "type": "event",
    },
]


def load_mech_abi() -> list[dict]:
    """Load the mech contract ABI (mech_new.json) for deliverToMarketplace."""
    if _IWA_ABI_PATH:
        abi_file = _IWA_ABI_PATH / "mech_new.json"
        if abi_file.exists():
            return json.loads(abi_file.read_text())
    logger.debug("Using bundled minimal mech delivery ABI")
    return MECH_DELIVER_ABI


def load_marketplace_abi() -> list[dict]:
    """Load the marketplace contract ABI."""
    if _IWA_ABI_PATH:
        abi_file = _IWA_ABI_PATH / "mech_marketplace.json"
        if abi_file.exists():
            return json.loads(abi_file.read_text())
    logger.debug("Using bundled minimal marketplace ABI")
    return MARKETPLACE_REQUEST_ABI


# ComplementaryServiceMetadata contract — stores mech metadata hash on-chain
COMPLEMENTARY_SERVICE_METADATA_ADDRESS = {
    "gnosis": "0x0598081D48FB80B0A7E52FAD2905AE9beCd6fC69",
}

COMPLEMENTARY_SERVICE_METADATA_ABI = [
    {
        "inputs": [
            {"internalType": "uint256", "name": "serviceId", "type": "uint256"},
            {"internalType": "bytes32", "name": "hash", "type": "bytes32"},
        ],
        "name": "changeHash",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint256", "name": "serviceId", "type": "uint256"}],
        "name": "tokenURI",
        "outputs": [{"internalType": "string", "name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
]
