"""Marketplace balance tracker utilities.

Shared by payment_withdraw (to withdraw) and status command (to display).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.bridge import IwaBridge

# Minimal ABI for the mech marketplace balance tracker.
# Last verified against Gnosis contracts: 2026-04-16.
# If contract interfaces change, update these ABIs accordingly.
BALANCE_TRACKER_ABI = [
    {
        "name": "mapMechBalances",
        "type": "function",
        "inputs": [{"name": "mech", "type": "address"}],
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "name": "processPaymentByMultisig",
        "type": "function",
        "inputs": [{"name": "mech", "type": "address"}],
        "outputs": [
            {"name": "mechPayment", "type": "uint256"},
            {"name": "marketplaceFee", "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
    },
]

MARKETPLACE_ABI_FRAGMENT = [
    {
        "name": "mapPaymentTypeBalanceTrackers",
        "type": "function",
        "inputs": [{"name": "paymentType", "type": "bytes32"}],
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
    },
]

MECH_ABI_FRAGMENT = [
    {
        "name": "paymentType",
        "type": "function",
        "inputs": [],
        "outputs": [{"type": "bytes32"}],
        "stateMutability": "view",
    },
]


def get_balance_tracker_address(
    bridge: IwaBridge,
    chain_name: str,
    mech_address: str,
    marketplace_address: str,
) -> str | None:
    """Resolve balance tracker address for the mech's payment type."""

    def _fetch() -> str:
        web3 = bridge.web3
        mech = web3.eth.contract(
            address=web3.to_checksum_address(mech_address),
            abi=MECH_ABI_FRAGMENT,
        )
        payment_type = mech.functions.paymentType().call()
        mp = web3.eth.contract(
            address=web3.to_checksum_address(marketplace_address),
            abi=MARKETPLACE_ABI_FRAGMENT,
        )
        return mp.functions.mapPaymentTypeBalanceTrackers(
            payment_type
        ).call()

    try:
        bt_addr = bridge.with_retry(_fetch)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "[{}] Failed to resolve balance tracker: {}", chain_name, e
        )
        return None

    web3 = bridge.web3
    zero = "0x" + "0" * 40
    if bt_addr == zero or bt_addr == web3.to_checksum_address(zero):
        logger.warning(
            "[{}] Balance tracker is zero address — no tracker for this"
            " payment type",
            chain_name,
        )
        return None
    return web3.to_checksum_address(bt_addr)


def get_pending_balance(
    bridge: IwaBridge, bt_address: str, mech_address: str
) -> float:
    """Return pending xDAI balance (ether units) for the mech."""

    def _fetch() -> int:
        web3 = bridge.web3
        bt = web3.eth.contract(
            address=web3.to_checksum_address(bt_address),
            abi=BALANCE_TRACKER_ABI,
        )
        return bt.functions.mapMechBalances(
            web3.to_checksum_address(mech_address)
        ).call()

    try:
        raw = bridge.with_retry(_fetch)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Failed to fetch pending balance for {}: {}", mech_address, e
        )
        return 0.0
    return raw / 1e18
