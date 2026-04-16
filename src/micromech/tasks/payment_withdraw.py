"""Mech payment withdrawal task.

Periodically checks accumulated xDAI in the mech marketplace balance tracker
and withdraws it to the multisig via processPaymentByMultisig(mech).

Flow:
  1. Read mech's paymentType from mech contract
  2. Look up balance tracker: marketplace.mapPaymentTypeBalanceTrackers(paymentType)
  3. Read mapMechBalances(mech) from balance tracker
  4. If balance >= threshold: call processPaymentByMultisig(mech) from multisig via Safe
  5. Notify

The mech earns xDAI for every delivered request (at maxDeliveryRate per delivery).
These earnings accumulate in the balance tracker until withdrawn.
"""

import asyncio
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from micromech.core.bridge import IwaBridge
    from micromech.core.config import MicromechConfig
    from micromech.tasks.notifications import NotificationService

# Minimal ABI for the mech marketplace balance tracker
_BT_ABI = [
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

# Minimal ABI fragments needed from marketplace and mech
_MARKETPLACE_ABI_FRAGMENT = [
    {
        "name": "mapPaymentTypeBalanceTrackers",
        "type": "function",
        "inputs": [{"name": "paymentType", "type": "bytes32"}],
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
    },
]
_MECH_ABI_FRAGMENT = [
    {
        "name": "paymentType",
        "type": "function",
        "inputs": [],
        "outputs": [{"type": "bytes32"}],
        "stateMutability": "view",
    },
]


def _get_balance_tracker_address(
    bridge: "IwaBridge", chain_name: str, mech_address: str, marketplace_address: str
) -> str | None:
    """Resolve balance tracker address for the mech's payment type."""
    web3 = bridge.web3
    mech = web3.eth.contract(
        address=web3.to_checksum_address(mech_address),
        abi=_MECH_ABI_FRAGMENT,
    )
    payment_type = mech.functions.paymentType().call()

    marketplace = web3.eth.contract(
        address=web3.to_checksum_address(marketplace_address),
        abi=_MARKETPLACE_ABI_FRAGMENT,
    )
    bt_addr = marketplace.functions.mapPaymentTypeBalanceTrackers(payment_type).call()

    zero = "0x" + "0" * 40
    if bt_addr == zero or bt_addr == web3.to_checksum_address(zero):
        logger.warning(
            "[{}] Balance tracker is zero address — no tracker for this payment type", chain_name
        )
        return None
    return web3.to_checksum_address(bt_addr)


def _get_pending_balance(bridge: "IwaBridge", bt_address: str, mech_address: str) -> float:
    """Return pending xDAI balance (in ether units) for the mech in the balance tracker."""
    web3 = bridge.web3
    bt = web3.eth.contract(
        address=web3.to_checksum_address(bt_address),
        abi=_BT_ABI,
    )
    raw = bt.functions.mapMechBalances(web3.to_checksum_address(mech_address)).call()
    return raw / 1e18


def _transfer_to_master(
    bridge: "IwaBridge",
    chain_name: str,
    multisig_address: str,
    amount_xdai: float,
) -> None:
    """Transfer native xDAI from the Safe to the master wallet.

    Sends a value-only Safe transaction (empty calldata) to master.
    """
    master = str(bridge.wallet.master_account.address)
    amount_wei = int(amount_xdai * 1e18)

    tx_hash = bridge.wallet.safe_service.execute_safe_transaction(
        safe_address_or_tag=multisig_address,
        to=master,
        value=amount_wei,
        chain_name=chain_name,
        data=b"",
    )
    tx_hash_str = tx_hash if isinstance(tx_hash, str) else tx_hash.hex()
    logger.info(
        "[{}] Transferred {:.6f} xDAI to master {}. TX: {}",
        chain_name,
        amount_xdai,
        master,
        tx_hash_str,
    )

    web3 = bridge.web3
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt["status"] != 1:
        logger.warning("[{}] xDAI transfer to master reverted: {}", chain_name, tx_hash_str)


def _withdraw(
    bridge: "IwaBridge",
    chain_name: str,
    bt_address: str,
    mech_address: str,
    multisig_address: str,
    balance: float = 0.0,
) -> float:
    """Call processPaymentByMultisig(mech) from the multisig via Safe.

    Returns the mechPayment amount in xDAI (ether units).
    Raises on failure.
    """
    web3 = bridge.web3
    bt = web3.eth.contract(
        address=web3.to_checksum_address(bt_address),
        abi=_BT_ABI,
    )
    fn_call = bt.functions.processPaymentByMultisig(web3.to_checksum_address(mech_address))
    calldata = fn_call.build_transaction({"from": web3.to_checksum_address(multisig_address)})[
        "data"
    ]

    tx_hash = bridge.wallet.safe_service.execute_safe_transaction(
        safe_address_or_tag=multisig_address,
        to=bt_address,
        value=0,
        chain_name=chain_name,
        data=calldata,
    )
    tx_hash_str = tx_hash if isinstance(tx_hash, str) else tx_hash.hex()
    logger.info("[{}] processPaymentByMultisig TX: {}", chain_name, tx_hash_str)

    # Wait for receipt and return mech payment amount
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt["status"] != 1:
        msg = f"[{chain_name}] processPaymentByMultisig reverted: {tx_hash_str}"
        raise RuntimeError(msg)

    # Return the pending balance that was withdrawn (reading from receipt logs is complex)
    return balance


async def payment_withdraw_task(
    bridges: "dict[str, IwaBridge]",
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Withdraw accumulated mech payments from the marketplace balance tracker."""
    threshold = config.payment_withdraw_threshold_xdai

    for chain_name, chain_config in config.enabled_chains.items():
        if not chain_config.mech_address:
            logger.debug("[{}] No mech_address configured, skipping payment withdraw", chain_name)
            continue

        bridge = bridges.get(chain_name)
        if bridge is None:
            logger.debug("[{}] No bridge for chain, skipping payment withdraw", chain_name)
            continue

        if not hasattr(bridge, "wallet") or not hasattr(bridge.wallet, "safe_service"):
            logger.debug("[{}] No safe_service on bridge, skipping payment withdraw", chain_name)
            continue

        try:
            bt_address = await asyncio.to_thread(
                _get_balance_tracker_address,
                bridge,
                chain_name,
                chain_config.mech_address,
                chain_config.marketplace_address,
            )
            if not bt_address:
                continue

            balance = await asyncio.to_thread(
                _get_pending_balance,
                bridge,
                bt_address,
                chain_config.mech_address,
            )
            logger.debug(
                "[{}] Pending mech payment in balance tracker: {:.6f} xDAI", chain_name, balance
            )

            if balance < threshold:
                logger.debug(
                    "[{}] Pending payment {:.6f} xDAI below threshold {:.4f} xDAI — skipping",
                    chain_name,
                    balance,
                    threshold,
                )
                continue

            # Get the multisig address from iwa service info
            from micromech.core.bridge import get_service_info

            svc_info = await asyncio.to_thread(get_service_info, chain_name)
            multisig = svc_info.get("multisig_address")
            if not multisig:
                logger.warning("[{}] No multisig_address — cannot withdraw payment", chain_name)
                continue

            logger.info(
                "[{}] Withdrawing {:.6f} xDAI mech payment to multisig {}",
                chain_name,
                balance,
                multisig,
            )

            await asyncio.to_thread(
                _withdraw,
                bridge,
                chain_name,
                bt_address,
                chain_config.mech_address,
                multisig,
                balance,
            )

            logger.info("[{}] Payment withdraw complete: {:.6f} xDAI", chain_name, balance)

            # Transfer the xDAI from Safe to master immediately after
            try:
                await asyncio.to_thread(
                    _transfer_to_master,
                    bridge,
                    chain_name,
                    multisig,
                    balance,
                )
                master = str(bridge.wallet.master_account.address)
                await notification_service.send(
                    "Mech Payment Withdrawn",
                    (
                        f"Chain: {chain_name}\nAmount: {balance:.6f} xDAI"
                        f"\nTransferred to master: {master}"
                    ),
                )
            except Exception as e:
                logger.error("[{}] xDAI transfer to master failed: {}", chain_name, e)
                await notification_service.send(
                    "Mech Payment Withdrawn",
                    (
                        f"Chain: {chain_name}\nAmount: {balance:.6f} xDAI"
                        f"\nTo Safe: {multisig}"
                        f"\nWARNING: transfer to master failed: {e}"
                    ),
                )

        except Exception as e:
            logger.error("[{}] Payment withdraw task error: {}", chain_name, e)
