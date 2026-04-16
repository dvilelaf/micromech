"""Mech payment withdrawal task.

Periodically checks accumulated xDAI in the mech marketplace balance tracker
and withdraws it to the multisig via processPaymentByMultisig(mech).

Flow:
  1. Read mech's paymentType from mech contract
  2. Look up balance tracker: marketplace.mapPaymentTypeBalanceTrackers(paymentType)
  3. Read mapMechBalances(mech) from balance tracker
  4. If balance >= threshold: call processPaymentByMultisig(mech) from multisig via Safe
  5. Drain xDAI from mech to Safe via mech.exec()
  6. Transfer xDAI from Safe to master wallet
  7. Notify

The mech earns xDAI for every delivered request (at maxDeliveryRate per delivery).
These earnings accumulate in the balance tracker until withdrawn.

NOTE: processPaymentByMultisig() sends xDAI to the mech contract (not the Safe).
The Safe (as mech operator) must call mech.exec() to pull the funds to the Safe
before the standard Safe→master transfer can happen.
"""

import asyncio
from typing import TYPE_CHECKING

from loguru import logger

from micromech.core.marketplace import (
    BALANCE_TRACKER_ABI,
    MECH_EXEC_ABI,
    get_balance_tracker_address,
    get_pending_balance,
)

if TYPE_CHECKING:
    from micromech.core.bridge import IwaBridge
    from micromech.core.config import MicromechConfig
    from micromech.tasks.notifications import NotificationService


def _transfer_to_master(
    bridge: "IwaBridge",
    chain_name: str,
    multisig_address: str,
    amount_wei: int,
) -> None:
    """Transfer native xDAI from the Safe to the master wallet.

    Uses wallet.send() (iwa's managed transfer pipeline) to ensure
    the Safe transaction is signed correctly. Direct execute_safe_transaction
    calls bypass the pipeline and can cause GS013 (Invalid signatures).
    """
    master = str(bridge.wallet.master_account.address)

    tx_hash = bridge.wallet.send(
        from_address_or_tag=multisig_address,
        to_address_or_tag=master,
        amount_wei=amount_wei,
        chain_name=chain_name,
    )
    if not tx_hash:
        raise RuntimeError(
            f"[{chain_name}] Safe→master transfer returned no tx hash"
        )
    tx_hash_str = tx_hash if isinstance(tx_hash, str) else tx_hash.hex()
    logger.info(
        "[{}] Transferred {:.6f} xDAI to master {}. TX: {}",
        chain_name,
        amount_wei / 1e18,
        master,
        tx_hash_str,
    )

    web3 = bridge.web3
    receipt = bridge.with_retry(
        lambda: web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    )
    if receipt["status"] != 1:
        logger.warning(
            "[{}] xDAI transfer to master reverted: {}",
            chain_name,
            tx_hash_str,
        )


def _withdraw(
    bridge: "IwaBridge",
    chain_name: str,
    bt_address: str,
    mech_address: str,
    multisig_address: str,
) -> None:
    """Call processPaymentByMultisig(mech) from the multisig via Safe.

    xDAI goes to the mech contract (not the Safe) — caller must follow up
    with _drain_mech_to_safe() to pull the funds back to the Safe.
    """
    web3 = bridge.web3
    bt = web3.eth.contract(
        address=web3.to_checksum_address(bt_address),
        abi=BALANCE_TRACKER_ABI,
    )
    fn_call = bt.functions.processPaymentByMultisig(
        web3.to_checksum_address(mech_address)
    )
    calldata = fn_call.build_transaction(
        {"from": web3.to_checksum_address(multisig_address)}
    )["data"]

    tx_hash = bridge.wallet.safe_service.execute_safe_transaction(
        safe_address_or_tag=multisig_address,
        to=bt_address,
        value=0,
        chain_name=chain_name,
        data=calldata,
    )
    tx_hash_str = tx_hash if isinstance(tx_hash, str) else tx_hash.hex()
    logger.info("[{}] processPaymentByMultisig TX: {}", chain_name, tx_hash_str)

    receipt = bridge.with_retry(
        lambda: web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    )
    if receipt["status"] != 1:
        msg = (
            f"[{chain_name}] processPaymentByMultisig reverted: {tx_hash_str}"
        )
        raise RuntimeError(msg)


def _drain_mech_to_safe(
    bridge: "IwaBridge",
    chain_name: str,
    mech_address: str,
    multisig_address: str,
    amount_wei: int,
) -> None:
    """Pull xDAI from the mech contract to the Safe via mech.exec().

    processPaymentByMultisig() sends xDAI to the mech contract (not the Safe).
    Since the Safe is the mech operator, it can call mech.exec() to transfer
    the native xDAI from the mech to the Safe.
    """
    web3 = bridge.web3
    mech = web3.eth.contract(
        address=web3.to_checksum_address(mech_address),
        abi=MECH_EXEC_ABI,
    )
    fn_call = mech.functions.exec(
        web3.to_checksum_address(multisig_address),
        amount_wei,
        b"",
        0,        # operation = Call
        100_000,  # txGas — 21k for native transfer + margin for Safe fallback/receive handler
    )
    calldata = fn_call.build_transaction(
        {"from": web3.to_checksum_address(multisig_address)}
    )["data"]

    tx_hash = bridge.wallet.safe_service.execute_safe_transaction(
        safe_address_or_tag=multisig_address,
        to=mech_address,
        value=0,
        chain_name=chain_name,
        data=calldata,
    )
    tx_hash_str = tx_hash if isinstance(tx_hash, str) else tx_hash.hex()
    logger.info("[{}] mech.exec drain TX: {}", chain_name, tx_hash_str)

    receipt = bridge.with_retry(
        lambda: web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    )
    if receipt["status"] != 1:
        raise RuntimeError(
            f"[{chain_name}] mech.exec drain reverted: {tx_hash_str}"
        )


async def payment_withdraw_task(
    bridges: "dict[str, IwaBridge]",
    notification_service: "NotificationService",
    config: "MicromechConfig",
) -> None:
    """Withdraw accumulated mech payments from the marketplace balance tracker."""
    threshold = config.payment_withdraw_threshold_xdai

    for chain_name, chain_config in config.enabled_chains.items():
        if not chain_config.mech_address:
            logger.debug(
                "[{}] No mech_address configured, skipping payment withdraw",
                chain_name,
            )
            continue

        bridge = bridges.get(chain_name)
        if bridge is None:
            logger.debug(
                "[{}] No bridge for chain, skipping payment withdraw",
                chain_name,
            )
            continue

        if not hasattr(bridge, "wallet") or not hasattr(
            bridge.wallet, "safe_service"
        ):
            logger.debug(
                "[{}] No safe_service on bridge, skipping payment withdraw",
                chain_name,
            )
            continue

        try:
            bt_address = await asyncio.to_thread(
                get_balance_tracker_address,
                bridge,
                chain_name,
                chain_config.mech_address,
                chain_config.marketplace_address,
            )
            if not bt_address:
                continue

            balance = await asyncio.to_thread(
                get_pending_balance,
                bridge,
                bt_address,
                chain_config.mech_address,
            )
            logger.debug(
                "[{}] Pending mech payment in balance tracker: {:.6f} xDAI",
                chain_name,
                balance,
            )

            if balance < threshold:
                logger.debug(
                    "[{}] Pending payment {:.6f} xDAI below threshold"
                    " {:.4f} xDAI — skipping",
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
                logger.warning(
                    "[{}] No multisig_address — cannot withdraw payment",
                    chain_name,
                )
                continue

            logger.info(
                "[{}] Withdrawing {:.6f} xDAI mech payment to multisig {}",
                chain_name,
                balance,
                multisig,
            )

            # Step 1: processPaymentByMultisig → xDAI lands in mech contract
            await asyncio.to_thread(
                _withdraw,
                bridge,
                chain_name,
                bt_address,
                chain_config.mech_address,
                multisig,
            )

            # Read actual mech balance in exact wei — avoids float round-trip
            # precision loss and accounts for marketplace fees or concurrent txs.
            web3 = bridge.web3
            mech_actual_wei = await asyncio.to_thread(
                bridge.with_retry,
                lambda: web3.eth.get_balance(
                    web3.to_checksum_address(chain_config.mech_address)
                ),
            )
            logger.debug(
                "[{}] Mech actual balance for drain: {} wei",
                chain_name,
                mech_actual_wei,
            )

            # Step 2: mech.exec → pull xDAI from mech to Safe
            await asyncio.to_thread(
                _drain_mech_to_safe,
                bridge,
                chain_name,
                chain_config.mech_address,
                multisig,
                mech_actual_wei,
            )

            mech_actual_xdai = mech_actual_wei / 1e18
            logger.info(
                "[{}] Payment withdraw complete: {:.6f} xDAI drained from mech",
                chain_name,
                mech_actual_xdai,
            )

            # Step 3: transfer exactly what was drained from mech to master.
            # Safe keeps its own pre-existing xDAI; only the mech payment is forwarded.
            try:
                await asyncio.to_thread(
                    _transfer_to_master,
                    bridge,
                    chain_name,
                    multisig,
                    mech_actual_wei,
                )
                master = str(bridge.wallet.master_account.address)
                await notification_service.send(
                    "Mech Payment Withdrawn",
                    (
                        f"Chain: {chain_name}\nAmount: {mech_actual_xdai:.6f} xDAI"
                        f"\nTransferred to master: {master}"
                    ),
                )
            except Exception as e:
                logger.error(
                    "[{}] xDAI transfer to master failed: {}", chain_name, e
                )
                await notification_service.send(
                    "Mech Payment Withdrawn",
                    (
                        f"Chain: {chain_name}\nAmount: {mech_actual_xdai:.6f} xDAI"
                        f"\nTo Safe: {multisig}"
                        f"\nWARNING: transfer to master failed: {e}"
                    ),
                )

        except Exception as e:
            logger.error(
                "[{}] Payment withdraw task error: {}", chain_name, e
            )
