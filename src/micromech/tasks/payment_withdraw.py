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

from micromech.core.locks import get_safe_lock
from micromech.core.marketplace import (
    BALANCE_TRACKER_ABI,
    MECH_EXEC_ABI,
    get_balance_tracker_address,
    get_pending_balance,
)
from micromech.runtime.delivery import _sanitize_error

if TYPE_CHECKING:
    from micromech.core.bridge import IwaBridge
    from micromech.core.config import MicromechConfig
    from micromech.tasks.notifications import NotificationService


async def _notify_withdraw_failure(
    notification_service: "NotificationService",
    chain_name: str,
    error: Exception,
    *,
    pending_xdai: float | None = None,
    mech_balance_wei: int | None = None,
    stage: str = "unknown",
) -> None:
    """Notify operator that the payment withdraw task failed.

    A failure after processPaymentByMultisig can leave xDAI in the mech
    contract while the balance tracker shows low pending payments. That state
    requires operator visibility, otherwise /status looks falsely reassuring.
    """
    lines = [
        f"Chain: {chain_name}",
        f"Stage: {stage}",
    ]
    if pending_xdai is not None:
        lines.append(f"Pending before attempt: {pending_xdai:.6f} xDAI")
    if mech_balance_wei is not None:
        lines.append(f"Mech contract balance: {mech_balance_wei / 1e18:.6f} xDAI")
    lines.extend(
        [
            f"Error: {_sanitize_error(error)}",
            "Action: check mech, Safe and balance-tracker balances before assuming payments are missing.",
        ]
    )
    try:
        await notification_service.send(
            "Mech Payment Withdraw Failed",
            "\n".join(lines),
            level="error",
        )
    except Exception as notify_error:
        logger.error(
            "[{}] Payment withdraw failure notification failed: {}",
            chain_name,
            _sanitize_error(notify_error),
        )


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
        raise RuntimeError(
            f"[{chain_name}] xDAI transfer to master reverted: {tx_hash_str}"
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
            balance: float | None = None
            mech_actual_wei: int | None = None
            pending_read_error: Exception | None = None
            stage = "initializing"
            # Also inspect the mech contract itself. If a previous run failed
            # after processPaymentByMultisig(), the balance tracker will show
            # low pending payments while xDAI is already sitting on the mech.
            web3 = bridge.web3
            stage = "read existing mech balance"
            existing_mech_wei = await asyncio.to_thread(
                bridge.with_retry,
                lambda: web3.eth.get_balance(
                    web3.to_checksum_address(chain_config.mech_address)
                ),
            )
            existing_mech_xdai = existing_mech_wei / 1e18
            logger.debug(
                "[{}] Existing mech contract balance: {:.6f} xDAI",
                chain_name,
                existing_mech_xdai,
            )

            stage = "resolve balance tracker"
            bt_address = await asyncio.to_thread(
                get_balance_tracker_address,
                bridge,
                chain_name,
                chain_config.mech_address,
                chain_config.marketplace_address,
                raise_on_error=True,
            )
            if bt_address:
                stage = "read pending balance"
                try:
                    balance = await asyncio.to_thread(
                        get_pending_balance,
                        bridge,
                        bt_address,
                        chain_config.mech_address,
                        raise_on_error=True,
                    )
                except Exception as e:
                    if existing_mech_xdai < threshold:
                        raise
                    pending_read_error = e
                    logger.warning(
                        "[{}] Pending balance read failed, but mech already holds"
                        " {:.6f} xDAI; proceeding with stranded drain: {}",
                        chain_name,
                        existing_mech_xdai,
                        _sanitize_error(e),
                    )
                stage = "checking threshold"
                if balance is not None:
                    logger.debug(
                        "[{}] Pending mech payment in balance tracker: {:.6f} xDAI",
                        chain_name,
                        balance,
                    )
            elif existing_mech_xdai < threshold:
                logger.debug(
                    "[{}] Balance tracker unavailable and mech balance {:.6f} xDAI below"
                    " threshold {:.4f} xDAI — skipping",
                    chain_name,
                    existing_mech_xdai,
                    threshold,
                )
                continue

            if (balance or 0.0) < threshold and existing_mech_xdai < threshold:
                logger.debug(
                    "[{}] Pending payment {:.6f} xDAI and mech balance {:.6f} xDAI below threshold"
                    " {:.4f} xDAI — skipping",
                    chain_name,
                    balance or 0.0,
                    existing_mech_xdai,
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
                "[{}] Withdrawing {:.6f} xDAI mech payment to multisig {}"
                " (existing mech balance {:.6f} xDAI)",
                chain_name,
                balance or 0.0,
                multisig,
                existing_mech_xdai,
            )

            # All three steps submit Safe TXs — hold the per-Safe lock so
            # delivery workers cannot submit concurrent TXs during withdrawal.
            transfer_error: Exception | None = None
            async with get_safe_lock(multisig):
                if (balance or 0.0) >= threshold:
                    if not bt_address:
                        raise RuntimeError(
                            f"[{chain_name}] Cannot process pending payments without balance tracker"
                        )
                    # Step 1: processPaymentByMultisig → xDAI lands in mech contract
                    stage = "processPaymentByMultisig"
                    await asyncio.to_thread(
                        _withdraw,
                        bridge,
                        chain_name,
                        bt_address,
                        chain_config.mech_address,
                        multisig,
                    )
                else:
                    logger.info(
                        "[{}] Balance tracker below threshold but mech holds {:.6f} xDAI;"
                        " draining stranded payment",
                        chain_name,
                        existing_mech_xdai,
                    )

                # Read actual mech balance in exact wei — avoids float round-trip
                # precision loss and accounts for marketplace fees or concurrent txs.
                # Why inside the lock: processPaymentByMultisig() just deposited
                # xDAI into the mech. Reading under the lock ensures we drain
                # exactly what was deposited, without racing against another
                # consumer that could also drain the mech between _withdraw and
                # _drain_mech_to_safe.
                stage = "read mech balance"
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
                assert mech_actual_wei is not None

                # Step 2: mech.exec → pull xDAI from mech to Safe
                stage = "mech.exec drain"
                await asyncio.to_thread(
                    _drain_mech_to_safe,
                    bridge,
                    chain_name,
                    chain_config.mech_address,
                    multisig,
                    mech_actual_wei,
                )

                # Step 3: transfer exactly what was drained from mech to master.
                # Safe keeps its own pre-existing xDAI; only the mech payment forwarded.
                stage = "safe-to-master transfer"
                try:
                    await asyncio.to_thread(
                        _transfer_to_master,
                        bridge,
                        chain_name,
                        multisig,
                        mech_actual_wei,
                    )
                except Exception as e:
                    logger.error(
                        "[{}] xDAI transfer to master failed: {}", chain_name, _sanitize_error(e)
                    )
                    transfer_error = e

            mech_actual_xdai = mech_actual_wei / 1e18
            logger.info(
                "[{}] Payment withdraw complete: {:.6f} xDAI drained from mech",
                chain_name,
                mech_actual_xdai,
            )

            if transfer_error is None:
                master = str(bridge.wallet.master_account.address)
                await notification_service.send(
                    "Mech Payment Withdrawn",
                    (
                        f"Chain: {chain_name}\nAmount: {mech_actual_xdai:.6f} xDAI"
                        f"\nTransferred to master: {master}"
                        + (
                            "\nWARNING: pending balance read failed before drain: "
                            f"{_sanitize_error(pending_read_error)}"
                            if pending_read_error is not None
                            else ""
                        )
                    ),
                )
            else:
                await notification_service.send(
                    "Mech Payment Withdrawn",
                    (
                        f"Chain: {chain_name}\nAmount: {mech_actual_xdai:.6f} xDAI"
                        f"\nTo Safe: {multisig}"
                        f"\nWARNING: transfer to master failed: {_sanitize_error(transfer_error)}"
                        + (
                            "\nWARNING: pending balance read failed before drain: "
                            f"{_sanitize_error(pending_read_error)}"
                            if pending_read_error is not None
                            else ""
                        )
                    ),
                )

        except Exception as e:
            logger.error(
                "[{}] Payment withdraw task error: {}", chain_name, _sanitize_error(e)
            )
            await _notify_withdraw_failure(
                notification_service,
                chain_name,
                e,
                pending_xdai=balance,
                mech_balance_wei=mech_actual_wei,
                stage=stage,
            )
