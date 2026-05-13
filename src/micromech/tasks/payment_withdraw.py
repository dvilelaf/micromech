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
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
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


@dataclass(frozen=True)
class PaymentWithdrawPreview:
    """Balances relevant to a payment withdraw."""

    chain_name: str
    multisig_address: str | None
    pending_xdai: float | None
    mech_balance_wei: int | None
    safe_excess_wei: int | None
    balance_tracker_address: str | None = None

    @property
    def has_any_balance(self) -> bool:
        return (
            (self.pending_xdai or 0.0) > 0
            or (self.mech_balance_wei or 0) > 0
            or (self.safe_excess_wei or 0) > 0
        )


@dataclass(frozen=True)
class PaymentWithdrawResult:
    """Outcome of one shared payment withdraw execution."""

    chain_name: str
    status: str
    pending_xdai: float | None = None
    mech_withdrawn_wei: int = 0
    transferred_to_master_wei: int = 0
    attempted_transfer_to_master_wei: int = 0
    multisig_address: str | None = None
    transfer_error: Exception | None = None
    pending_read_error: Exception | None = None

    @property
    def success(self) -> bool:
        return self.status != "failed"

    @property
    def has_moved_funds(self) -> bool:
        return self.mech_withdrawn_wei > 0 or self.transferred_to_master_wei > 0


class PaymentWithdrawExecutionError(RuntimeError):
    """Execution failure annotated with the withdraw stage."""

    def __init__(
        self,
        stage: str,
        error: Exception,
        *,
        pending_xdai: float | None = None,
        mech_balance_wei: int | None = None,
    ) -> None:
        super().__init__(str(error))
        self.stage = stage
        self.original_error = error
        self.pending_xdai = pending_xdai
        self.mech_balance_wei = mech_balance_wei


@asynccontextmanager
async def _safe_lock(multisig_address: str, timeout_seconds: float | None = None):
    """Acquire the per-Safe lock, optionally with a bounded wait."""
    lock = get_safe_lock(multisig_address)
    if timeout_seconds is None:
        async with lock:
            yield
        return

    await asyncio.wait_for(lock.acquire(), timeout=timeout_seconds)
    try:
        yield
    finally:
        lock.release()


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
            "Error details: check local logs.",
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
        raise RuntimeError(f"[{chain_name}] Safe→master transfer returned no tx hash")
    tx_hash_str = tx_hash if isinstance(tx_hash, str) else tx_hash.hex()
    logger.info(
        "[{}] Transferred {:.6f} xDAI to master {}. TX: {}",
        chain_name,
        amount_wei / 1e18,
        master,
        tx_hash_str,
    )

    web3 = bridge.web3
    receipt = bridge.with_retry(lambda: web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120))
    if receipt["status"] != 1:
        raise RuntimeError(f"[{chain_name}] xDAI transfer to master reverted: {tx_hash_str}")


def _is_gs026_error(error: Exception) -> bool:
    return "GS026" in str(error)


def _transfer_to_master_with_retry(
    bridge: "IwaBridge",
    chain_name: str,
    multisig_address: str,
    amount_wei: int,
    *,
    attempts: int = 3,
    retry_delay_seconds: float = 5.0,
) -> None:
    """Transfer Safe xDAI to master, retrying Safe nonce-state races."""
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            _transfer_to_master(bridge, chain_name, multisig_address, amount_wei)
            return
        except Exception as e:
            last_error = e
            if not _is_gs026_error(e) or attempt >= attempts:
                raise
            logger.warning(
                "[{}] Safe→master transfer hit GS026 on attempt {}/{}; retrying in {:.1f}s",
                chain_name,
                attempt,
                attempts,
                retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)
    if last_error is not None:
        raise last_error


def _xdai_to_wei(amount_xdai: float) -> int:
    return int(Decimal(str(amount_xdai)) * Decimal(10) ** 18)


def _get_safe_excess_balance_wei(
    bridge: "IwaBridge",
    chain_name: str,
    multisig_address: str,
    reserve_xdai: float,
) -> int:
    """Return Safe xDAI above the configured reserve."""
    web3 = bridge.web3
    safe_balance_wei = bridge.with_retry(
        lambda: web3.eth.get_balance(web3.to_checksum_address(multisig_address))
    )
    reserve_wei = _xdai_to_wei(reserve_xdai)
    return max(int(safe_balance_wei) - reserve_wei, 0)


def _transfer_safe_excess_to_master(
    bridge: "IwaBridge",
    chain_name: str,
    multisig_address: str,
    reserve_xdai: float,
) -> int:
    """Transfer Safe xDAI above reserve to master. Returns transferred wei."""
    amount_wei = _get_safe_excess_balance_wei(bridge, chain_name, multisig_address, reserve_xdai)
    if amount_wei <= 0:
        logger.debug(
            "[{}] Safe balance is at or below reserve {:.6f} xDAI; no sweep",
            chain_name,
            reserve_xdai,
        )
        return 0

    _transfer_to_master_with_retry(bridge, chain_name, multisig_address, amount_wei)
    return amount_wei


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

    receipt = bridge.with_retry(lambda: web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120))
    if receipt["status"] != 1:
        msg = f"[{chain_name}] processPaymentByMultisig reverted: {tx_hash_str}"
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
        0,  # operation = Call
        100_000,  # txGas — 21k for native transfer + margin for Safe fallback/receive handler
    )
    calldata = fn_call.build_transaction({"from": web3.to_checksum_address(multisig_address)})[
        "data"
    ]

    tx_hash = bridge.wallet.safe_service.execute_safe_transaction(
        safe_address_or_tag=multisig_address,
        to=mech_address,
        value=0,
        chain_name=chain_name,
        data=calldata,
    )
    tx_hash_str = tx_hash if isinstance(tx_hash, str) else tx_hash.hex()
    logger.info("[{}] mech.exec drain TX: {}", chain_name, tx_hash_str)

    receipt = bridge.with_retry(lambda: web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120))
    if receipt["status"] != 1:
        raise RuntimeError(f"[{chain_name}] mech.exec drain reverted: {tx_hash_str}")


def _meets_withdraw_threshold(amount_xdai: float, threshold_xdai: float) -> bool:
    """Manual threshold 0 means any positive amount; scheduled threshold is inclusive."""
    if threshold_xdai <= 0:
        return amount_xdai > 0
    return amount_xdai >= threshold_xdai


async def preview_payment_withdraw(
    bridge: "IwaBridge",
    chain_name: str,
    chain_config: object,
    *,
    safe_reserve_xdai: float = 0.0,
) -> PaymentWithdrawPreview:
    """Read withdrawable balances for UI/API confirmation."""
    from micromech.core.bridge import get_service_info

    svc_info = await asyncio.to_thread(get_service_info, chain_name)
    multisig = svc_info.get("multisig_address")

    bt_address: str | None = None
    pending: float | None = None
    mech_wei: int | None = None
    safe_excess_wei: int | None = None

    try:
        bt_address = await asyncio.to_thread(
            get_balance_tracker_address,
            bridge,
            chain_name,
            chain_config.mech_address,
            chain_config.marketplace_address,
        )
        if bt_address:
            pending = await asyncio.to_thread(
                get_pending_balance,
                bridge,
                bt_address,
                chain_config.mech_address,
                raise_on_error=True,
            )
    except Exception:
        pending = None

    try:
        web3 = bridge.web3
        mech_wei = await asyncio.to_thread(
            bridge.with_retry,
            lambda: web3.eth.get_balance(web3.to_checksum_address(chain_config.mech_address)),
        )
    except Exception:
        mech_wei = None

    if isinstance(multisig, str) and multisig.startswith("0x"):
        try:
            safe_excess_wei = await asyncio.to_thread(
                _get_safe_excess_balance_wei,
                bridge,
                chain_name,
                multisig,
                safe_reserve_xdai,
            )
        except Exception:
            safe_excess_wei = None

    return PaymentWithdrawPreview(
        chain_name=chain_name,
        multisig_address=multisig if isinstance(multisig, str) else None,
        pending_xdai=pending,
        mech_balance_wei=mech_wei,
        safe_excess_wei=safe_excess_wei,
        balance_tracker_address=bt_address,
    )


async def execute_payment_withdraw(
    bridge: "IwaBridge",
    chain_name: str,
    chain_config: object,
    *,
    threshold_xdai: float = 0.0,
    safe_reserve_xdai: float = 0.0,
    sweep_existing_safe_excess: bool = False,
    safe_lock_timeout_seconds: float | None = None,
) -> PaymentWithdrawResult:
    """Execute the single shared payment withdraw/sweep flow.

    This is the only place that submits withdraw/drain/Safe→master txs. Telegram,
    web and the scheduler should call this instead of assembling the flow.
    """
    from micromech.core.bridge import get_service_info

    pending: float | None = None
    pending_read_error: Exception | None = None

    try:
        svc_info = await asyncio.to_thread(get_service_info, chain_name)
        multisig = svc_info.get("multisig_address")
    except Exception as e:
        raise PaymentWithdrawExecutionError("resolve service info", e) from e
    if not multisig:
        raise RuntimeError(f"[{chain_name}] No multisig_address found")

    web3 = bridge.web3
    try:
        existing_mech_wei = await asyncio.to_thread(
            bridge.with_retry,
            lambda: web3.eth.get_balance(web3.to_checksum_address(chain_config.mech_address)),
        )
    except Exception as e:
        raise PaymentWithdrawExecutionError("read existing mech balance", e) from e
    existing_mech_xdai = existing_mech_wei / 1e18

    tracker_resolution_error: Exception | None = None
    bt_address: str | None = None
    try:
        bt_address = await asyncio.to_thread(
            get_balance_tracker_address,
            bridge,
            chain_name,
            chain_config.mech_address,
            chain_config.marketplace_address,
            raise_on_error=True,
        )
    except Exception as e:
        tracker_resolution_error = e
    if bt_address:
        try:
            pending = await asyncio.to_thread(
                get_pending_balance,
                bridge,
                bt_address,
                chain_config.mech_address,
                raise_on_error=True,
            )
        except Exception as e:
            pending_read_error = e
            if _meets_withdraw_threshold(existing_mech_xdai, threshold_xdai):
                logger.warning(
                    "[{}] Pending balance read failed, but mech already holds {:.6f} xDAI;"
                    " proceeding with stranded drain: {}",
                    chain_name,
                    existing_mech_xdai,
                    _sanitize_error(e),
                )

    should_process_pending = pending is not None and _meets_withdraw_threshold(
        pending, threshold_xdai
    )
    should_drain_mech = _meets_withdraw_threshold(existing_mech_xdai, threshold_xdai)
    mech_actual_wei = 0
    transfer_error: Exception | None = None

    try:
        lock_context = _safe_lock(multisig, safe_lock_timeout_seconds)
        await lock_context.__aenter__()
    except asyncio.TimeoutError:
        return PaymentWithdrawResult(
            chain_name=chain_name,
            status="lock_busy",
            pending_xdai=pending,
            mech_withdrawn_wei=0,
            multisig_address=multisig,
            pending_read_error=pending_read_error,
        )

    try:
        if should_process_pending:
            if not bt_address:
                raise RuntimeError(
                    f"[{chain_name}] Cannot process pending payments without balance tracker"
                )
            try:
                await asyncio.to_thread(
                    _withdraw,
                    bridge,
                    chain_name,
                    bt_address,
                    chain_config.mech_address,
                    multisig,
                )
            except Exception as e:
                raise PaymentWithdrawExecutionError(
                    "processPaymentByMultisig",
                    e,
                    pending_xdai=pending,
                    mech_balance_wei=existing_mech_wei,
                ) from e

        if should_process_pending or should_drain_mech:
            try:
                mech_actual_wei = await asyncio.to_thread(
                    bridge.with_retry,
                    lambda: web3.eth.get_balance(
                        web3.to_checksum_address(chain_config.mech_address)
                    ),
                )
            except Exception as e:
                raise PaymentWithdrawExecutionError(
                    "read mech balance",
                    e,
                    pending_xdai=pending,
                    mech_balance_wei=existing_mech_wei,
                ) from e
            if mech_actual_wei > 0:
                try:
                    await asyncio.to_thread(
                        _drain_mech_to_safe,
                        bridge,
                        chain_name,
                        chain_config.mech_address,
                        multisig,
                        mech_actual_wei,
                    )
                except Exception as e:
                    raise PaymentWithdrawExecutionError(
                        "mech.exec drain",
                        e,
                        pending_xdai=pending,
                        mech_balance_wei=mech_actual_wei,
                    ) from e

        if sweep_existing_safe_excess:
            try:
                transfer_to_master_wei = await asyncio.to_thread(
                    _get_safe_excess_balance_wei,
                    bridge,
                    chain_name,
                    multisig,
                    safe_reserve_xdai,
                )
            except Exception as e:
                raise PaymentWithdrawExecutionError(
                    "read Safe excess balance",
                    e,
                    pending_xdai=pending,
                    mech_balance_wei=mech_actual_wei,
                ) from e
        elif should_process_pending or should_drain_mech:
            transfer_to_master_wei = mech_actual_wei
        else:
            transfer_to_master_wei = 0

        if transfer_to_master_wei <= 0:
            if tracker_resolution_error is not None:
                raise PaymentWithdrawExecutionError(
                    "resolve balance tracker",
                    tracker_resolution_error,
                    mech_balance_wei=existing_mech_wei,
                ) from tracker_resolution_error
            if pending_read_error is not None and not should_drain_mech and mech_actual_wei <= 0:
                raise PaymentWithdrawExecutionError(
                    "read pending balance",
                    pending_read_error,
                    mech_balance_wei=existing_mech_wei,
                ) from pending_read_error
            return PaymentWithdrawResult(
                chain_name=chain_name,
                status="no_funds" if mech_actual_wei <= 0 else "drained_to_safe",
                pending_xdai=pending,
                mech_withdrawn_wei=mech_actual_wei,
                multisig_address=multisig,
                pending_read_error=pending_read_error,
            )

        try:
            await asyncio.to_thread(
                _transfer_to_master_with_retry,
                bridge,
                chain_name,
                multisig,
                transfer_to_master_wei,
            )
        except Exception as e:
            transfer_error = e
            logger.warning(
                "[{}] Safe→master transfer failed after withdraw/sweep: {}",
                chain_name,
                _sanitize_error(e),
            )
    finally:
        await lock_context.__aexit__(None, None, None)

    if transfer_error is not None:
        return PaymentWithdrawResult(
            chain_name=chain_name,
            status="transfer_failed",
            pending_xdai=pending,
            mech_withdrawn_wei=mech_actual_wei,
            transferred_to_master_wei=0,
            attempted_transfer_to_master_wei=transfer_to_master_wei,
            multisig_address=multisig,
            transfer_error=transfer_error,
            pending_read_error=pending_read_error,
        )

    status = "swept_safe" if mech_actual_wei <= 0 else "withdrawn"
    return PaymentWithdrawResult(
        chain_name=chain_name,
        status=status,
        pending_xdai=pending,
        mech_withdrawn_wei=mech_actual_wei,
        transferred_to_master_wei=transfer_to_master_wei,
        attempted_transfer_to_master_wei=transfer_to_master_wei,
        multisig_address=multisig,
        pending_read_error=pending_read_error,
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

        if not hasattr(bridge, "wallet") or not hasattr(bridge.wallet, "safe_service"):
            logger.debug(
                "[{}] No safe_service on bridge, skipping payment withdraw",
                chain_name,
            )
            continue

        try:
            result = await execute_payment_withdraw(
                bridge,
                chain_name,
                chain_config,
                threshold_xdai=threshold,
                safe_reserve_xdai=config.payment_withdraw_safe_reserve_xdai,
                sweep_existing_safe_excess=False,
            )
            if result.status == "no_funds":
                logger.debug("[{}] No payment withdraw funds available", chain_name)
                continue

            if result.status == "swept_safe":
                await notification_service.send(
                    "Safe Payment Swept",
                    (
                        f"Chain: {chain_name}\n"
                        f"Amount: {result.transferred_to_master_wei / 1e18:.6f} xDAI\n"
                        "Source: Safe excess from previous withdraw"
                    ),
                )
                continue

            if result.status == "drained_to_safe":
                await notification_service.send(
                    "Mech Payment Drained",
                    (
                        f"Chain: {chain_name}\n"
                        f"Amount: {result.mech_withdrawn_wei / 1e18:.6f} xDAI\n"
                        "To Safe: transfer to master skipped because Safe excess is at or below reserve."
                    ),
                )
                continue

            if result.transfer_error is None:
                master = str(bridge.wallet.master_account.address)
                await notification_service.send(
                    "Mech Payment Withdrawn",
                    (
                        f"Chain: {chain_name}\nAmount: {result.mech_withdrawn_wei / 1e18:.6f} xDAI"
                        f"\nTransferred to master: {result.transferred_to_master_wei / 1e18:.6f} xDAI"
                        f"\nMaster: {master}"
                        + (
                            "\nWARNING: pending balance read failed before drain; check local logs."
                            if result.pending_read_error is not None
                            else ""
                        )
                    ),
                )
            else:
                title = (
                    "Safe Payment Sweep Failed"
                    if result.mech_withdrawn_wei <= 0
                    else "Mech Payment Withdrawn"
                )
                amount_line = (
                    f"Safe amount still pending transfer: "
                    f"{result.attempted_transfer_to_master_wei / 1e18:.6f} xDAI"
                    if result.mech_withdrawn_wei <= 0
                    else f"Amount: {result.mech_withdrawn_wei / 1e18:.6f} xDAI"
                )
                await notification_service.send(
                    title,
                    (
                        f"Chain: {chain_name}\n{amount_line}"
                        f"\nSafe: {result.multisig_address}"
                        "\nWARNING: transfer to master failed; check local logs."
                        + (
                            "\nWARNING: pending balance read failed before drain; check local logs."
                            if result.pending_read_error is not None
                            else ""
                        )
                    ),
                )

        except Exception as e:
            logger.error("[{}] Payment withdraw task error: {}", chain_name, _sanitize_error(e))
            stage = getattr(e, "stage", "payment withdraw")
            pending_xdai = getattr(e, "pending_xdai", None)
            mech_balance_wei = getattr(e, "mech_balance_wei", None)
            await _notify_withdraw_failure(
                notification_service,
                chain_name,
                e,
                pending_xdai=pending_xdai,
                mech_balance_wei=mech_balance_wei,
                stage=stage,
            )
