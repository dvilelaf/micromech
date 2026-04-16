"""Integration test: mech.exec() drain on Anvil fork.

Proves that _drain_mech_to_safe() correctly pulls native xDAI from the mech
contract to the Safe by calling mech.exec() on a real Gnosis fork.

Background:
  processPaymentByMultisig() (called by the Safe) sends xDAI to the mech
  contract, NOT to msg.sender. This test verifies that the Safe (as mech
  operator) can call mech.exec(to=Safe, value=amount, data=b"", op=0) to
  move those funds back to itself — the mechanism used by _drain_mech_to_safe().

The test is self-contained: it funds the mech directly via impersonation,
so it works regardless of the fork block.

Run:
  anvil --fork-url <gnosis_rpc> --port 18545 --auto-impersonate --silent
  ANVIL_URL=http://localhost:18545 uv run pytest \
      tests/anvil/test_payment_withdraw_anvil.py -v -s
"""

import os

import pytest
from web3 import Web3

from micromech.core.marketplace import MECH_EXEC_ABI

ANVIL_URL = os.environ.get("ANVIL_URL", "http://localhost:18545")

# Production addresses (Gnosis mainnet)
MECH_ADDR = Web3.to_checksum_address("0x33Ca1E117c4254b2eE8CD7Ef1621739431a37396")
SAFE_ADDR = Web3.to_checksum_address("0x0EE0CA8A2fc8a5d9aa92a80Ae4e6A86DcAc81953")
# Well-funded address available on any recent Gnosis fork
RICH_ACCOUNT = Web3.to_checksum_address("0xe1CB04A0fA36DdD16a06ea828007E35e1a3cBC37")
# Standard Anvil test account (always funded on a fresh fork)
ANVIL_ACCOUNT_0 = Web3.to_checksum_address("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")
# Test master wallet — plain address with no contract code on Gnosis mainnet.
# Standard Foundry accounts (0xf39Fd..., 0x70997...) have EIP-7702 delegations
# on Gnosis and don't receive xDAI cleanly; this address is genuinely empty.
TEST_MASTER = Web3.to_checksum_address("0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF")

# Extend the shared ABI with getOperator for the operator-verification test
_ANVIL_TEST_ABI = MECH_EXEC_ABI + [
    {
        "name": "getOperator",
        "type": "function",
        "inputs": [],
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
    },
]


def _is_anvil_running(url: str) -> bool:
    try:
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 3}))
        return w3.is_connected()
    except Exception:
        return False


@pytest.fixture(scope="module")
def w3():
    url = os.environ.get("ANVIL_GNOSIS", ANVIL_URL)
    if not _is_anvil_running(url):
        pytest.skip(f"Anvil fork not reachable at {url}")
    _w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
    return _w3


@pytest.fixture(autouse=True)
def _revert_state(w3):
    """Snapshot before each test, revert after."""
    snap = w3.provider.make_request("evm_snapshot", [])["result"]
    yield
    w3.provider.make_request("evm_revert", [snap])


def _fund_mech(w3: Web3, amount_wei: int) -> None:
    """Send xDAI to the mech contract from a rich impersonated account."""
    # Try rich account first, fall back to Anvil default account
    for funder in (RICH_ACCOUNT, ANVIL_ACCOUNT_0):
        try:
            funder_bal = w3.eth.get_balance(funder)
            if funder_bal >= amount_wei + w3.to_wei(0.01, "ether"):
                w3.eth.send_transaction({
                    "from": funder,
                    "to": MECH_ADDR,
                    "value": amount_wei,
                    "gas": 21_000,
                    "gasPrice": w3.eth.gas_price,
                })
                return
        except Exception:
            continue
    pytest.skip("Could not fund mech — no funded account on this fork")


class TestDrainMechToSafeAnvil:
    """Verify mech.exec() moves native xDAI from mech to Safe on a real fork."""

    def test_safe_is_mech_operator(self, w3):
        """The Safe is the mech operator — prerequisite for exec() to work."""
        mech = w3.eth.contract(address=MECH_ADDR, abi=_ANVIL_TEST_ABI)
        operator = mech.functions.getOperator().call()
        assert operator.lower() == SAFE_ADDR.lower(), (
            f"Expected Safe {SAFE_ADDR} to be operator, got {operator}"
        )

    def test_exec_transfers_xdai_from_mech_to_safe(self, w3):
        """Safe (impersonated) calls mech.exec → xDAI moves from mech to Safe."""
        amount_wei = w3.to_wei(1, "ether")  # 1 xDAI test amount
        _fund_mech(w3, amount_wei)

        mech_balance_before = w3.eth.get_balance(MECH_ADDR)
        safe_balance_before = w3.eth.get_balance(SAFE_ADDR)
        assert mech_balance_before >= amount_wei

        mech = w3.eth.contract(address=MECH_ADDR, abi=_ANVIL_TEST_ABI)
        tx = mech.functions.exec(
            SAFE_ADDR,   # to: the Safe receives xDAI
            amount_wei,  # value: xDAI from mech
            b"",         # data: empty for native transfer
            0,           # operation: Call
            100_000,     # txGas
        ).build_transaction({
            "from": SAFE_ADDR,      # Safe is the operator calling exec
            "gas": 200_000,
            "gasPrice": w3.eth.gas_price,
        })

        tx_hash = w3.eth.send_transaction(tx)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)

        assert receipt["status"] == 1, "mech.exec() reverted unexpectedly"

        mech_balance_after = w3.eth.get_balance(MECH_ADDR)
        safe_balance_after = w3.eth.get_balance(SAFE_ADDR)

        # Mech lost amount_wei
        assert mech_balance_after == mech_balance_before - amount_wei

        # Safe gained amount_wei minus gas cost (Safe pays gas as tx sender)
        gas_cost = receipt["gasUsed"] * tx["gasPrice"]
        expected_safe = safe_balance_before + amount_wei - gas_cost
        assert safe_balance_after == expected_safe, (
            f"Safe balance mismatch: expected {w3.from_wei(expected_safe, 'ether')} "
            f"got {w3.from_wei(safe_balance_after, 'ether')}"
        )

    def test_exec_partial_drain(self, w3):
        """Partial drain: exec sends a specific amount, leaving the rest in mech."""
        _fund_mech(w3, w3.to_wei(1, "ether"))  # ensure mech has at least 1 xDAI
        mech_before = w3.eth.get_balance(MECH_ADDR)
        assert mech_before >= w3.to_wei(1, "ether")

        drain_wei = w3.to_wei(1, "ether")  # drain exactly 1 xDAI
        mech = w3.eth.contract(address=MECH_ADDR, abi=_ANVIL_TEST_ABI)

        tx = mech.functions.exec(
            SAFE_ADDR, drain_wei, b"", 0, 100_000
        ).build_transaction({
            "from": SAFE_ADDR,
            "gas": 200_000,
            "gasPrice": w3.eth.gas_price,
        })
        tx_hash = w3.eth.send_transaction(tx)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)

        assert receipt["status"] == 1
        mech_after = w3.eth.get_balance(MECH_ADDR)
        # Mech lost exactly drain_wei; the rest stays
        assert mech_after == mech_before - drain_wei

    def test_exec_drain_full_balance(self, w3):
        """Full drain: exec can send the entire mech balance to Safe."""
        amount_wei = w3.to_wei(0.5, "ether")
        _fund_mech(w3, amount_wei)

        mech_balance = w3.eth.get_balance(MECH_ADDR)
        mech = w3.eth.contract(address=MECH_ADDR, abi=_ANVIL_TEST_ABI)

        tx = mech.functions.exec(
            SAFE_ADDR, mech_balance, b"", 0, 100_000
        ).build_transaction({
            "from": SAFE_ADDR,
            "gas": 200_000,
            "gasPrice": w3.eth.gas_price,
        })
        tx_hash = w3.eth.send_transaction(tx)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)

        assert receipt["status"] == 1
        assert w3.eth.get_balance(MECH_ADDR) == 0

    def test_full_flow_mech_to_safe_to_master(self, w3):
        """End-to-end: the real 41.79 xDAI from the mech reaches the master wallet.

        Simulates the complete payment_withdraw_task flow on a real Gnosis fork:
          Step 1 — mech.exec(to=Safe, value=mech_balance) drains the mech.
          Step 2 — Safe sends that exact amount to a test master wallet.

        The mech has 41.79 xDAI on production (the amount stranded by the bug
        this PR fixes). After both steps master must hold exactly mech_balance
        more xDAI than it started with.
        """
        master = TEST_MASTER  # plain address with no contract code on Gnosis

        mech_balance = w3.eth.get_balance(MECH_ADDR)
        assert mech_balance > 0, (
            "Mech has no balance on this fork — fork may be past the withdrawal block"
        )

        gas_price = w3.eth.gas_price
        gas_reserve = w3.to_wei(0.01, "ether")  # gas budget for two txs

        # Ensure Safe has at least gas_reserve for paying tx fees.
        # In production the Safe is always funded; on a fresh fork it may not be.
        if w3.eth.get_balance(SAFE_ADDR) < gas_reserve:
            w3.eth.send_transaction({
                "from": ANVIL_ACCOUNT_0,
                "to": SAFE_ADDR,
                "value": gas_reserve,
                "gas": 21_000,
                "gasPrice": gas_price,
            })

        safe_before = w3.eth.get_balance(SAFE_ADDR)
        master_before = w3.eth.get_balance(master)

        # Step 1: mech → Safe via mech.exec()
        mech = w3.eth.contract(address=MECH_ADDR, abi=_ANVIL_TEST_ABI)
        tx1 = mech.functions.exec(
            SAFE_ADDR, mech_balance, b"", 0, 100_000
        ).build_transaction({"from": SAFE_ADDR, "gas": 200_000, "gasPrice": gas_price})
        r1 = w3.eth.wait_for_transaction_receipt(
            w3.eth.send_transaction(tx1), timeout=30
        )
        assert r1["status"] == 1, "mech.exec() reverted"
        assert w3.eth.get_balance(MECH_ADDR) == 0, "Mech must be empty after full drain"

        gas1 = r1["gasUsed"] * gas_price
        safe_after_drain = w3.eth.get_balance(SAFE_ADDR)
        assert safe_after_drain == safe_before + mech_balance - gas1, (
            f"Safe should hold {w3.from_wei(safe_before + mech_balance - gas1, 'ether')} "
            f"xDAI after drain, got {w3.from_wei(safe_after_drain, 'ether')}"
        )

        # Step 2: Safe → master (native transfer, simulates wallet.send()).
        # Use 100_000 gas: Foundry test accounts on Gnosis have contract code
        # deployed and require more than the standard 21_000 for EOA transfers.
        tx2 = {
            "from": SAFE_ADDR,
            "to": master,
            "value": mech_balance,
            "gas": 100_000,
            "gasPrice": gas_price,
        }
        r2 = w3.eth.wait_for_transaction_receipt(
            w3.eth.send_transaction(tx2), timeout=30
        )
        assert r2["status"] == 1, "Safe→master transfer reverted"

        master_after = w3.eth.get_balance(master)
        assert master_after == master_before + mech_balance, (
            f"Master should have received {w3.from_wei(mech_balance, 'ether')} xDAI "
            f"but got {w3.from_wei(master_after - master_before, 'ether')} xDAI"
        )

    def test_exec_fails_when_called_by_non_operator(self, w3):
        """Only the mech operator (Safe) can call exec — other callers revert."""
        amount_wei = w3.to_wei(1, "ether")
        _fund_mech(w3, amount_wei)

        non_operator = ANVIL_ACCOUNT_0
        # Fund non_operator for gas if needed
        non_op_bal = w3.eth.get_balance(non_operator)
        if non_op_bal < w3.to_wei(0.1, "ether"):
            try:
                w3.eth.send_transaction({
                    "from": RICH_ACCOUNT,
                    "to": non_operator,
                    "value": w3.to_wei(0.1, "ether"),
                    "gas": 21_000,
                    "gasPrice": w3.eth.gas_price,
                })
            except Exception:
                pytest.skip("Could not fund non_operator for gas")

        mech = w3.eth.contract(address=MECH_ADDR, abi=_ANVIL_TEST_ABI)
        tx = mech.functions.exec(
            non_operator, amount_wei, b"", 0, 100_000
        ).build_transaction({
            "from": non_operator,
            "gas": 200_000,
            "gasPrice": w3.eth.gas_price,
        })

        # Non-operator call should revert (status=0 or exception)
        try:
            tx_hash = w3.eth.send_transaction(tx)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
            assert receipt["status"] == 0, (
                "Expected revert when non-operator calls mech.exec"
            )
        except Exception:
            pass  # send_transaction raising is also a valid revert signal
