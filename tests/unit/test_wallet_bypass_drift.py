"""CI guard: detect iwa Wallet attribute drift vs our object.__new__ bypass.

In `micromech.core.bridge.get_wallet` (Path A) we construct a Wallet via
`object.__new__(Wallet)` to bypass `Wallet.__init__` (which normally prompts
for a password) and manually assign every service attribute.

**This is fragile**: if a future iwa release adds a new attribute in
`Wallet.__init__`, our bypass silently omits it. For most attributes the
downstream code raises AttributeError (loud fail). But a new *security-
relevant* attribute with an opt-in check (e.g. a transaction policy or risk
checker) would silently become absent, and transactions would sign without
that check.

This test compares the set of attributes our bridge assigns against the set
of attributes a freshly-constructed Wallet would have. Any drift fails CI
loudly, forcing a reviewer to either (a) update the bypass or (b) migrate to
a proper `Wallet.from_key_storage(ks)` factory upstream.

Security finding H3 (Round 1) / advisory (Round 2).
"""

from __future__ import annotations

import ast
from pathlib import Path

BRIDGE_PATH = Path(__file__).resolve().parents[2] / "src" / "micromech" / "core" / "bridge.py"


def _extract_bypass_attributes(source: str) -> set[str]:
    """Find every `wallet.<attr> = ...` assignment inside get_wallet.

    R3-L5: handle both plain ``ast.Assign`` and annotated ``ast.AnnAssign``
    (``wallet.x: T = ...``) so future typed assignments aren't silently missed.
    """
    tree = ast.parse(source)
    assigned: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        if node.name != "get_wallet":
            continue
        for sub in ast.walk(node):
            if isinstance(sub, ast.Assign):
                for target in sub.targets:
                    if (
                        isinstance(target, ast.Attribute)
                        and isinstance(target.value, ast.Name)
                        and target.value.id == "wallet"
                    ):
                        assigned.add(target.attr)
            elif isinstance(sub, ast.AnnAssign):
                target = sub.target
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "wallet"
                ):
                    assigned.add(target.attr)
    return assigned


def test_get_wallet_bypass_covers_known_attributes():
    """The bypass must assign at least the attributes we know about.

    If iwa adds a new Wallet attribute that our bypass misses, this test
    shows the drift explicitly. The known set below is the baseline at the
    time of the Round 1/2 review — bumping it is intentional and requires
    a reviewer to check that the new attribute does not have a hidden
    security invariant we'd be bypassing.
    """
    source = BRIDGE_PATH.read_text()
    assigned = _extract_bypass_attributes(source)

    # Baseline captured from iwa 0.7.1.
    expected = {
        "key_storage",
        "account_service",
        "balance_service",
        "safe_service",
        "transaction_service",
        "transfer_service",
        "plugin_service",
        "chain_interfaces",
    }
    missing = expected - assigned
    assert not missing, (
        f"get_wallet() bypass no longer assigns: {missing}. "
        "Either restore the assignment or update the expected set."
    )


def test_get_wallet_bypass_attributes_match_iwa_wallet_signature():
    """Compare bypass attributes against what iwa's Wallet.__init__ sets.

    Skipped if iwa is not importable (e.g. in minimal test envs).

    If iwa's Wallet grows a new attribute in __init__, this test fails —
    forcing the reviewer to consciously decide whether to extend the
    bypass or migrate to a proper factory.
    """
    try:
        from iwa.core.wallet import Wallet
    except ImportError:
        import pytest

        pytest.skip("iwa not installed — skip drift check")
        return

    # Introspect Wallet.__init__ to find `self.<attr> = ...` assignments.
    import inspect
    import textwrap

    try:
        source_code = textwrap.dedent(inspect.getsource(Wallet.__init__))
        src = ast.parse(source_code)
    except (OSError, TypeError, SyntaxError):
        import pytest

        pytest.skip("cannot read iwa Wallet source")
        return

    iwa_attrs: set[str] = set()
    for sub in ast.walk(src):
        if isinstance(sub, ast.Assign):
            for target in sub.targets:
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "self"
                ):
                    iwa_attrs.add(target.attr)
        elif isinstance(sub, ast.AnnAssign):
            target = sub.target
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
            ):
                iwa_attrs.add(target.attr)

    source = BRIDGE_PATH.read_text()
    assigned = _extract_bypass_attributes(source)

    # Attributes iwa Wallet.__init__ sets but our bypass does NOT.
    missing_in_bypass = iwa_attrs - assigned
    # Some attributes may be intentional omissions (e.g., UI state). If
    # this test fails, inspect the diff; update the bypass if the new
    # attribute is needed at runtime.
    assert not missing_in_bypass, (
        f"iwa Wallet has new attributes not covered by the bypass: "
        f"{missing_in_bypass}. Review whether they need to be added to "
        f"get_wallet() Path A, then update this test."
    )
