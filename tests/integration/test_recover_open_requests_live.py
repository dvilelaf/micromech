"""Live Anvil proof for the open-request recovery script.

This test is opt-in because it forks Gnosis through a public RPC and sends
transactions on a local Anvil node. It should finish in roughly 1-2 minutes
when public RPCs are healthy.

Run:
    RUN_RECOVERY_ANVIL_TEST=1 uv run pytest \
      tests/integration/test_recover_open_requests_live.py -s --no-cov
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_RECOVERY_ANVIL_TEST") != "1",
    reason="set RUN_RECOVERY_ANVIL_TEST=1 to run the live Anvil recovery proof",
)


def test_recover_open_requests_anvil_recovers_xdai(tmp_path: Path):
    config = tmp_path / "config.yaml"
    wallet = tmp_path / "wallet.json"
    checkpoint = tmp_path / "recover.json"

    config.write_text(
        """
plugins:
  micromech:
    chains:
      gnosis:
        chain: gnosis
        enabled: true
        mech_address: "0x33Ca1E117c4254b2eE8CD7Ef1621739431a37396"
        marketplace_address: "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"
        factory_address: "0x0000000000000000000000000000000000000000"
        staking_address: "0x0000000000000000000000000000000000000000"
        delivery_rate: 10000000000000000
        account_tag: mech
  olas:
    services:
      gnosis:recovery:
        chain_name: gnosis
        multisig_address: "0x0EE0CA8A2fc8a5d9aa92a80Ae4e6A86DcAc81953"
""".strip()
    )
    wallet.write_text('{"accounts": {}}')

    cmd = [
        sys.executable,
        "scripts/recover_open_requests.py",
        "--config",
        str(config),
        "--wallet",
        str(wallet),
        "--checkpoint",
        str(checkpoint),
        "--anvil-test",
        "--anvil-max-requests",
        "1",
    ]
    result = subprocess.run(
        cmd,
        cwd=Path(__file__).parents[2],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=140,
        check=False,
    )
    assert result.returncode == 0, result.stdout
    assert "PASS" in result.stdout
    assert "xDAI credited" in result.stdout
