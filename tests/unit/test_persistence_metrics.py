"""Tests for PersistentQueue aggregate query methods."""

from datetime import datetime, timezone

from micromech.core.models import MechRequest, ToolResult
from micromech.core.persistence import PersistentQueue


def _make_request(
    request_id: str,
    tool: str = "echo",
    is_offchain: bool = False,
    chain: str = "gnosis",
) -> MechRequest:
    return MechRequest(
        request_id=request_id,
        prompt="test",
        tool=tool,
        is_offchain=is_offchain,
        chain=chain,
    )


class TestToolStats:
    def test_empty_db(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        assert q.tool_stats() == []
        q.close()

    def test_counts_by_tool(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        # Add requests with different tools
        for i in range(3):
            req = _make_request(f"echo-{i}", tool="echo")
            q.add_request(req)
            q.mark_executing(req.request_id)
            q.mark_executed(req.request_id, ToolResult(output="ok", execution_time=1.0))
            q.mark_delivered(req.request_id, tx_hash=f"0x{i:064x}")

        req = _make_request("llm-0", tool="llm")
        q.add_request(req)
        q.mark_executing(req.request_id)
        q.mark_executed(req.request_id, ToolResult(error="timeout", execution_time=5.0))

        stats = q.tool_stats()
        assert len(stats) == 2
        echo_stat = next(s for s in stats if s["tool"] == "echo")
        assert echo_stat["total"] == 3
        assert echo_stat["delivered"] == 3
        assert echo_stat["failed"] == 0
        assert echo_stat["avg_time"] == 1.0

        llm_stat = next(s for s in stats if s["tool"] == "llm")
        assert llm_stat["total"] == 1
        assert llm_stat["failed"] == 1
        q.close()


class TestDailyStats:
    def test_empty_db(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        assert q.daily_stats() == []
        q.close()

    def test_groups_by_day(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        req = _make_request("r1", tool="echo")
        q.add_request(req)

        stats = q.daily_stats(30)
        assert len(stats) >= 1
        assert stats[0]["total"] >= 1
        q.close()


class TestMonthlyStats:
    def test_empty_db(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        assert q.monthly_stats() == []
        q.close()

    def test_groups_by_month(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        req = _make_request("r1", tool="echo")
        q.add_request(req)

        stats = q.monthly_stats(12)
        assert len(stats) >= 1
        today = datetime.now(timezone.utc).strftime("%Y-%m")
        assert stats[-1]["month"] == today
        q.close()


class TestOnchainOffchainCounts:
    def test_empty_db(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        counts = q.onchain_offchain_counts()
        assert counts == {"onchain": 0, "offchain": 0}
        q.close()

    def test_counts_correctly(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        q.add_request(_make_request("r1", is_offchain=False))
        q.add_request(_make_request("r2", is_offchain=False))
        q.add_request(_make_request("r3", is_offchain=True))

        counts = q.onchain_offchain_counts()
        assert counts["onchain"] == 2
        assert counts["offchain"] == 1
        q.close()


class TestChainFiltering:
    """Verify all aggregate queries correctly filter by chain."""

    def _setup_multichain(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        # Gnosis: 3 requests (2 delivered, 1 failed)
        for i in range(2):
            req = _make_request(f"gno-{i}", tool="echo", chain="gnosis")
            q.add_request(req)
            q.mark_executing(req.request_id)
            q.mark_executed(req.request_id, ToolResult(output="ok", execution_time=1.0))
            q.mark_delivered(req.request_id, tx_hash=f"0x{i:064x}")
        req = _make_request("gno-fail", tool="echo", chain="gnosis")
        q.add_request(req)
        q.mark_executing(req.request_id)
        q.mark_executed(req.request_id, ToolResult(error="err", execution_time=0.5))

        # Base: 1 request (pending)
        q.add_request(_make_request("base-0", tool="llm", chain="base"))
        return q

    def test_count_by_chain(self, tmp_path):
        q = self._setup_multichain(tmp_path)
        by_chain = q.count_by_chain()
        assert by_chain["gnosis"] == 3
        assert by_chain["base"] == 1
        q.close()

    def test_count_by_status_filtered(self, tmp_path):
        q = self._setup_multichain(tmp_path)
        gnosis = q.count_by_status(chain="gnosis")
        assert gnosis["delivered"] == 2
        assert gnosis["failed"] == 1
        assert gnosis["pending"] == 0

        base = q.count_by_status(chain="base")
        assert base["pending"] == 1
        assert base["delivered"] == 0
        q.close()

    def test_count_by_status_all(self, tmp_path):
        q = self._setup_multichain(tmp_path)
        all_chains = q.count_by_status(chain=None)
        assert all_chains["delivered"] == 2
        assert all_chains["failed"] == 1
        assert all_chains["pending"] == 1
        q.close()

    def test_tool_stats_filtered(self, tmp_path):
        q = self._setup_multichain(tmp_path)
        gnosis_stats = q.tool_stats(chain="gnosis")
        assert len(gnosis_stats) == 1
        assert gnosis_stats[0]["tool"] == "echo"
        assert gnosis_stats[0]["total"] == 3

        base_stats = q.tool_stats(chain="base")
        assert len(base_stats) == 1
        assert base_stats[0]["tool"] == "llm"
        q.close()

    def test_daily_stats_filtered(self, tmp_path):
        q = self._setup_multichain(tmp_path)
        gnosis_daily = q.daily_stats(chain="gnosis")
        assert len(gnosis_daily) >= 1
        assert gnosis_daily[0]["total"] == 3

        base_daily = q.daily_stats(chain="base")
        assert len(base_daily) >= 1
        assert base_daily[0]["total"] == 1
        q.close()

    def test_onchain_offchain_filtered(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        q.add_request(_make_request("g1", is_offchain=False, chain="gnosis"))
        q.add_request(_make_request("b1", is_offchain=True, chain="base"))

        gnosis = q.onchain_offchain_counts(chain="gnosis")
        assert gnosis == {"onchain": 1, "offchain": 0}

        base = q.onchain_offchain_counts(chain="base")
        assert base == {"onchain": 0, "offchain": 1}
        q.close()

    def test_get_recent_filtered(self, tmp_path):
        q = self._setup_multichain(tmp_path)
        gnosis = q.get_recent(50, chain="gnosis")
        assert len(gnosis) == 3
        assert all(r.request.chain == "gnosis" for r in gnosis)

        base = q.get_recent(50, chain="base")
        assert len(base) == 1
        assert base[0].request.chain == "base"
        q.close()

    def test_get_undelivered_filtered(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        # Create executed requests on different chains
        for chain in ["gnosis", "base"]:
            req = _make_request(f"{chain}-exec", chain=chain)
            q.add_request(req)
            q.mark_executing(req.request_id)
            q.mark_executed(req.request_id, ToolResult(output="ok"))

        gnosis = q.get_undelivered(chain="gnosis")
        assert len(gnosis) == 1
        assert gnosis[0].request.chain == "gnosis"

        base = q.get_undelivered(chain="base")
        assert len(base) == 1
        assert base[0].request.chain == "base"
        q.close()


# ---------------------------------------------------------------------------
# timed_out field in aggregate stats
# ---------------------------------------------------------------------------


class TestTimedOutStats:
    """Verify timed_out is correctly counted in tool_stats and period_stats."""

    def _seed_with_timeout(self, tmp_path):
        """Create a queue with 1 delivered, 1 timed-out, 1 failed (other error)."""
        q = PersistentQueue(tmp_path / "test.db")

        # Delivered
        req = _make_request("r-delivered", tool="echo")
        q.add_request(req)
        q.mark_executing("r-delivered")
        q.mark_executed("r-delivered", ToolResult(output="ok", execution_time=1.0))
        q.mark_delivered("r-delivered", tx_hash="0x" + "11" * 32)

        # On-chain timeout
        req = _make_request("r-timeout", tool="echo")
        q.add_request(req)
        q.mark_executing("r-timeout")
        q.mark_executed("r-timeout", ToolResult(output="ok", execution_time=0.5))
        q.mark_timed_out("r-timeout", tx_hash="0x" + "22" * 32)

        # Generic failure (not timeout)
        req = _make_request("r-failed", tool="echo")
        q.add_request(req)
        q.mark_failed("r-failed", "tool crashed")

        return q

    def test_tool_stats_includes_timed_out(self, tmp_path):
        q = self._seed_with_timeout(tmp_path)
        stats = q.tool_stats()
        assert len(stats) == 1
        s = stats[0]
        assert s["tool"] == "echo"
        assert s["delivered"] == 1
        assert s["failed"] == 2       # timeout + generic failure both FAILED
        assert s["timed_out"] == 1    # only the on_chain_timeout one
        q.close()

    def test_tool_stats_zero_timed_out_when_none(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        req = _make_request("r1", tool="echo")
        q.add_request(req)
        q.mark_executing("r1")
        q.mark_executed("r1", ToolResult(output="ok"))
        q.mark_delivered("r1", tx_hash="0x" + "aa" * 32)

        stats = q.tool_stats()
        assert stats[0]["timed_out"] == 0
        q.close()

    def test_period_stats_includes_timed_out(self, tmp_path):
        q = self._seed_with_timeout(tmp_path)
        stats = q.period_stats(hours=24)
        assert stats["failed"] == 2        # timeout + generic failure
        assert stats["timed_out"] == 1     # only on_chain_timeout
        assert stats["delivered"] == 1
        q.close()

    def test_period_stats_zero_timed_out_when_none(self, tmp_path):
        q = PersistentQueue(tmp_path / "test.db")
        stats = q.period_stats(hours=24)
        assert stats["timed_out"] == 0
        q.close()
