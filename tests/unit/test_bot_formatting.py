"""Tests for bot.formatting utilities."""

from micromech.bot.formatting import (
    bold,
    code,
    escape_html,
    format_address,
    format_balance,
    format_chain_status,
)


class TestEscapeHtml:
    def test_ampersand(self):
        assert escape_html("a & b") == "a &amp; b"

    def test_less_than(self):
        assert escape_html("a < b") == "a &lt; b"

    def test_greater_than(self):
        assert escape_html("a > b") == "a &gt; b"

    def test_combined(self):
        assert escape_html("<a>&b</a>") == "&lt;a&gt;&amp;b&lt;/a&gt;"

    def test_empty_string(self):
        assert escape_html("") == ""

    def test_no_special_chars(self):
        assert escape_html("hello") == "hello"


class TestBold:
    def test_simple(self):
        assert bold("hello") == "<b>hello</b>"

    def test_escapes_content(self):
        assert bold("a<b") == "<b>a&lt;b</b>"


class TestCode:
    def test_simple(self):
        assert code("hello") == "<code>hello</code>"

    def test_escapes_content(self):
        assert code("x&y") == "<code>x&amp;y</code>"


class TestFormatBalance:
    def test_normal_value(self):
        assert format_balance(1.5, "OLAS") == "1.500 OLAS"

    def test_none_value(self):
        assert format_balance(None, "xDAI") == "? xDAI"

    def test_near_zero(self):
        assert format_balance(0.0001, "ETH") == "0 ETH"

    def test_exact_zero(self):
        assert format_balance(0.0, "ETH") == "0 ETH"

    def test_large_value(self):
        result = format_balance(1234.567, "OLAS")
        assert result == "1234.567 OLAS"


class TestFormatAddress:
    def test_full_address(self):
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        assert format_address(addr) == "0x1234...5678"

    def test_none(self):
        assert format_address(None) == "N/A"

    def test_empty(self):
        assert format_address("") == "N/A"

    def test_short_address(self):
        assert format_address("0x1234") == "0x1234"


class TestFormatChainStatus:
    def test_basic_format(self):
        status = {
            "staking_state": "STAKED",
            "requests_this_epoch": 5,
            "required_requests": 10,
            "rewards": 1.5,
        }
        result = format_chain_status("gnosis", status)
        assert "<b>GNOSIS</b>" in result
        assert "STAKED" in result
        assert "5/10" in result
        assert "1.500 OLAS" in result

    def test_defaults_for_missing_fields(self):
        result = format_chain_status("gnosis", {})
        assert "unknown" in result
        assert "0/0" in result
