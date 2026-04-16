"""Tests for MarkdownV2 formatting utilities in bot.formatting."""

from micromech.bot.formatting import (
    bold_md,
    code_md,
    escape_md,
    format_currency,
    format_token,
    italic_md,
    link_md,
    split_md_blocks,
)


class TestEscapeMd:
    def test_no_special_chars(self):
        assert escape_md("hello") == "hello"

    def test_escapes_dot(self):
        assert escape_md("v1.2") == "v1\\.2"

    def test_escapes_parens(self):
        assert escape_md("(x)") == "\\(x\\)"

    def test_escapes_dash(self):
        assert escape_md("a-b") == "a\\-b"

    def test_escapes_exclamation(self):
        assert escape_md("wow!") == "wow\\!"

    def test_escapes_underscore(self):
        assert escape_md("a_b") == "a\\_b"

    def test_escapes_star(self):
        assert escape_md("a*b") == "a\\*b"

    def test_escapes_brackets(self):
        assert escape_md("[x]") == "\\[x\\]"

    def test_escapes_tilde(self):
        assert escape_md("~x~") == "\\~x\\~"

    def test_empty_string(self):
        assert escape_md("") == ""

    def test_all_reserved(self):
        text = "_*[]()~`>#+-=|{}.!\\"
        result = escape_md(text)
        for ch in text:
            assert f"\\{ch}" in result

    def test_int_input(self):
        result = escape_md(42)  # type: ignore[arg-type]
        assert result == "42"


class TestBoldMd:
    def test_simple(self):
        assert bold_md("hello") == "*hello*"

    def test_escapes_content(self):
        assert bold_md("a.b") == "*a\\.b*"


class TestCodeMd:
    def test_simple(self):
        assert code_md("hello") == "`hello`"

    def test_escapes_backtick(self):
        assert code_md("a`b") == "`a\\`b`"

    def test_escapes_backslash(self):
        assert code_md("a\\b") == "`a\\\\b`"

    def test_number_input(self):
        assert code_md(42) == "`42`"  # type: ignore[arg-type]


class TestItalicMd:
    def test_simple(self):
        assert italic_md("hello") == "_hello_"

    def test_escapes_content(self):
        assert italic_md("a!b") == "_a\\!b_"


class TestLinkMd:
    def test_simple_link(self):
        result = link_md("click", "https://example.com")
        assert result == "[click](https://example.com)"

    def test_label_escaped(self):
        result = link_md("a.b", "https://example.com")
        assert "a\\.b" in result


class TestFormatToken:
    def test_normal_value(self):
        assert format_token(1.5, "OLAS") == "1.50 OLAS"

    def test_large_value_comma(self):
        assert format_token(1234.5, "OLAS") == "1,234.50 OLAS"

    def test_none_value(self):
        assert format_token(None, "OLAS") == "? OLAS"

    def test_zero(self):
        assert format_token(0.0, "xDAI") == "0.00 xDAI"


class TestFormatCurrency:
    def test_normal(self):
        assert format_currency(10.5) == "€10.50"

    def test_large(self):
        assert format_currency(1234.0) == "€1,234.00"


class TestSplitMdBlocks:
    def test_single_block_no_header(self):
        blocks = ["block1"]
        result = split_md_blocks(blocks)
        assert result == ["block1"]

    def test_multiple_blocks_joined(self):
        blocks = ["a", "b", "c"]
        result = split_md_blocks(blocks)
        assert len(result) == 1
        assert "a\n\nb\n\nc" in result[0]

    def test_header_prepended(self):
        blocks = ["block1"]
        result = split_md_blocks(blocks, header="*HEADER*\n")
        assert result[0].startswith("*HEADER*\n")

    def test_splits_on_max_length(self):
        # blocks large enough to force split
        blocks = ["x" * 2000, "y" * 2000]
        result = split_md_blocks(blocks, max_length=2500)
        assert len(result) == 2

    def test_empty_blocks(self):
        result = split_md_blocks([])
        assert result == []

    def test_custom_separator(self):
        blocks = ["a", "b"]
        result = split_md_blocks(blocks, separator="\n")
        assert "a\nb" in result[0]
