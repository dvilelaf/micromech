"""Preventive tests: MarkdownV2 injection via external strings.

These tests document the invariant that all external strings that reach
Telegram's MarkdownV2 parser (chain names, contract names, error messages,
addresses) go through `escape_md` / `bold_md` / `code_md` before being
rendered. They exist so future changes to the command handlers don't
accidentally re-introduce injection by dropping a dynamic value into an
f-string without escaping.

Advisory A1 from the Round 1 team review (security-md).
"""

from micromech.bot.formatting import (
    bold_md,
    code_md,
    escape_md,
    italic_md,
    link_md,
)

# A payload with every MarkdownV2 reserved character. If escape_md is correct,
# every reserved character appears preceded by a backslash in the output.
HOSTILE = "*_`[]()~>#+-=|{}.!\\"


class TestEscapeMdHostilePayload:
    def test_every_reserved_char_is_escaped(self):
        escaped = escape_md(HOSTILE)
        for ch in "_*[]()~`>#+-=|{}.!\\":
            # Each reserved char must appear backslash-prefixed somewhere in output.
            assert f"\\{ch}" in escaped, f"missing backslash for {ch!r}"

    def test_bold_md_escapes_payload(self):
        # bold_md(x) == f"*{escape_md(x)}*" — outer stars must not be escaped,
        # inner content must be.
        result = bold_md(HOSTILE)
        assert result.startswith("*") and result.endswith("*")
        assert "\\*" in result  # inner stars escaped
        assert "\\_" in result
        assert "\\`" in result

    def test_link_md_escapes_label_and_url(self):
        # Labels are escape_md'd; URLs must have \ and ) escaped.
        label = "click (here)"
        url = "https://example.com/path)?q=1\\danger"
        result = link_md(label, url)
        # Label chars must be escaped
        assert "\\(" in result
        assert "\\)" in result
        # URL ) must be escaped as \) (inside the link URL part)
        # URL \ must be escaped as \\
        assert "\\\\danger" in result

    def test_code_md_handles_backticks(self):
        # code_md only escapes ` and \ (not other MarkdownV2 reserved chars).
        result = code_md("x`y\\z")
        assert result == "`x\\`y\\\\z`"

    def test_italic_md_escapes_payload(self):
        # italic_md(x) == f"_{escape_md(x)}_" — outer underscores must not be
        # escaped, inner content must be.
        result = italic_md(HOSTILE)
        assert result.startswith("_") and result.endswith("_")
        inner = result[1:-1]
        # Every _ in the inner must be backslash-escaped.
        i = 0
        while i < len(inner):
            if inner[i] == "_":
                assert i > 0 and inner[i - 1] == "\\", f"unescaped _ at position {i} in {inner!r}"
            i += 1


class TestChainAndContractNameInjection:
    """A hostile on-chain contract name must never smuggle MarkdownV2 formatting."""

    def test_bold_md_contract_name_cant_escape_wrapper(self):
        # A hostile contract name trying to close the bold wrapper must not.
        hostile = "*actually_not_bold*"
        result = bold_md(f"CONTRACT — {hostile}")
        # Inner * must be escaped; wrapper * remain unescaped.
        assert result.startswith("*")
        assert result.endswith("*")
        # There must be NO unescaped * inside the payload area.
        inner = result[1:-1]
        # Every * in the inner must be backslash-escaped
        i = 0
        while i < len(inner):
            if inner[i] == "*":
                assert i > 0 and inner[i - 1] == "\\", f"unescaped * at position {i} in {inner!r}"
            i += 1

    def test_escape_md_does_not_produce_bare_backticks(self):
        # Avoid accidentally starting a code span.
        hostile = "prefix `payload` suffix"
        result = escape_md(hostile)
        # Every backtick must be preceded by a backslash.
        for i, ch in enumerate(result):
            if ch == "`":
                assert i > 0 and result[i - 1] == "\\"
