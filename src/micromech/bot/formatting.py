"""Telegram message formatting utilities.

HTML helpers (legacy, kept for backward compat with settings/manage):
  bold(), code(), escape_html(), format_address(), format_balance(),
  split_message_blocks()

MarkdownV2 helpers (used by status, wallet, claim, contracts, last_rewards, info):
  escape_md(), bold_md(), code_md(), italic_md(), link_md(),
  format_token(), format_currency(), split_md_blocks(),
  explorer_link_md(), format_epoch_countdown(), user_error()
"""

from datetime import datetime, timezone
from typing import List, Optional

from loguru import logger

# ---------------------------------------------------------------------------
# HTML helpers (parse_mode="HTML")
# ---------------------------------------------------------------------------


def escape_html(text: str) -> str:
    """Escape HTML special characters for Telegram."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def bold(text: str) -> str:
    """Wrap text in bold HTML tags."""
    return f"<b>{escape_html(text)}</b>"


def code(text: str) -> str:
    """Wrap text in code HTML tags."""
    return f"<code>{escape_html(text)}</code>"


def format_balance(amount: Optional[float], symbol: str) -> str:
    """Format a token balance with symbol (HTML)."""
    if amount is None:
        return f"? {symbol}"
    if abs(amount) < 0.001:
        return f"0 {symbol}"
    return f"{amount:.3f} {symbol}"


def format_address(addr: Optional[str]) -> str:
    """Format an address as shortened 0x1234...abcd."""
    if not addr:
        return "N/A"
    if len(addr) < 12:
        return addr
    return f"{addr[:6]}...{addr[-4:]}"


def split_message_blocks(blocks: list[str], max_length: int = 4096) -> list[str]:
    """Split a list of HTML text blocks into messages within Telegram's limit."""
    messages: list[str] = []
    current_parts: list[str] = []
    current_len = 0

    for block in blocks:
        needed = len(block) + (2 if current_parts else 0)
        if current_parts and current_len + needed > max_length:
            messages.append("\n\n".join(current_parts))
            current_parts = []
            current_len = 0
        current_parts.append(block)
        current_len += needed

    if current_parts:
        messages.append("\n\n".join(current_parts))

    return messages


def format_chain_status(chain: str, status: dict) -> str:
    """Format a per-chain status block (HTML, legacy)."""
    lines = [bold(chain.upper())]
    state = status.get("staking_state", "unknown")
    lines.append(f"State: {code(state)}")
    requests = status.get("requests_this_epoch", 0)
    required = status.get("required_requests", 0)
    lines.append(f"Deliveries: {code(f'{requests}/{required}')}")
    rewards = status.get("rewards", 0)
    lines.append(f"Rewards: {code(format_balance(rewards, 'OLAS'))}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MarkdownV2 helpers (parse_mode=ParseMode.MARKDOWN_V2)
# ---------------------------------------------------------------------------

_MD_RESERVED = r"_*[]()~`>#+-=|{}.!\\"


def escape_md(text: str) -> str:
    """Escape text for MarkdownV2."""
    return "".join(f"\\{c}" if c in _MD_RESERVED else c for c in str(text))


def bold_md(text: str) -> str:
    """Bold text in MarkdownV2."""
    return f"*{escape_md(str(text))}*"


def code_md(text: str) -> str:
    r"""Inline code in MarkdownV2 (only ` and \ need escaping inside)."""
    escaped = str(text).replace("\\", "\\\\").replace("`", "\\`")
    return f"`{escaped}`"


def italic_md(text: str) -> str:
    """Italic text in MarkdownV2."""
    return f"_{escape_md(str(text))}_"


def link_md(label: str, url: str) -> str:
    """Markdown link [label](url) with URL escaping for backslash and closing paren."""
    safe_url = url.replace("\\", "\\\\").replace(")", "\\)")
    return f"[{escape_md(label)}]({safe_url})"


EXPLORER_URLS = {
    "gnosis": "https://gnosisscan.io/address/",
    "base": "https://basescan.org/address/",
    "ethereum": "https://etherscan.io/address/",
}


def explorer_link_md(chain: str, address: str, label: Optional[str] = None) -> str:
    """Render a MarkdownV2 link to a block explorer for an address.

    Fails closed: if chain is unknown, returns escaped address without link
    (prevents user trust issue of wrong-chain explorer links).
    """
    base = EXPLORER_URLS.get(chain)
    if not base:
        return code_md(address)
    display = label if label is not None else address
    return link_md(display, f"{base}{address}")


def format_epoch_countdown(
    epoch_number: int,
    epoch_end_utc: Optional[str],
    remaining_seconds: float = 0.0,
) -> str:
    """Format an epoch countdown line (MarkdownV2).

    Prefers `epoch_end_utc` (ISO 8601); falls back to `remaining_seconds`.
    """
    if epoch_end_utc:
        try:
            epoch_end = datetime.fromisoformat(epoch_end_utc)
            remaining_seconds = (epoch_end - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError) as e:
            logger.debug(f"Could not parse epoch_end_utc={epoch_end_utc!r}: {e}")

    label = f"Epoch {epoch_number}" if epoch_number else "Epoch"
    if remaining_seconds >= 0:
        h = int(remaining_seconds / 3600)
        m = int((remaining_seconds % 3600) / 60)
        time_str = f"{h}h {m}m"
        return f"{escape_md(label)} ends in: {code_md(time_str)}"
    abs_s = abs(remaining_seconds)
    h = int(abs_s / 3600)
    m = int((abs_s % 3600) / 60)
    time_str = f"-{h}h {m}m ⚠️"
    return f"{escape_md(label)} ended: {code_md(time_str)}"


def _categorize_exception(exc: BaseException) -> str:
    """Classify an exception for user-facing display.

    R2-M6: prefer `isinstance` over substring matching on type names.
    Substring matching misclassified types like `HttpValidationError`
    (matched "http" → "RPC unavailable" instead of "Invalid data").
    Fall back to substring only for exception classes we cannot import
    without introducing hard dependencies.

    R3-L5: `asyncio.TimeoutError` is an alias of the builtin `TimeoutError`
    since Python 3.11 (the project's minimum), so only `TimeoutError` is
    listed in the tuple.
    """
    if isinstance(exc, TimeoutError):
        return "RPC timeout"
    if isinstance(exc, (ConnectionError, OSError)):
        return "RPC unavailable"
    if isinstance(exc, (ValueError, TypeError)):
        return "Invalid data"

    # web3 / requests / httpx exceptions are only probed by name to avoid
    # importing those packages into formatting.py.
    name = type(exc).__name__.lower()
    if "timeout" in name:
        return "RPC timeout"
    if "contractlogicerror" in name or "revert" in name:
        return "Contract reverted"
    if "validation" in name or "badrequest" in name:
        return "Invalid data"
    if "httperror" in name or "connectionerror" in name or "rpcerror" in name:
        return "RPC unavailable"
    return "Error"


def user_error(context: str, exc: BaseException) -> str:
    """Render a user-facing error message that NEVER leaks exception internals.

    Logs the full exception server-side and returns a generic, categorized
    message for display to the user.

    This exists because `str(exc)` on Web3/requests errors often includes full
    RPC URLs with embedded API keys, which would leak to the chat.
    """
    # R2-SEC2: the server-side log uses the developer-provided `context` only.
    # `logger.opt(exception=True)` attaches the full traceback (with type
    # name and str(exc)) to the log record automatically — we don't need to
    # inline `type(exc).__name__` in the message string (R3-L1: would be
    # duplicate information). The traceback goes to the configured loguru
    # sink (file/stderr), never to the chat.
    logger.opt(exception=True).error(f"{context} failed")

    category = _categorize_exception(exc)
    return f"{category} \\({escape_md(context)}\\) — check logs"


def format_token(amount: Optional[float], symbol: str) -> str:
    """Format token amount for MarkdownV2 display."""
    if amount is None:
        return f"? {symbol}"
    return f"{amount:,.2f} {symbol}"


def format_currency(amount: float) -> str:
    """Format EUR amount."""
    return f"€{amount:,.2f}"


MAX_MD_MESSAGE_LENGTH = 3900


def split_md_blocks(
    blocks: List[str],
    header: Optional[str] = None,
    max_length: int = MAX_MD_MESSAGE_LENGTH,
    separator: str = "\n\n",
) -> List[str]:
    """Split MarkdownV2 text blocks into messages within Telegram's limit."""
    messages: List[str] = []
    current = header if header else ""

    for block in blocks:
        candidate = current + separator + block if current else block
        if len(candidate) > max_length:
            if current:
                messages.append(current)
            current = block
        else:
            current = candidate

    if current:
        messages.append(current)

    return messages
