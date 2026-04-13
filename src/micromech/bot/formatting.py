"""Telegram message formatting utilities (HTML parse mode)."""

from typing import Optional


def escape_html(text: str) -> str:
    """Escape HTML special characters for Telegram."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def bold(text: str) -> str:
    """Wrap text in bold tags."""
    return f"<b>{escape_html(text)}</b>"


def code(text: str) -> str:
    """Wrap text in code tags."""
    return f"<code>{escape_html(text)}</code>"


def format_balance(amount: Optional[float], symbol: str) -> str:
    """Format a token balance with symbol."""
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
    """Split a list of text blocks into messages that fit within Telegram's limit.

    Each block is kept whole — not split mid-block.
    Returns a list of message strings ready to send.
    """
    messages: list[str] = []
    current_parts: list[str] = []
    current_len = 0

    for block in blocks:
        needed = len(block) + (2 if current_parts else 0)  # 2 for "\n\n" separator
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
    """Format a per-chain status block."""
    lines = [bold(chain.upper())]

    state = status.get("staking_state", "unknown")
    lines.append(f"State: {code(state)}")

    requests = status.get("requests_this_epoch", 0)
    required = status.get("required_requests", 0)
    lines.append(f"Deliveries: {code(f'{requests}/{required}')}")

    rewards = status.get("rewards", 0)
    lines.append(f"Rewards: {code(format_balance(rewards, 'OLAS'))}")

    return "\n".join(lines)
