"""Bridge to iwa framework for wallet, chain, and contract operations.

iwa is an optional dependency. When not installed, operations that
require it will raise ImportError with a clear message.
"""

from typing import Any, Optional

from loguru import logger

_IWA_AVAILABLE = False
try:
    from iwa.core.chain import ChainInterfaces
    from iwa.core.wallet import Wallet

    _IWA_AVAILABLE = True
except ImportError:
    pass


def require_iwa() -> None:
    """Raise if iwa is not installed."""
    if not _IWA_AVAILABLE:
        msg = "iwa is required for chain operations. Install it with: pip install iwa"
        raise ImportError(msg)


class IwaBridge:
    """Wrapper to use iwa services from micromech.

    Provides:
    - Wallet (key management, signing)
    - ChainInterface (web3, RPC rotation)
    - Contract interactions (mech, marketplace, staking)
    """

    def __init__(self, chain_name: str = "gnosis") -> None:
        require_iwa()
        self.chain_name = chain_name
        self._wallet: Optional[Any] = None
        self._chain_interface: Optional[Any] = None

    @property
    def wallet(self) -> Any:
        """Lazy-load wallet via get_wallet() (respects wizard password)."""
        if self._wallet is None:
            self._wallet = get_wallet()
            logger.debug("iwa Wallet initialized")
        return self._wallet

    @property
    def chain_interface(self) -> Any:
        """Lazy-load chain interface with RPC rotation."""
        if self._chain_interface is None:
            interfaces = ChainInterfaces()
            ci = interfaces.get(self.chain_name)
            if ci is None:
                msg = f"Chain '{self.chain_name}' not found in iwa ChainInterfaces"
                raise ValueError(msg)
            self._chain_interface = ci
            logger.debug("iwa ChainInterface initialized for {}", self.chain_name)
        return self._chain_interface

    @property
    def web3(self) -> Any:
        """Get web3 instance (with RPC rotation)."""
        return self.chain_interface.web3

    def with_retry(self, fn: Any, **kwargs: Any) -> Any:
        """Execute a function with iwa's RPC auto-rotation."""
        return self.chain_interface.with_retry(fn, **kwargs)


def create_bridges(config: Any) -> dict[str, "IwaBridge"]:
    """Create IwaBridge instances for all enabled chains in config."""
    bridges: dict = {}
    try:
        for chain_name in config.enabled_chains:
            try:
                bridges[chain_name] = IwaBridge(chain_name=chain_name)
            except Exception as e:
                logger.warning("Bridge failed for {}: {}", chain_name, e)
    except Exception:
        logger.warning("iwa not available — running without chain access")
    return bridges


# Cached instances (avoid re-probing RPCs on every call)
_cached_wallet: Optional[Any] = None
_cached_interfaces: Optional[Any] = None
# Set by web setup wizard (POST /api/setup/wallet)
_cached_key_storage: Optional[Any] = None


def get_wallet() -> Any:
    """Get or create a Wallet instance.

    Priority:
    1. Return cached wallet if available.
    2. If web wizard set _cached_key_storage, build Wallet from it
       (uses the password the user entered in the wizard).
    3. Try standard Wallet() which reads wallet_password from env/secrets.
       Only if wallet file already exists (never auto-create).
    """
    global _cached_wallet  # noqa: PLW0603

    if _cached_wallet is not None:
        return _cached_wallet

    # Path A: Web wizard provided a KeyStorage with user's password.
    # Build Wallet manually to ensure the wizard password is used for signing.
    if _cached_key_storage is not None:
        logger.debug("get_wallet: building from wizard KeyStorage")

        from iwa.core.db import init_db
        from iwa.core.wallet import (
            AccountService,
            BalanceService,
            PluginService,
            SafeService,
            TransactionService,
            TransferService,
        )

        wallet = object.__new__(Wallet)
        wallet.key_storage = _cached_key_storage
        wallet.account_service = AccountService(_cached_key_storage)
        wallet.balance_service = BalanceService(
            _cached_key_storage, wallet.account_service,
        )
        wallet.safe_service = SafeService(
            _cached_key_storage, wallet.account_service,
        )
        wallet.transaction_service = TransactionService(
            _cached_key_storage, wallet.account_service, wallet.safe_service,
        )
        wallet.transfer_service = TransferService(
            _cached_key_storage, wallet.account_service,
            wallet.balance_service, wallet.safe_service,
            wallet.transaction_service,
        )
        wallet.plugin_service = PluginService()
        wallet.chain_interfaces = ChainInterfaces()
        init_db()

        _cached_wallet = wallet
        return _cached_wallet

    # Path B: No wizard — try standard Wallet() (env password).
    # Only if wallet file exists (never auto-create).
    from pathlib import Path
    from iwa.core.constants import WALLET_PATH
    if Path(WALLET_PATH).exists():
        try:
            _cached_wallet = Wallet()
            if not hasattr(_cached_wallet, "chain_interfaces"):
                _cached_wallet.chain_interfaces = ChainInterfaces()
            return _cached_wallet
        except (AttributeError, TypeError):
            logger.debug("Wallet() failed")

    msg = "No wallet available. Use the web wizard or set wallet_password in secrets.env."
    raise RuntimeError(msg)


def check_balances(chain_name: str) -> tuple[float, float]:
    """Check native token and OLAS balances for the wallet on a chain.

    Returns (native_balance, olas_balance) in whole units.
    All RPC calls go through iwa's ChainInterface.
    """
    global _cached_wallet, _cached_interfaces  # noqa: PLW0603

    try:
        if not _IWA_AVAILABLE:
            return 0.0, 0.0

        # Get address from cached key_storage (web wizard) or Wallet.
        # Never create a new Wallet — only use what's already unlocked.
        address = None
        if _cached_key_storage is not None:
            address = str(_cached_key_storage.get_address_by_tag("master"))
        elif _cached_wallet is not None:
            address = _cached_wallet.master_account.address
        else:
            return 0.0, 0.0

        if not address:
            return 0.0, 0.0

        if _cached_interfaces is None:
            _cached_interfaces = ChainInterfaces()
        ci = _cached_interfaces.get(chain_name)
        if not ci:
            return 0.0, 0.0

        native_wei = ci.with_retry(
            lambda: ci.web3.eth.get_balance(address),
        )
        native = float(ci.web3.from_wei(native_wei, "ether"))

        # Get OLAS balance
        olas_balance = 0.0
        try:
            chain_model = ci.chain
            olas_addr = chain_model.get_token_address("OLAS")
            if olas_addr:
                erc20_abi = [
                    {
                        "inputs": [{"name": "account", "type": "address"}],
                        "name": "balanceOf",
                        "outputs": [{"name": "", "type": "uint256"}],
                        "stateMutability": "view",
                        "type": "function",
                    }
                ]
                contract = ci.web3.eth.contract(
                    address=str(olas_addr), abi=erc20_abi
                )
                raw = ci.with_retry(
                    lambda: contract.functions.balanceOf(address).call(),
                )
                olas_balance = float(ci.web3.from_wei(raw, "ether"))
        except Exception:
            logger.debug("Failed to check OLAS balance on {}", chain_name)

        return native, olas_balance
    except Exception:
        logger.debug("Failed to check balances on {}", chain_name)
        return 0.0, 0.0
