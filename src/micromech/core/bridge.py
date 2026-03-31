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
        """Lazy-load wallet."""
        if self._wallet is None:
            self._wallet = Wallet()
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

    def get_contract(self, contract_class: type, address: str) -> Any:
        """Instantiate an iwa ContractInstance."""
        return contract_class(address, self.chain_name)

    def with_retry(self, fn: Any, **kwargs: Any) -> Any:
        """Execute a function with iwa's RPC auto-rotation."""
        return self.chain_interface.with_retry(fn, **kwargs)


# Cached instances for balance checking (avoid re-probing RPCs on every call)
_cached_wallet: Optional[Any] = None
_cached_interfaces: Optional[Any] = None


def check_balances(chain_name: str) -> tuple[float, float]:
    """Check native token and OLAS balances for the wallet on a chain.

    Returns (native_balance, olas_balance) in whole units.
    Caches Wallet and ChainInterfaces across calls to avoid repeated RPC probing.
    """
    global _cached_wallet, _cached_interfaces  # noqa: PLW0603

    try:
        if not _IWA_AVAILABLE:
            return 0.0, 0.0

        if _cached_wallet is None:
            _cached_wallet = Wallet()
        if _cached_interfaces is None:
            _cached_interfaces = ChainInterfaces()

        address = _cached_wallet.address
        ci = _cached_interfaces.get(chain_name)
        if not ci:
            return 0.0, 0.0

        native_wei = ci.with_retry(lambda: ci.web3.eth.get_balance(address))
        native = native_wei / 1e18

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
                # olas_addr is already an EthereumAddress (checksummed)
                contract = ci.web3.eth.contract(
                    address=str(olas_addr), abi=erc20_abi
                )
                raw = ci.with_retry(
                    lambda: contract.functions.balanceOf(address).call()
                )
                olas_balance = raw / 1e18
        except Exception:
            logger.debug("Failed to check OLAS balance on {}", chain_name)

        return native, olas_balance
    except Exception:
        logger.debug("Failed to check balances on {}", chain_name)
        return 0.0, 0.0
