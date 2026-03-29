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
