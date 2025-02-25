import ccxt
import logging
import multiprocessing
from typing import Dict, Any
from enum import Enum, unique
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from exceptionExch import ExceptionExchange


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@unique
class Exchanges(Enum):
    HYPERLIQUID = "hyperliquid"
    BINANCE = "binance"
    BYBIT = "bybit"
    BITGET = "bitget"
    GATEIO = "gateio"


@unique
class DefaultType(Enum):
    SWAP = "swap"
    FUTURE = "future"


@dataclass
class CoinConfig:
    exchange: Exchanges
    default_type: DefaultType

    def get_params(self) -> Dict[str, Any]:
        return {'defaultType': self.default_type.value}


class CoinRegister:
    def __init__(self) -> None:
        self._configs = []

    def add_config(self, config: CoinConfig) -> None:
        self._configs.append(config)

    def get_all_configs(self):
        return list(self._configs)

    def __len__(self) -> int:
        return len(self._configs)

    def __repr__(self) -> str:
        return f"<CoinRegister size={len(self._configs)}>"


class ExchangeManager:
    def __init__(self, registry: CoinRegister = None) -> None:
        if registry is None:
            registry = default_registry()
        self._registry = registry
        self._exchanges = {}
        self._initialize_exchanges()

    def _initialize_exchanges(self) -> None:
        def initialize_exchange(conf: CoinConfig):
            exch_name = conf.exchange.value
            try:
                exchange_class = getattr(ccxt, exch_name)
                exchange = exchange_class()

                params = conf.get_params()
                exchange.options.update(params)
                exchange = ExceptionExchange(exchange=exchange)
                logger.info(
                    f"[ExchangeManager] Initialized exchange: {exch_name} with params: {params}")
                return exch_name, exchange
            except Exception as e:
                logger.error(
                    f"[ExchangeManager] Error initializing {exch_name}: {str(e)}")
                return None

        futures = []
        max_workers = min(multiprocessing.cpu_count(), len(self._registry))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for conf in self._registry.get_all_configs():
                futures.append(executor.submit(initialize_exchange, conf))
            for f in as_completed(futures):
                result = f.result()
                if result is not None:
                    exch_name, exchange = result
                    self._exchanges[exch_name] = exchange

    @property
    def exchanges(self):
        return self._exchanges

    @property
    def configs(self):
        return self._registry.get_all_configs()


def default_registry() -> CoinRegister:
    registry = CoinRegister()
    registry.add_config(CoinConfig(Exchanges.HYPERLIQUID, DefaultType.SWAP))
    registry.add_config(CoinConfig(Exchanges.BINANCE, DefaultType.SWAP))
    registry.add_config(CoinConfig(Exchanges.BYBIT, DefaultType.SWAP))
    registry.add_config(CoinConfig(Exchanges.BITGET, DefaultType.SWAP))
    registry.add_config(CoinConfig(Exchanges.GATEIO, DefaultType.SWAP))
    return registry
