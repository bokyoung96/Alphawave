import ccxt
from enum import Enum, unique


@unique
class Exchanges(Enum):
    HYPERLIQUID = "hyperliquid"
    BINANCE = "binance"
    BYBIT = "bybit"
    BITGET = "bitget"


@unique
class Stables(Enum):
    USDC = "USDC"
    USDT = "USDT"


@unique
class RateLimit(Enum):
    STANDARD = 50


class CoinConfig:
    def __init__(self, exchange: Exchanges, stable: Stables, rate_limit: RateLimit):
        self.exchange = exchange
        self.stable = stable
        self.rate_limit = rate_limit

    def __repr__(self):
        return f"<CoinConfig exchange={self.exchange.value}, stable={self.stable.value}, rate_limit={self.rate_limit}>"


class CoinRegister:
    def __init__(self):
        self._configs = []

    def add_config(self, config: CoinConfig):
        self._configs.append(config)

    def get_all_configs(self):
        return list(self._configs)

    def __len__(self):
        return len(self._configs)

    def __repr__(self):
        return f"<CoinRegister size={len(self._configs)}>"


class ExchangeManager:
    def __init__(self, registry: CoinRegister = None):
        if registry is None:
            registry = default_registry()

        self._registry = registry
        self._exchanges = {}

        self._initialize_exchanges()

    def _initialize_exchanges(self):
        for conf in self._registry.get_all_configs():
            exch_name = conf.exchange.value
            stable = conf.stable.value
            rate_limit = conf.rate_limit.value
            try:
                exchange_class = getattr(ccxt, exch_name)
                exchange = exchange_class({'enableRateLimit': True})
                exchange.load_markets()
                exchange.rateLimit = rate_limit

                self._exchanges[exch_name] = exchange
                print(
                    f"[ExchangeManager] Initialized exchange: {exch_name} (stable={stable})")
            except Exception as e:
                print(
                    f"[ExchangeManager] Error initializing {exch_name}: {str(e)}")

    @property
    def exchanges(self):
        return self._exchanges

    @property
    def configs(self):
        return self._registry.get_all_configs()


def default_registry():
    registry = CoinRegister()
    registry.add_config(CoinConfig(Exchanges.HYPERLIQUID,
                                   Stables.USDC,
                                   RateLimit.STANDARD))
    registry.add_config(CoinConfig(Exchanges.BINANCE,
                                   Stables.USDT,
                                   RateLimit.STANDARD))
    registry.add_config(CoinConfig(Exchanges.BYBIT,
                                   Stables.USDT,
                                   RateLimit.STANDARD))
    registry.add_config(CoinConfig(Exchanges.BITGET,
                                   Stables.USDT,
                                   RateLimit.STANDARD))
    return registry


if __name__ == "__main__":
    exch_mgr = ExchangeManager(registry=None)
