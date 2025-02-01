import pandas as pd
from tqdm import tqdm
from typing import NamedTuple, Callable, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from exchange import ExchangeManager
from fetcher import DataFilter
from tools import Tools


class ExceptionRegister(NamedTuple):
    name: str
    func: Callable[[str, Any], tuple[str, dict]]
    target_exchanges: list[str] | None = None
    target_filter: Optional[str] = None


class ExceptionFilter(DataFilter):
    def __init__(self, exch_mgr: ExchangeManager):
        super().__init__(exch_mgr)

        self.exception_methods = [
            ExceptionRegister(
                "fetchBidsAsks",
                self._load_exception_fetchBidsAsks,
                target_exchanges=["binance"],
                target_filter='BidAskFilter'
            ),
            ExceptionRegister(
                "fetchFundingIntervals",
                self._load_exception_fetchFundingIntervals,
                target_exchanges=["binance"],
                target_filter='FundingRatesFilter'
            ),
            ExceptionRegister(
                "fetchTradingFees",
                self._load_exception_fetchTradingFees,
                target_exchanges=["bitget"],
                target_filter='FundingRatesFilter'
            ),
        ]

    def _load_exception_fetchBidsAsks(self, exch_name: str, exch) -> tuple[str, dict]:
        params = {'type': 'swap'}
        df = pd.DataFrame(Tools.safe_execute(
            exch.fetchBidsAsks, params=params))
        return exch_name, {
            'bid': df.loc['bid'],
            'ask': df.loc['ask'],
            'bid_volume': df.loc['bidVolume'],
            'ask_volume': df.loc['askVolume'],
        }

    def _load_exception_fetchFundingIntervals(self, exch_name: str, exch) -> tuple[str, dict]:
        params = {'type': 'swap'}
        df = pd.DataFrame(Tools.safe_execute(
            exch.fetchFundingIntervals, params=params))
        return exch_name, {'interval': df.loc['interval'].apply(Tools.convert_interval_to_float)}

    def _load_exception_fetchTradingFees(self, exch_name: str, exch) -> tuple[str, dict]:
        params = {'type': 'swap'}
        df = pd.DataFrame(Tools.safe_execute(
            exch.fetchTradingFees, params=params))
        return exch_name, {
            'interval': df.loc['info'].apply(lambda x: x['fundInterval'])
        }

    def apply(self) -> dict[str, dict]:
        if not self._exchanges:
            return {}

        snapshot: dict[str, dict] = {exch_name: {}
                                     for exch_name in self._exchanges}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {}

            for method in self.exception_methods:
                if method.target_exchanges is None:
                    target_exchanges = self._exchanges.keys()
                else:
                    target_exchanges = [
                        exch for exch in method.target_exchanges
                        if exch in self._exchanges
                    ]

                for exch_name in target_exchanges:
                    exch = self._exchanges[exch_name]
                    future = executor.submit(method.func, exch_name, exch)
                    future_map[future] = method.name

            with tqdm(total=future_map.__len__(), desc="Fetching exceptions") as pbar:
                for fut in as_completed(future_map):
                    method_name = future_map[fut]
                    exch_name, result_dict = fut.result()
                    snapshot[exch_name][method_name] = result_dict
                    pbar.update(1)

        return snapshot
