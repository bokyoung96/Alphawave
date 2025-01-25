import multiprocessing
import pandas as pd
from tqdm import tqdm
from abc import ABC, abstractmethod
from typing import List, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from tools import Tools
from exchange import ExchangeManager


class DataFilter(ABC):
    def __init__(self, exch_mgr: ExchangeManager):
        self._exchanges = exch_mgr.exchanges
        self._configs = exch_mgr.configs

        self._max_workers = min(
            multiprocessing.cpu_count(), len(self._exchanges))

    @property
    def max_workers(self) -> int:
        return self._max_workers

    @abstractmethod
    def apply(self) -> dict[str, dict]:
        pass


class FundingRatesFilter(DataFilter):
    def __init__(self, exch_mgr: ExchangeManager):
        super().__init__(exch_mgr)

    def apply(self) -> dict[str, dict]:
        if not self._exchanges:
            return {}

        def _load_datas(exch_name: str,
                        exch) -> pd.DataFrame:
            params = {'subtype': 'linear', 'error': 'error'}
            df = pd.DataFrame(Tools.safe_execute(exch.fetchFundingRates,
                                                 params=params))

            res = {
                'symbol': df.loc['symbol'],
                'funding_rate': df.loc['fundingRate'],
                'fundingTimestamp': df.loc['fundingTimestamp'].apply(Tools.convert_timestamp_to_kst),
                'index_price': df.loc['indexPrice']
            }
            return exch_name, res

        snapshot = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for exch_name, exchange in self._exchanges.items():
                fut = executor.submit(_load_datas,
                                      exch_name,
                                      exchange)
                futures.append(fut)

            with tqdm(total=futures.__len__(), desc='Fetching funding rates') as pbar:
                for f in as_completed(futures):
                    exch_name, df_result = f.result()
                    snapshot[exch_name] = df_result
                    pbar.update(1)
        return snapshot


class LoadMarketsFilter(DataFilter):
    def __init__(self, exch_mgr: ExchangeManager):
        super().__init__(exch_mgr)

    def apply(self) -> dict[str, dict]:
        if not self._exchanges:
            return {}

        def _load_datas(exch_name: str,
                        exch) -> pd.DataFrame:
            temp = pd.DataFrame(exch.loadMarkets())
            df = temp.loc[:, temp.loc['swap']]

            res = {
                'ticker': df.loc['base'],
                'price_decimal': df.loc['precision'].apply(lambda prec_dict: Tools.convert_precision_to_decimal(prec_dict['price'])),
                'size_decimal': df.loc['precision'].apply(lambda prec_dict: Tools.convert_precision_to_decimal(prec_dict['amount'])),
                'max_leverage': df.loc['limits'].apply(lambda limits_dict: limits_dict['leverage']['max']),
                'taker': df.loc['taker'],
                'maker': df.loc['maker']
            }
            return exch_name, res

        snapshot = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for exch_name, exchange in self._exchanges.items():
                fut = executor.submit(_load_datas,
                                      exch_name,
                                      exchange)
                futures.append(fut)

            with tqdm(total=futures.__len__(), desc='Fetching load markets') as pbar:
                for f in as_completed(futures):
                    exch_name, df_result = f.result()
                    snapshot[exch_name] = df_result
                    pbar.update(1)
        return snapshot


class BidAskFilter(DataFilter):
    def __init__(self, exch_mgr: ExchangeManager):
        super().__init__(exch_mgr)

    def apply(self) -> dict[str, dict]:
        if not self._exchanges:
            return {}

        def _load_datas(exch_name: str,
                        exch) -> pd.DataFrame:
            params = {'type': 'swap', 'subtype': 'linear'}
            df = pd.DataFrame(Tools.safe_execute(exch.fetchTickers,
                                                 params=params))

            res = {
                'bid': df.loc['bid'],
                'ask': df.loc['ask'],
                'quoteVolume': df.loc['quoteVolume'],
                'price': df.loc['last']
            }
            return exch_name, res

        snapshot = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for exch_name, exchange in self._exchanges.items():
                fut = executor.submit(_load_datas,
                                      exch_name,
                                      exchange)
                futures.append(fut)

            with tqdm(total=futures.__len__(), desc='Fetching bid asks') as pbar:
                for f in as_completed(futures):
                    exch_name, df_result = f.result()
                    snapshot[exch_name] = df_result
                    pbar.update(1)
        return snapshot


class SnapShotFetcher:
    def __init__(self):
        self.steps: List[Tuple[DataFilter, bool]] = []
        self._filter_history: List = []

    def add_filter(self, flt: DataFilter, enabled: bool = True):
        self.steps.append((flt, enabled))

    def run(self) -> dict[str, dict[str, dict]]:
        res = {}
        for (flt, enabled) in self.steps:
            filter_name = flt.__class__.__name__

            if not enabled:
                self._filter_history.append(
                    (filter_name, "Skipped", None)
                )
                continue

            try:
                snapshot = flt.apply()
                self._filter_history.append(
                    (filter_name, "Ran", snapshot)
                )
                res[filter_name] = snapshot
            except Exception as e:
                self._filter_history.append(
                    (filter_name, f"Error: {str(e)}", None)
                )
        return res

    @property
    def history(self) -> pd.DataFrame:
        df = []
        for (fname, status, snapshot) in self._filter_history:
            snap_str = str(snapshot)[:100] + "..." if snapshot else None
            df.append({
                "Filter": fname,
                "Status": status,
                "SnapshotPreview": snap_str
            })
        return pd.DataFrame(df)
