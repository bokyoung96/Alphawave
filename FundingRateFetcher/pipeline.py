import pandas as pd
import multiprocessing
from tqdm import tqdm
from functools import reduce
from concurrent.futures import ThreadPoolExecutor, as_completed

from tools import Tools
from exchange import ExchangeManager
from fetcher import FundingRatesFilter, LoadMarketsFilter, BidAskFilter, SnapShotFetcher


class PipelineManager:
    def __init__(self,
                 exch_mgr: ExchangeManager,
                 get_fr: bool = True,
                 get_lm: bool = True,
                 get_ba: bool = True):
        self.exch_mgr = exch_mgr
        self._exchanges = self.exch_mgr.exchanges
        self._configs = self.exch_mgr.configs

        self.fetcher = SnapShotFetcher()

        self.get_fr = get_fr
        self.get_lm = get_lm
        self.get_ba = get_ba

        self.pipeline: dict[str, dict[str, dict]] = None
        self.history: pd.DataFrame = None

    def run(self):
        filters = [
            (self.get_fr, FundingRatesFilter),
            (self.get_lm, LoadMarketsFilter),
            (self.get_ba, BidAskFilter),
        ]
        for enabled, fcls in filters:
            if enabled:
                self.fetcher.add_filter(fcls(self.exch_mgr), enabled=True)

        self.pipeline = self.fetcher.run()
        self.history = self.fetcher.history


class PipelineFinder(PipelineManager):
    def __init__(self,
                 exch_mgr: ExchangeManager,
                 get_fr: bool = True,
                 get_lm: bool = True,
                 get_ba: bool = True):
        super().__init__(exch_mgr, get_fr, get_lm, get_ba)
        self._max_workers = min(
            multiprocessing.cpu_count(), len(self._exchanges))

    @classmethod
    def load_pipeline(cls,
                      exch_mgr: ExchangeManager,
                      get_fr: bool = True,
                      get_lm: bool = True,
                      get_ba: bool = True):
        inst = cls(exch_mgr, get_fr, get_lm, get_ba)
        inst.run()
        return inst

    def _exchange_finder(self,
                         exch_name: str) -> pd.DataFrame:
        if not self.pipeline:
            print("No pipeline result found.")
            return {}

        dfs = []
        for filter_name, exch_dict in self.pipeline.items():
            data: dict = exch_dict.get(exch_name)

            if not data:
                print(
                    f"[Warning] '{filter_name}' has NO data for '{exch_name}'")
                continue

            df = pd.DataFrame(data)
            if df.empty:
                print(
                    f"[Warning] '{filter_name}' DataFrame is empty for '{exch_name}'")
                continue

            df.set_index("ticker", inplace=True)
            dfs.append(df)

        if not dfs:
            print(f"No data found for exchange: {exch_name}")
            return {}

        temp = reduce(lambda left, right: left.join(right, how='outer'), dfs)
        res = Tools.filter_symbols(df=temp, base='funding_rate')
        return res

    def multi_exchange_finder(self) -> dict[str, pd.DataFrame]:
        exch_names = list(self._exchanges.keys())
        res = {}

        def _load_datas(exch_name):
            df = self._exchange_finder(exch_name)
            return exch_name, df

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = []
            for exch_name in exch_names:
                fut = executor.submit(_load_datas, exch_name)
                futures.append(fut)

            with tqdm(total=futures.__len__(), desc='Processing multi exchange finder') as pbar:
                for f in as_completed(futures):
                    try:
                        exch_name, df_result = f.result()
                        res[exch_name] = df_result
                    except Exception as exc:
                        print(f"[Error] exchange={exch_name} exception={exc}")
                    finally:
                        pbar.update(1)
        return res

    @staticmethod
    def ticker_finder(data_map: dict[str, pd.DataFrame],
                      ticker: str) -> pd.DataFrame:
        datas = {}
        for exch_name, df in data_map.items():
            if ticker in df.index:
                row = df.loc[ticker].to_dict()
                datas[exch_name] = row

        if not datas:
            return pd.DataFrame()

        res = pd.DataFrame.from_dict(datas, orient='index')
        return res


if __name__ == "__main__":
    exch_mgr = ExchangeManager(registry=None)
    finder = PipelineFinder.load_pipeline(exch_mgr=exch_mgr)

    data_map = finder.multi_exchange_finder()
    res = finder.ticker_finder(data_map=data_map,
                               ticker='ACE')
