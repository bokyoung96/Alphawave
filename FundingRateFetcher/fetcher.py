import multiprocessing
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from tools import Tools
from register import ExchangeManager

"""
1. USDT/C both required?
 - If not, parsing will be required before accessing datas.
 
 2. Data copy required?
 - Inplace method should be used with caution.
"""


class SnapShotFetcher:
    def __init__(self, exch_mgr: ExchangeManager):
        self.mgr = exch_mgr
        self._exchanges = exch_mgr.exchanges
        self._configs = exch_mgr.configs

    @property
    def max_workers(self):
        wkrs = min(multiprocessing.cpu_count(),
                   self._exchanges.__len__())
        return wkrs

    def fetch_raw_snapshots(self) -> dict[str, pd.DataFrame]:
        if not self._exchanges:
            return {}

        def _fetch_snapshot(exch_name: str,
                            exch) -> pd.DataFrame:
            df = pd.DataFrame(exch.fetchFundingRates())
            return exch_name, df

        snapshot = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for exch_name, exchange in self._exchanges.items():
                fut = executor.submit(_fetch_snapshot,
                                      exch_name,
                                      exchange)
                futures.append(fut)

            with tqdm(total=futures.__len__(), desc='Fetching snapshots') as pbar:
                for f in as_completed(futures):
                    exch_name, df_result = f.result()
                    snapshot[exch_name] = df_result
                    pbar.update(1)
        return snapshot

    def get_snapshots(self,
                      snapshot: dict[str, pd.DataFrame],
                      base_exch: str = 'hyperliquid') -> dict[str, pd.DataFrame]:
        if not snapshot.__contains__(base_exch):
            return snapshot

        base_df = snapshot[base_exch]
        base_map = base_df.loc['symbol'].apply(Tools.get_base_symbol)

        def _get_snapshot(exch_name: str,
                          df: pd.DataFrame,
                          valid_syms) -> None:
            syms = df.loc['symbol'].apply(Tools.get_base_symbol)
            valid_cols = syms[syms.isin(values=valid_syms)].index
            df.drop(columns=[col for col in df.columns if col not in valid_cols],
                    inplace=True)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for exch_name, df in snapshot.items():
                fut = executor.submit(_get_snapshot,
                                      exch_name,
                                      df,
                                      base_map)

            for _ in as_completed(futures):
                pass
        return snapshot

    @property
    def raw_snapshot(self):
        return self.fetch_raw_snapshots()

    @property
    def snapshot(self):
        return self.get_snapshots()


if __name__ == "__main__":
    cls1 = ExchangeManager(registry=None)
    cls2 = SnapShotFetcher(exch_mgr=cls1)
    res = cls2.fetch_raw_snapshots()
    res2 = cls2.get_snapshots(snapshot=res,
                              base_exch='hyperliquid')
