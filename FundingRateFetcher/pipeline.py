import pytz
import numpy as np
import pandas as pd
import multiprocessing
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from tools import Tools
from exchange import ExchangeManager
from fetcher import FundingRatesFilter, LoadMarketsFilter, BidAskFilter, SnapShotFetcher
from exception import ExceptionFilter

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class PipelineManager:
    def __init__(self,
                 exch_mgr: ExchangeManager,
                 get_fr: bool = True,
                 get_lm: bool = True,
                 get_ba: bool = True,
                 get_ex: bool = True):
        self.exch_mgr = exch_mgr
        self._exchanges = self.exch_mgr.exchanges
        self._configs = self.exch_mgr.configs

        self.fetcher = SnapShotFetcher()

        self.get_fr = get_fr
        self.get_lm = get_lm
        self.get_ba = get_ba
        self.get_ex = get_ex

        self.pipeline: dict[str, dict[str, dict]] = None
        self.history: pd.DataFrame = None

    def run(self):
        filters = [
            (self.get_fr, FundingRatesFilter),
            (self.get_lm, LoadMarketsFilter),
            (self.get_ba, BidAskFilter),
            (self.get_ex, ExceptionFilter)
        ]
        for enabled, fcls in filters:
            if enabled:
                self.fetcher.add_filter(fcls(self.exch_mgr), enabled=True)

        self.pipeline = self.fetcher.run()
        self.history = self.fetcher.history

        self._merge_exceptions()

    def _merge_exceptions(self):
        ex_flt_data = self.pipeline.get("ExceptionFilter")
        if not ex_flt_data:
            return

        ex_flt_inst = None
        for (flt, enabled) in self.fetcher.steps:
            if isinstance(flt, ExceptionFilter) and enabled:
                ex_flt_inst = flt
                break

        if not ex_flt_inst:
            return

        method_map = {
            m.name: m.target_filter for m in ex_flt_inst.exception_methods}

        for exch_name, methods_dict in ex_flt_data.items():
            for method_name, exc_res_dict in methods_dict.items():
                target_flt_name = method_map.get(method_name)
                if not target_flt_name:
                    continue

                target_filter_res = self.pipeline.get(target_flt_name, {})
                if not target_filter_res:
                    continue

                main_dict = target_filter_res.get(exch_name, {})
                if not main_dict:
                    continue

                Tools.override_if_exists(main_dict=main_dict,
                                         exc_dict=exc_res_dict)


class PipelineMerger(PipelineManager):
    def __init__(self,
                 exch_mgr: ExchangeManager,
                 get_fr: bool = True,
                 get_lm: bool = True,
                 get_ba: bool = True,
                 get_ex: bool = True):
        super().__init__(exch_mgr, get_fr, get_lm, get_ba, get_ex)
        self._max_workers = min(
            multiprocessing.cpu_count(), len(self._exchanges))
        self.data_map = None

    @classmethod
    def load_pipeline(cls,
                      exch_mgr: ExchangeManager,
                      get_fr: bool = True,
                      get_lm: bool = True,
                      get_ba: bool = True,
                      get_ex: bool = True):
        inst = cls(exch_mgr, get_fr, get_lm, get_ba, get_ex)
        inst.run()
        inst.data_map = inst.multi_exchange_merger()
        return inst

    def _exchange_merger(self,
                         exch_name: str) -> pd.DataFrame:
        if not self.pipeline:
            logging.warning("No pipeline result found.")
            return {}

        flt_pipeline = {
            filter_name: exch_dict
            for filter_name, exch_dict in self.pipeline.items()
            if filter_name != "ExceptionFilter"
        }

        dfs = []
        for filter_name, exch_dict in flt_pipeline.items():
            data: dict = exch_dict.get(exch_name)

            if not data:
                logging.warning(
                    f"'{filter_name}' has no data for '{exch_name}'")
                continue

            df = pd.DataFrame(data)
            if df.empty:
                logging.warning(
                    f"'{filter_name}' DataFrame is empty for '{exch_name}'")
                continue

            dfs.append(df)
        if not dfs:
            logging.warning(f"No data found for exchange: {exch_name}")
            return {}

        temp = pd.concat(dfs, axis=1)
        temp = Tools.get_ticker(df=temp)
        res = Tools.filter_data_map(df=temp, base='funding_rate')
        return res

    def multi_exchange_merger(self) -> dict[str, pd.DataFrame]:
        exch_names = list(self._exchanges.keys())
        res = {}

        def _load_datas(exch_name):
            df = self._exchange_merger(exch_name)
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
                        logging.error(f"exchange={exch_name} exception={exc}")
                    finally:
                        pbar.update(1)
        return res

    def ticker_finder(self,
                      ticker: str) -> pd.DataFrame:
        datas = {}
        for exch_name, df in self.data_map.items():
            if ticker in df.index:
                data = df.loc[[ticker]]
                datas[exch_name] = data

        if not datas:
            return pd.DataFrame()

        res = pd.concat(datas, names=['exchange', 'ticker'])
        return res
