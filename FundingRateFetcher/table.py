import pytz
import logging
import warnings
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from tools import Tools
from exchange import ExchangeManager
from pipeline import PipelineMerger

warnings.filterwarnings("ignore", category=FutureWarning)
logger = logging.getLogger(__name__)


class TableViewer:
    def __init__(self,
                 exch_mgr: ExchangeManager,
                 pipeline: PipelineMerger,
                 data_map: dict[str, pd.DataFrame],
                 base_exch: str = 'hyperliquid',
                 timezone: str = 'Asia/Seoul'):
        self.exch_mgr = exch_mgr
        self.pipeline = pipeline
        self.data_map = data_map
        self.base_exch = base_exch
        self.tz = pytz.timezone(timezone)

    @classmethod
    def default_viewer(cls,
                       exch_mgr: ExchangeManager = None,
                       base_exch: str = 'hyperliquid',
                       timezone: str = 'Asia/Seoul'):
        if exch_mgr is None:
            exch_mgr = ExchangeManager()
        pipeline = PipelineMerger.load_pipeline(
            exch_mgr=exch_mgr, get_fr=True, get_lm=True, get_ba=True)
        return cls(exch_mgr=exch_mgr,
                   pipeline=pipeline,
                   data_map=pipeline.data_map,
                   base_exch=base_exch,
                   timezone=timezone)

    def _get_convert_rates(self) -> pd.DataFrame:
        def _load_datas(exch_name: str,
                        exch):
            df = Tools.safe_execute(exch.fetchTicker,
                                    symbol='USDC/USDT',
                                    skip=True)
            if df is None:
                res = {}
            else:
                res = {
                    'symbol': df['symbol'],
                    'bid': df['bid'],
                    'ask': df['ask']
                }
            return exch_name, res

        snapshot = {}
        with ThreadPoolExecutor(max_workers=self.pipeline._max_workers) as executor:
            futures = []
            for exch_name, exchange in self.exch_mgr.exchanges.items():
                fut = executor.submit(_load_datas,
                                      exch_name,
                                      exchange)
                futures.append(fut)

            for f in as_completed(futures):
                exch_name, result_dict = f.result()
                snapshot[exch_name] = result_dict
        return snapshot

    def get_info_table(self) -> pd.DataFrame:
        df = pd.concat(self.data_map, names=['exchange'], axis=0)
        df.set_index('settle', append=True, inplace=True)
        df.index.set_names(['exchange', 'ticker', 'settle'], inplace=True)
        df = df[['symbol',
                 'funding_rate', 'interval',
                 'bid', 'ask', 'quoteVolume',
                 'taker', 'maker']]

        convert_rates = self._get_convert_rates()

        def _convert_bid_ask(df: pd.DataFrame, convert_rates: dict[str, dict]):
            def _calculate_usdc(row: pd.Series) -> pd.Series:
                exch, _, settle = row.name
                row_bid = row.get('bid', np.nan)
                row_ask = row.get('ask', np.nan)

                convert_info = convert_rates.get(exch, {})
                convert_bid = convert_info.get('bid', np.nan)
                convert_ask = convert_info.get('ask', np.nan)

                if settle == 'USDC':
                    return pd.Series({'bid_USDC': row_bid,
                                      'ask_USDC': row_ask})
                elif settle == 'USDT':
                    return pd.Series({'bid_USDC': row_bid * convert_bid,
                                      'ask_USDC': row_ask * convert_ask})
                else:
                    return pd.Series({'bid_USDC': np.nan,
                                      'ask_USDC': np.nan})

            df[['bid_USDC', 'ask_USDC']] = df.apply(_calculate_usdc, axis=1)
            return df

        res = _convert_bid_ask(df=df, convert_rates=convert_rates)
        return res

    def _get_time_slots(self, hours_ahead: int) -> list[pd.Timestamp]:
        now = pd.Timestamp.now(tz=self.tz)
        next_hour = now.replace(
            minute=0, second=0, microsecond=0) + pd.Timedelta(hours=1)
        return [next_hour + pd.Timedelta(hours=i) for i in range(hours_ahead)]

    def get_funding_table(self,
                          hours_ahead: int = 8,
                          tolerance_minutes: int = 5) -> pd.DataFrame:
        if self.base_exch not in self.data_map:
            return pd.DataFrame()

        slots = self._get_time_slots(hours_ahead)
        base_tickers = set(self.data_map[self.base_exch]['ticker'].unique())
        records = []

        for exch, df in self.data_map.items():
            for _, row in df.iterrows():
                ticker = row['ticker']
                if ticker not in base_tickers:
                    continue

                try:
                    ts = pd.to_datetime(row['fundingTimestamp'])
                except Exception:
                    continue

                if ts.tzinfo is None:
                    ts = ts.tz_localize(self.tz)
                elif ts.tzinfo != self.tz:
                    ts = ts.tz_convert(self.tz)

                closest = min(slots, key=lambda slot: abs(ts - slot))
                if abs(ts - closest) > pd.Timedelta(minutes=tolerance_minutes):
                    continue

                rec = {
                    'time': closest,
                    'ticker': ticker,
                    'settle': row.get('settle'),
                    'exchange': exch,
                    'funding_rate': row.get('funding_rate'),
                    'order': 0
                }
                records.append(rec)

                _interval = row.get('interval')

                if pd.notna(_interval):
                    interval = _interval
                    if interval > 0:
                        k = 1
                        while True:
                            extra_slot = closest + \
                                pd.Timedelta(hours=k * interval)
                            if extra_slot in slots:
                                records.append({
                                    'time': extra_slot,
                                    'ticker': ticker,
                                    'settle': row.get('settle'),
                                    'exchange': exch,
                                    'funding_rate': row.get('funding_rate'),
                                    'order': 1
                                })
                                k += 1
                            else:
                                break

        if not records:
            return pd.DataFrame(index=pd.DatetimeIndex(slots, name='time'))

        rec_df = pd.DataFrame(records)
        table = rec_df.pivot_table(
            index='time',
            columns=['ticker', 'settle', 'exchange'],
            values='funding_rate',
            aggfunc='first'
        ).reindex(slots)

        if not table.empty and isinstance(table.columns, pd.MultiIndex):
            valid = [tkr for tkr in table.columns.get_level_values(0).unique()
                     if len(set(table.xs(tkr, level=0, axis=1).columns)) >= 2]
            table = table.loc[:, table.columns.get_level_values(0).isin(valid)]
        return table


# if __name__ == "__main__":
#     viewer = TableViewer.default_viewer(base_exch='hyperliquid',
#                                         timezone='Asia/Seoul')
#     funding_table = viewer.get_funding_table(hours_ahead=8,
#                                              tolerance_minutes=5)
#     info_table = viewer.get_info_table()
