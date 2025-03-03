import pytz
import logging
import warnings
import numpy as np
import pandas as pd
from itertools import permutations
from functools import cached_property
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

    @cached_property
    def get_info_table(self) -> pd.DataFrame:
        df = pd.concat(self.data_map, names=['exchange'], axis=0)
        df.set_index('settle', append=True, inplace=True)
        df.index.set_names(['exchange', 'ticker', 'settle'], inplace=True)
        df = df[['symbol',
                 'funding_rate', 'interval',
                 'bid', 'ask', 'quoteVolume',
                 'taker', 'maker',
                 'fundingTimestamp']]

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

    def get_pair_table(self,
                       interval_equals: bool = True,
                       pos_exists: bool = True,
                       fr_mgmt: bool = True) -> pd.DataFrame:
        infos = self.get_info_table.reset_index()
        if infos.empty:
            return pd.DataFrame()

        def get_pair(row1: dict, row2: dict, ticker: str) -> dict | None:
            if interval_equals and row1.get('interval') != row2.get('interval'):
                return None
            if not row1.get('interval') or not row2.get('interval'):
                return None

            try:
                # NOTE: Scale funding rate to 8 hours
                fr1 = row1['funding_rate'] * (8 / row1['interval'])
                fr2 = row2['funding_rate'] * (8 / row2['interval'])
            except Exception:
                return None

            diff = fr1 - fr2

            if diff > 0:
                pos1, pos2 = 'S', 'L'
            elif diff < 0:
                pos1, pos2 = 'L', 'S'
            else:
                pos1, pos2 = None, None

            if pos_exists and (pos1 is None or pos2 is None):
                return None

            return {
                'ticker': ticker,
                'exch1': row1.get('exchange'),
                'exch2': row2.get('exchange'),
                'time1': row1.get('fundingTimestamp'),
                'time2': row2.get('fundingTimestamp'),
                'fr1': row1.get('funding_rate'),
                'fr2': row2.get('funding_rate'),
                'interval1': row1.get('interval'),
                'interval2': row2.get('interval'),
                'bid1': row1.get('bid'),
                'bid2': row2.get('bid'),
                'ask1': row1.get('ask'),
                'ask2': row2.get('ask'),
                'maker1': row1.get('maker'),
                'maker2': row2.get('maker'),
                'taker1': row1.get('taker'),
                'taker2': row2.get('taker'),
                'diff': diff,
                'pos1': pos1,
                'pos2': pos2,
            }

        records = []
        for ticker, group in infos.groupby('ticker'):
            if len(group) < 2:
                continue

            rows = group.to_dict('records')
            for row1, row2 in permutations(rows, 2):
                record = get_pair(row1, row2, ticker)
                if record is not None:
                    records.append(record)

        temp = pd.DataFrame(records)
        if not temp.empty:
            temp['diff'] = temp['diff'].round(6)

        pairs = temp[temp['diff'] < 0].sort_values(
            by='diff', ascending=True) if fr_mgmt else temp

        pis = []
        for _, row in pairs.iterrows():
            if row['pos1'] == 'L':
                long_leg = {'bid': row['bid1'], 'ask': row['ask1'],
                            'maker': row['maker1'], 'taker': row['taker1']}
                short_leg = {'bid': row['bid2'], 'ask': row['ask2'],
                             'maker': row['maker2'], 'taker': row['taker2']}

            elif row['pos2'] == 'L':
                long_leg = {'bid': row['bid2'], 'ask': row['ask2'],
                            'maker': row['maker2'], 'taker': row['taker2']}
                short_leg = {'bid': row['bid1'], 'ask': row['ask1'],
                             'maker': row['maker1'], 'taker': row['taker1']}

            else:
                continue

            # NOTE: Long maker, Short taker (LmSt)
            try:
                eff_l_bid = long_leg['bid'] * (1 - long_leg['maker'])
                eff_s_bid = short_leg['bid'] * (1 - short_leg['taker'])
                pi_bid = (eff_s_bid - eff_l_bid) / eff_l_bid
            except Exception:
                pi_bid = np.nan
            row_bid = row.copy()
            row_bid['tm'] = 'LmSt'
            row_bid['pi'] = pi_bid

            # NOTE: Long taker, Short maker (LtSm)
            try:
                eff_l_ask = long_leg['ask'] * (1 + long_leg['taker'])
                eff_s_ask = short_leg['ask'] * (1 + short_leg['maker'])
                pi_ask = (eff_s_ask - eff_l_ask) / eff_l_ask
            except Exception:
                pi_ask = np.nan
            row_ask = row.copy()
            row_ask['tm'] = 'LtSm'
            row_ask['pi'] = pi_ask

            pis.append(row_bid)
            pis.append(row_ask)

        res = pd.DataFrame(pis)
        return res

    @property
    def get_table(self):
        pairs = self.get_pair_table(interval_equals=True,
                                    pos_exists=True,
                                    fr_mgmt=True)
        if pairs.empty:
            return pairs

        pairs['diff'] = -pairs['diff']
        pairs['ER'] = pairs['diff'] + pairs['pi']

        pairs = pairs[['ticker',
                       'exch1', 'exch2',
                       'time1', 'interval1',
                       'pos1', 'pos2', 'tm',
                       'diff', 'ER']]
        res = pairs.sort_values(by='ER', ascending=False)

        res = res.rename(columns={'time1': 't', 'interval1': 'int'})

        today = pd.Timestamp.now(tz=self.tz).normalize()

        def format_timestamp(ts):
            ts = pd.to_datetime(ts, errors='coerce')
            if pd.isna(ts):
                return ""
            if ts.tzinfo is None:
                ts = ts.tz_localize(self.tz)
            else:
                ts = ts.tz_convert(self.tz)
            day_diff = (ts.normalize() - today).days
            day_str = "T" if day_diff == 0 else f"T+{day_diff}"
            hour = ts.hour
            return f"{day_str} / {hour}"

        res['t'] = res['t'].apply(format_timestamp)
        res = res.set_index('ticker')
        return res


# if __name__ == "__main__":
#     viewer = TableViewer.default_viewer(base_exch='hyperliquid',
#                                         timezone='Asia/Seoul')
#     funding_table = viewer.get_funding_table(hours_ahead=8,
#                                              tolerance_minutes=5)
#     info_table = viewer.get_info_table
#     pair_table = viewer.get_pair_table(interval_equals=True,
#                                        pos_exists=True,
#                                        fr_mgmt=True)
#     table = viewer.get_table()
