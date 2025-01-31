import re
import math
import pytz
import pandas as pd
from datetime import datetime


class Tools:
    def __init__(self):
        pass

    @staticmethod
    def convert_timestamp_to_kst(ts) -> str:
        kst = pytz.timezone("Asia/Seoul")
        if not ts:
            return "Unknown"
        dt = datetime.fromtimestamp(
            ts / 1000, tz=pytz.utc).astimezone(kst)
        return dt.strftime("%m-%d %H:%M")

    @staticmethod
    def convert_precision_to_decimal(prec_val) -> float:
        return float(f"1e-{prec_val}") if isinstance(prec_val, int) else prec_val

    @staticmethod
    def convert_interval_to_float(interval: str) -> float | None:
        if interval is None or (isinstance(interval, float) and math.isnan(interval)):
            return None

        interval_str = str(interval)

        match = re.findall(r"\d+", interval_str)
        if not match:
            return None
        return float(match[0])

    @staticmethod
    def safe_execute(func, *args, **kwargs) -> pd.DataFrame:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(
                f"[WARNING] Error using args={args}, kwargs={kwargs} - {e}. Retrying without kwargs.")
            try:
                return func(*args)
            except Exception as final_e:
                raise RuntimeError(
                    f"Failed to execute function after retry: {final_e}")

    @staticmethod
    def override_if_exists(main_dict: dict, exc_dict: dict):
        for key, exc_val in exc_dict.items():
            if key in main_dict:
                main_dict[key] = exc_val

    @staticmethod
    def get_ticker(symbol: str) -> str:
        return symbol.split('/')[0]

    @staticmethod
    def get_ticker_with_symbols(df: pd.DataFrame) -> pd.DataFrame:
        df.reset_index(names='symbol', inplace=True)
        df['ticker'] = df['symbol'].apply(Tools.get_ticker)
        df.set_index('ticker', drop=False, inplace=True)
        df.index.name = 'ticker'
        return df

    @staticmethod
    def filter_symbols(df: pd.DataFrame,
                       base: str) -> pd.DataFrame:
        if base not in df.columns:
            return pd.DataFrame()

        res = df[df[base].notna()]
        return res
