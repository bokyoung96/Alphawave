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
    def get_base_symbol(symbol: str) -> str:
        return symbol.split('/')[0]

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
