from typing import Optional
import re
import json
import pytz
import logging
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Tools:
    def __init__(self):
        pass

    @staticmethod
    def load_config(file_path: str) -> dict:
        try:
            with open(file_path, 'r') as file:
                config = json.load(file)
            return config
        except FileNotFoundError:
            logging.error(f"Configuration file {file_path} not found.")
            return {}
        except json.JSONDecodeError:
            logging.error("Error decoding JSON from the configuration file.")
            return {}

    @staticmethod
    def convert_timestamp_to_kst(ts) -> datetime:
        kst = pytz.timezone("Asia/Seoul")
        if not ts:
            return "Unknown"
        dt = datetime.fromtimestamp(
            ts / 1000, tz=pytz.utc).astimezone(kst)
        return dt

    @staticmethod
    def convert_precision_to_decimal(prec_val) -> float:
        return float(f"1e-{prec_val}") if isinstance(prec_val, int) else prec_val

    @staticmethod
    def convert_interval_to_float(interval: str) -> Optional[float]:
        if interval is None:
            return None

        try:
            value = float(interval)
            if value >= 3600:
                hours = value / 3600
                if hours == int(hours):
                    return int(hours)
                return hours
            return value
        except (ValueError, TypeError):
            pass

        interval_str = str(interval)
        match = re.search(r"\d+(\.\d+)?", interval_str)
        if match:
            value = float(match.group(0))
            if value >= 3600:
                hours = value / 3600
                if hours == int(hours):
                    return int(hours)
                return hours
            return value
        return None

    @staticmethod
    def safe_execute(func, *args, **kwargs) -> pd.DataFrame:
        skip = kwargs.pop('skip', False)
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(
                f"Error using args={args}, kwargs={kwargs} - {e}. Retrying without kwargs.")
            try:
                return func(*args)
            except Exception as final_e:
                logger.error(
                    f"Failed to execute function after retry: {final_e}")
                if skip:
                    logger.warning(
                        "Skipping execution and returning empty DataFrame.")
                    return None
                else:
                    raise RuntimeError(
                        f"Failed to execute function after retry: {final_e}")

    @staticmethod
    def override_if_exists(main_dict: dict, exc_dict: dict):
        for key, exc_val in exc_dict.items():
            if key in main_dict:
                main_dict[key] = exc_val

    @staticmethod
    def get_ticker(df: pd.DataFrame) -> pd.DataFrame:
        df.reset_index(names='symbol', inplace=True)
        df['ticker_prev'] = df['symbol'].apply(
            lambda symbol: symbol.split('/')[0])
        df['ticker'] = df['ticker_prev'].copy()
        df.set_index('ticker', drop=False, inplace=True)
        df.index.name = 'ticker'
        return df

    @staticmethod
    def adjust_numerical_ticker(df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Adjusting numerical tickers.")
        try:
            pattern = r'^(\d+)([A-Za-z]+)$'
            extracted = df['ticker'].str.extract(pattern)

            df['prefix'] = pd.to_numeric(extracted[0], errors='coerce')
            df['suffix'] = extracted[1]

            mask = df['prefix'] >= 10
            df.loc[mask, 'ticker'] = df.loc[mask, 'suffix']

            for col in ['bid', 'ask', 'price']:
                if col in df.columns:
                    try:
                        df.loc[mask, col] = df.loc[mask, col] / \
                            df.loc[mask, 'prefix']
                    except Exception as e:
                        logger.error(f"Error adjusting column '{col}': {e}")

            df.drop(columns=['prefix', 'suffix'], inplace=True)
            df.set_index('ticker', drop=False, inplace=True)
            df.index.name = 'ticker'

        except Exception as e:
            logger.error(f"Unexpected error in adjust_numerical_ticker: {e}")
            raise
        return df

    @staticmethod
    def filter_data_map(df: pd.DataFrame, base: str) -> pd.DataFrame:
        if 'active' not in df.columns:
            logger.warning(
                "Column 'active' not found. Returning empty DataFrame.")
            return pd.DataFrame()

        if 'settle' not in df.columns:
            logger.warning(
                "Column 'settle' not found. Returning empty DataFrame.")
            return pd.DataFrame()

        if 'linear' not in df.columns:
            logger.warning(
                "Column 'linear' not found. Returning empty DataFrame.")

        if base not in df.columns:
            logger.warning(
                f"Column '{base}' not found. Filtering only by 'active'.")
            return df[df['active'] == True]

        res = df[(df[base].notna()) & (df['active'] == True)
                 & (df['linear'] == True)]
        return res
