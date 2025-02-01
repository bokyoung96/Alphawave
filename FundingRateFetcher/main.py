import argparse
import logging
import pandas as pd
from typing import Optional, Dict

from pipeline import PipelineFinder
from exchange import ExchangeManager


class Main:
    def __init__(self,
                 exch_name: str = "hyperliquid",
                 get_fr: bool = True,
                 get_lm: bool = True,
                 get_ba: bool = True,
                 get_ex: bool = True,
                 registry: Optional[Dict] = None) -> None:
        """
        :param exch_name: Base exchange name used for funding rate lookup.
        :param get_fr: Whether to enable Funding Rates filter.
        :param get_lm: Whether to enable Load Markets filter.
        :param get_ba: Whether to enable Bid/Ask filter.
        :param get_ex: Whether to enable Exception filter.
        :param registry: A registry dictionary to be passed to ExchangeManager.
        """
        self.exch_name = exch_name
        self.get_fr = get_fr
        self.get_lm = get_lm
        self.get_ba = get_ba
        self.get_ex = get_ex
        self.registry = registry

        logging.debug(
            "Initializing ExchangeManager with registry: %s", self.registry)
        self.exch_mgr = ExchangeManager(registry=self.registry)

        logging.debug(
            "Loading PipelineFinder with filters: get_fr=%s, get_lm=%s, get_ba=%s, get_ex=%s",
            self.get_fr,
            self.get_lm,
            self.get_ba,
            self.get_ex,
        )
        self.pipeline_finder = PipelineFinder.load_pipeline(
            exch_mgr=self.exch_mgr,
            get_fr=self.get_fr,
            get_lm=self.get_lm,
            get_ba=self.get_ba,
            get_ex=self.get_ex,
        )

    def show_funding_rates(self, base_exch: Optional[str] = None) -> pd.DataFrame:
        """
        Displays the Funding Rates pivot table based on the provided or default base exchange.

        :param base_exch: The base exchange name to use for the funding rate pivot table.
                          If not provided, defaults to self.exch_name.
        :return: A pandas DataFrame containing the funding rate data in a pivot table format.
        """
        base_exch = base_exch or self.exch_name

        if not self.get_fr:
            logging.error("Funding Rates data locked (get_fr=False).")
            return pd.DataFrame()

        logging.info(
            "Retrieving Funding Rates for base exchange: %s", base_exch)
        fr_df = self.pipeline_finder.funding_rate_finder(base_exch=base_exch)

        if fr_df.empty:
            logging.warning(
                "No Funding Rate data found for exchange: '%s'.", base_exch)
        else:
            logging.debug("Funding Rates data:\n%s", fr_df)

        return fr_df

    def show_ticker_info(self, ticker: str) -> pd.DataFrame:
        """
        Displays information about a specific ticker across multiple exchanges.

        :param ticker: The ticker symbol to look up (e.g. 'BTC/USDT').
        :return: A pandas DataFrame containing data for the given ticker across all enabled exchanges.
        """
        logging.info("Retrieving ticker info for: %s", ticker)
        df = self.pipeline_finder.ticker_finder(ticker=ticker)

        if df.empty:
            logging.warning("No data found for ticker: '%s'.", ticker)
        else:
            logging.debug("Ticker data:\n%s", df)

        return df

    def show_merged_data(self, base_exch: Optional[str] = None) -> pd.DataFrame:
        """
        Displays merged data (all columns) across exchanges based on the provided or default base exchange.

        :param base_exch: The base exchange name to use as reference.
                          If not provided, defaults to self.exch_name.
        :return: A pandas DataFrame with merged data from all exchanges.
        """
        base_exch = base_exch or self.exch_name

        logging.info("Retrieving merged data for base exchange: %s", base_exch)
        merged_df = self.pipeline_finder.merged_data_finder(
            base_exch=base_exch)

        if merged_df.empty:
            logging.warning(
                "No merged data found for exchange: '%s'.", base_exch)
        else:
            logging.debug("Merged data:\n%s", merged_df)

        return merged_df

    def run(self, show_fr: bool = False, ticker: Optional[str] = None, show_merged: bool = False) -> None:
        """
        Main runner method, which can optionally show funding rates, merged data,
        and/or query a specific ticker. Extend this method to perform other actions.

        :param show_fr: If True, display the funding rates pivot table.
        :param ticker: If provided, display the ticker info.
        :param show_merged: If True, display the merged data (all columns) based on the base exchange.
        """
        logging.info("Starting Main execution.")
        if show_fr:
            self.show_funding_rates()
        if ticker:
            self.show_ticker_info(ticker)
        if show_merged:
            self.show_merged_data()
        logging.info("Main execution completed.")


def parse_cli_args() -> argparse.Namespace:
    """
    Parses CLI arguments using argparse and returns the parsed arguments.

    :return: The parsed arguments as an argparse.Namespace object.
    """
    parser = argparse.ArgumentParser(
        description="Exchange Data Pipeline Application (CLI)"
    )
    parser.add_argument(
        "--exch_name",
        type=str,
        default="hyperliquid",
        help="Base exchange name for funding rate lookup (default: hyperliquid)",
    )
    parser.add_argument(
        "--no_fr",
        action="store_true",
        help="Disable Funding Rates filter",
    )
    parser.add_argument(
        "--no_lm",
        action="store_true",
        help="Disable Load Markets filter",
    )
    parser.add_argument(
        "--no_ba",
        action="store_true",
        help="Disable Bid/Ask filter",
    )
    parser.add_argument(
        "--no_ex",
        action="store_true",
        help="Disable Exception filter",
    )
    parser.add_argument(
        "--show_fr",
        action="store_true",
        help="Display Funding Rates pivot table after pipeline run",
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="Display data for a specific ticker (e.g. BTC/USDT)",
    )
    parser.add_argument(
        "--show_merged",
        action="store_true",
        help="Display merged data (all columns) based on the base exchange",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable detailed logging (DEBUG level)",
    )
    return parser.parse_args()


def run_from_cli(args: argparse.Namespace) -> None:
    app = Main(
        exch_name=args.exch_name,
        get_fr=not args.no_fr,
        get_lm=not args.no_lm,
        get_ba=not args.no_ba,
        get_ex=not args.no_ex,
        registry=None,
    )
    app.run(show_fr=args.show_fr, ticker=args.ticker,
            show_merged=args.show_merged)


def run_from_interactive(**kwargs) -> Main:
    return Main(**kwargs)


def main() -> None:
    args = parse_cli_args()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    run_from_cli(args)


if __name__ == "__main__":
    main()
