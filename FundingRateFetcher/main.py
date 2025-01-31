import argparse
import pandas as pd
from typing import Optional

from pipeline import PipelineFinder
from exchange import ExchangeManager


class Main:
    """
    Main execution class that provides an interface to the PipelineFinder.
    Through CLI arguments or direct instantiation, one can easily toggle 
    various filters (funding rates, load markets, bid/ask, and exception handling).
    """

    def __init__(self,
                 exch_name: str = "hyperliquid",
                 get_fr: bool = True,
                 get_lm: bool = True,
                 get_ba: bool = True,
                 get_ex: bool = True,
                 registry: Optional[dict] = None):
        """
        :param exch_name: The base exchange name (for reference in funding_rate_finder).
        :param get_fr: Whether to enable the Funding Rates filter.
        :param get_lm: Whether to enable the Load Markets filter.
        :param get_ba: Whether to enable the Bid/Ask filter.
        :param get_ex: Whether to enable the Exception filter.
        :param registry: A dictionary registry that can be passed to ExchangeManager, if needed.
        """
        self.exch_name = exch_name
        self.get_fr = get_fr
        self.get_lm = get_lm
        self.get_ba = get_ba
        self.get_ex = get_ex
        self.registry = registry

        # Initialize ExchangeManager
        self.exch_mgr = ExchangeManager(registry=self.registry)

        # Load PipelineFinder with the specified filters
        self.pipeline_finder = PipelineFinder.load_pipeline(
            exch_mgr=self.exch_mgr,
            get_fr=self.get_fr,
            get_lm=self.get_lm,
            get_ba=self.get_ba,
            get_ex=self.get_ex
        )

    def show_funding_rates(self,
                           base_exch: Optional[str] = None) -> pd.DataFrame:
        """
        Shows the Funding Rates pivot table based on the provided or default base exchange.

        :param base_exch: The base exchange name to use for the funding rate pivot table. 
                          If not provided, it defaults to self.exch_name.
        :return: A pandas DataFrame containing the funding rate data in a pivot table format.
        """
        if base_exch is None:
            base_exch = self.exch_name

        if not self.get_fr:
            print("[Error] get_fr=False. Funding Rates data is disabled.")
            return pd.DataFrame()

        print(f"=== Funding Rates Finder (base_exch='{base_exch}') ===")
        fr_df = self.pipeline_finder.funding_rate_finder(base_exch=base_exch)

        if fr_df.empty:
            print("[Warning] No Funding Rate data found.")
        else:
            print(fr_df)

        print("======================================================\n")
        return fr_df

    def show_ticker_info(self, ticker: str) -> pd.DataFrame:
        """
        Shows information about a specific ticker across multiple exchanges.

        :param ticker: The ticker symbol to look up (e.g., 'BTC').
        :return: A pandas DataFrame containing data for the given ticker across all enabled exchanges.
        """
        print(f"=== Ticker Finder (ticker='{ticker}') ===")
        df = self.pipeline_finder.ticker_finder(ticker=ticker)

        if df.empty:
            print(f"[Warning] No data for ticker: {ticker}")
        else:
            print(df)
        print("========================================\n")
        return df

    def run(self, show_fr: bool = False, ticker: Optional[str] = None):
        """
        Main runner method, which can optionally show funding rates and/or
        query a specific ticker. Extend this method to perform other actions.

        :param show_fr: If True, display the funding rates pivot table after running the pipeline.
        :param ticker: If provided, display the ticker info.
        """
        print(">>> Main is running... <<<")
        if show_fr:
            self.show_funding_rates()

        if ticker:
            self.show_ticker_info(ticker)


def parse_arguments():
    """
    Parses CLI arguments using argparse and returns the parsed arguments.

    :return: The parsed arguments containing settings for the pipeline.
    """
    parser = argparse.ArgumentParser(
        description="CLI for PipelineFinder to retrieve and display exchange data."
    )

    parser.add_argument(
        "--exch_name",
        type=str,
        default="hyperliquid",
        help="Specify the base exchange name for funding rates. (default: hyperliquid)"
    )

    parser.add_argument(
        "--no_fr",
        action="store_true",
        help="Disable Funding Rates filter."
    )

    parser.add_argument(
        "--no_lm",
        action="store_true",
        help="Disable Load Markets filter."
    )

    parser.add_argument(
        "--no_ba",
        action="store_true",
        help="Disable Bid/Ask filter."
    )

    parser.add_argument(
        "--no_ex",
        action="store_true",
        help="Disable Exception filter."
    )

    parser.add_argument(
        "--show_fr",
        action="store_true",
        help="Show Funding Rates pivot table after pipeline run."
    )

    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="Show data for a specific ticker (e.g. BTC/USDT)."
    )

    return parser.parse_args()


def main():
    """
    Main entry point when running this script from the command line.
    It creates a Main object with the desired filters and performs 
    optional display operations based on CLI arguments.
    """
    args = parse_arguments()

    # Create Main object with toggled filters
    app = Main(
        exch_name=args.exch_name,
        get_fr=not args.no_fr,
        get_lm=not args.no_lm,
        get_ba=not args.no_ba,
        get_ex=not args.no_ex,
        registry=None  # You can customize the registry if necessary
    )

    # Run optional display methods
    app.run(
        show_fr=args.show_fr,
        ticker=args.ticker
    )


if __name__ == "__main__":
    main()
