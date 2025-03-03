import argparse
import logging

from exchange import ExchangeManager
from pipeline import PipelineMerger
from table import TableViewer

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

"""
Run this script in the terminal to get the funding rate table, info table, or ticker finder result.

Usage examples:
- Display the info table:
    python main.py --info
- Display the funding table:
    python main.py --fund
- Display the ticker finder result for a given ticker (e.g., BTC):
    python main.py --ticker BTC

Use the `run` function in an interactive environment to get the TableViewer object.
"""


def run_terminal():
    parser = argparse.ArgumentParser(
        description="Funding Rate Fetcher"
    )
    parser.add_argument("--info", action="store_true",
                        help="Display the info table")
    parser.add_argument("--fund", action="store_true",
                        help="Display the funding table")
    parser.add_argument("--ticker", type=str, default=None,
                        help="Display ticker finder result for a given ticker (e.g., BTC)")
    parser.add_argument("--exch", type=str, default="hyperliquid",
                        help="Base exchange name (default: hyperliquid)")
    parser.add_argument("--tz", type=str, default="Asia/Seoul",
                        help="Timezone (default: Asia/Seoul)")
    args = parser.parse_args()

    exch_mgr = ExchangeManager()
    pipeline = PipelineMerger.load_pipeline(
        exch_mgr=exch_mgr, get_fr=True, get_lm=True, get_ba=True
    )
    viewer = TableViewer(
        exch_mgr=exch_mgr,
        pipeline=pipeline,
        data_map=pipeline.data_map,
        base_exch=args.exch,
        timezone=args.tz
    )

    if args.info:
        print("=== Info Table ===")
        print(viewer.get_info_table())
    if args.fund:
        print("=== Funding Table ===")
        print(viewer.get_funding_table(hours_ahead=8,
                                       tolerance_minutes=30))
    if args.pair:
        print("=== Pair Table ===")
        print(viewer.get_pair_table(interval_equals=True,
                                    pos_exists=True,
                                    fr_mgmt=True))
    if args.table:
        print("=== Table ===")
        print(viewer.get_table)
    if args.ticker:
        print(f"=== Ticker Finder for {args.ticker} ===")
        print(pipeline.ticker_finder(args.ticker))


def run(**kwargs) -> TableViewer:
    exch_mgr = ExchangeManager()
    pipeline = PipelineMerger.load_pipeline(
        exch_mgr=exch_mgr, get_fr=True, get_lm=True, get_ba=True
    )
    return TableViewer(
        exch_mgr=exch_mgr,
        pipeline=pipeline,
        data_map=pipeline.data_map,
        base_exch=kwargs.get("exch_name", "hyperliquid"),
        timezone=kwargs.get("tz", "Asia/Seoul")
    )


if __name__ == "__main__":
    # run_terminal()

    viewer = run()
    print("=== Info Table ===")
    print(viewer.get_info_table)
    print("=== Funding Table ===")
    print(viewer.get_funding_table(hours_ahead=8, tolerance_minutes=30))
    print("=== Pair Table ===")
    print(viewer.get_pair_table(interval_equals=True,
                                pos_exists=True,
                                fr_mgmt=True))
    print("=== Table ===")
    print(viewer.get_table)
