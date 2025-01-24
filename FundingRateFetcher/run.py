import pandas as pd

from register import ExchangeManager
from fetcher import FundingRatesFilter, LoadMarketsFilter, BidAskFilter, SnapShotFetcher


class PipelineManager:
    def __init__(self,
                 exch_mgr: ExchangeManager,
                 get_fr: bool = True,
                 get_lm: bool = True,
                 get_ba: bool = False):
        self.exch_mgr = exch_mgr
        self.fetcher = SnapShotFetcher()

        self.get_fr = get_fr
        self.get_lm = get_lm
        self.get_ba = get_ba

        self.res: dict[str, dict] = None
        self.history: pd.DataFrame = None

    def run(self):
        filters = [
            (self.get_fr, FundingRatesFilter),
            (self.get_lm, LoadMarketsFilter),
            (self.get_ba, BidAskFilter),
        ]
        for enabled, fcls in filters:
            if enabled:
                self.fetcher.add_filter(fcls(self.exch_mgr), enabled=True)

        self.res = self.fetcher.run()
        self.history = self.fetcher.history


if __name__ == "__main__":
    exch_mgr = ExchangeManager(registry=None)
    pipeline = PipelineManager(exch_mgr=exch_mgr)
    pipeline.run()
