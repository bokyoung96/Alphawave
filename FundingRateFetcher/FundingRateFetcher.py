import asyncio
import pytz
import pandas as pd
from datetime import datetime
from tqdm.asyncio import tqdm_asyncio

from Register import ExchangeManager


class BaseFetcher:
    def __init__(self, exch_mgr: ExchangeManager):
        self._mgr = exch_mgr
        self._exchanges = exch_mgr.exchanges
        self._configs = exch_mgr.configs
        self._kst = pytz.timezone("Asia/Seoul")

    def _convert_timestamp_to_kst(self, ts):
        if not ts:
            return "Unknown"
        dt = datetime.fromtimestamp(
            ts / 1000, tz=pytz.utc).astimezone(self._kst)
        return dt.strftime("%m-%d %H:%M")


class FundingRateFetcher(BaseFetcher):
    async def fetch_funding_rate(self, exchange, symbol):
        loop = asyncio.get_event_loop()
        try:
            rate_info = await loop.run_in_executor(None, exchange.fetch_funding_rate, symbol)
            return {
                "exchange": exchange.name,
                "symbol": symbol,
                "fundingRate": rate_info.get("fundingRate"),
                "fundingDatetime": self._convert_timestamp_to_kst(rate_info.get("fundingTimestamp")),
            }
        except Exception as e:
            print(f"Error occurred in funding_rate for {symbol}: {e}")
            return None

    async def fetch_all_funding_rates(self) -> pd.DataFrame:
        results = []
        tasks = []
        progress_bars = []
        exchange_symbols = []

        for conf in self._configs:
            exch_name = conf.exchange.value
            stable = conf.stable.value
            exchange = self._exchanges.get(exch_name)
            if not exchange:
                continue
            swaps = [
                swap for swap in exchange.symbols
                if "swap" in exchange.markets[swap].get("type", "").lower() and stable in swap
            ]
            if not swaps:
                continue
            exchange_symbols.append((exchange, swaps))

        for position, (exchange, symbols) in enumerate(exchange_symbols):
            progress_bar = tqdm_asyncio(
                total=len(symbols),
                desc=f"{exchange.name} Funding Rates",
                position=position,
                unit="symbol"
            )
            progress_bars.append(progress_bar)

            async def fetch_exchange_funding_rates(exchange, symbols, progress_bar):
                nonlocal results
                for symbol in symbols:
                    result = await self.fetch_funding_rate(exchange, symbol)
                    if result:
                        results.append(result)
                    progress_bar.update(1)

            task = asyncio.create_task(
                fetch_exchange_funding_rates(exchange, symbols, progress_bar)
            )
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks)

        for progress_bar in progress_bars:
            progress_bar.close()

        return pd.DataFrame(results)


class AdditionalDataFetcher(BaseFetcher):
    async def fetch_additional_data(self, exchange, symbol):
        loop = asyncio.get_event_loop()
        try:
            tkr = await loop.run_in_executor(None, exchange.fetch_ticker, symbol)
            bid = tkr.get("bid")
            ask = tkr.get("ask")
            price = tkr.get("last")
            return {
                "exchange": exchange.name,
                "symbol": symbol,
                "bid/ask": (bid / ask) if ask else None,
                "price": price
            }
        except Exception as e:
            print(f"Error occurred in ticker for {symbol}: {e}")
            return None

    async def fetch_all_additionals(self) -> pd.DataFrame:
        results = []
        tasks = []
        progress_bars = []
        exchange_symbols = []

        for conf in self._configs:
            exch_name = conf.exchange.value
            stable = conf.stable.value
            exchange = self._exchanges.get(exch_name)
            if not exchange:
                continue
            swaps = [
                swap for swap in exchange.symbols
                if "swap" in exchange.markets[swap].get("type", "").lower() and stable in swap
            ]
            if not swaps:
                continue
            exchange_symbols.append((exchange, swaps))

        for position, (exchange, symbols) in enumerate(exchange_symbols):
            progress_bar = tqdm_asyncio(
                total=len(symbols),
                desc=f"{exchange.name} Additionals",
                position=position,
                unit="symbol"
            )
            progress_bars.append(progress_bar)

            async def fetch_exchange_additionals(exchange, symbols, progress_bar):
                nonlocal results
                for symbol in symbols:
                    result = await self.fetch_additional_data(exchange, symbol)
                    if result:
                        results.append(result)
                    progress_bar.update(1)

            task = asyncio.create_task(
                fetch_exchange_additionals(exchange, symbols, progress_bar)
            )
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks)

        for progress_bar in progress_bars:
            progress_bar.close()

        return pd.DataFrame(results)


class DataGetter:
    def __init__(self):
        self.exch_mgr = ExchangeManager(registry=None)
        self.funding_fetcher = FundingRateFetcher(exch_mgr=self.exch_mgr)
        self.additional_fetcher = AdditionalDataFetcher(exch_mgr=self.exch_mgr)
        self.funding_rates = pd.DataFrame()
        self.additionals = pd.DataFrame()

    async def fetch_funding_rates(self):
        self.funding_rates = await self.funding_fetcher.fetch_all_funding_rates()

    async def fetch_additionals(self):
        self.additionals = await self.additional_fetcher.fetch_all_additionals()

    async def gather_data(self):
        funding_task = asyncio.create_task(self.fetch_funding_rates())
        additional_task = asyncio.create_task(self.fetch_additionals())
        await asyncio.gather(funding_task, additional_task)


async def main():
    getter = DataGetter()

    await getter.fetch_funding_rates()
    print("Funding Rates:")
    print(getter.funding_rates.head())

    await getter.fetch_additionals()
    print("Additional Data:")
    print(getter.additionals.head())
    return getter.funding_rates, getter.additionals


if __name__ == "__main__":
    funding_rates, additionals = asyncio.run(main())
