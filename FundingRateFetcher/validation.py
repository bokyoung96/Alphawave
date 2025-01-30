import ccxt
import numpy as np
import pandas as pd

from tools import Tools


def validation_fetchFundingRates(mkt_name):
    mkt = getattr(ccxt, mkt_name)
    inst = mkt()
    df = pd.DataFrame(Tools.safe_execute(inst.fetchFundingRates,
                                         params=None))
    return df


def validation_loadMarkets(mkt_name):
    mkt = getattr(ccxt, mkt_name)
    inst = mkt()
    df = pd.DataFrame(Tools.safe_execute(inst.loadMarkets,
                                         params=None))
    return df


def validation_fetchTickers(mkt_name):
    mkt = getattr(ccxt, mkt_name)
    inst = mkt()
    params = {'type': 'swap', 'subtype': 'linear'}
    df = pd.DataFrame(Tools.safe_execute(inst.fetchTickers,
                                         params=params))
    return df


def validation_fetchBidsAsks(mkt_name):
    mkt = getattr(ccxt, mkt_name)
    inst = mkt()
    params = {'type': 'swap', 'subtype': 'linear'}
    df = pd.DataFrame(Tools.safe_execute(inst.fetchBidsAsks,
                                         params=params))
    return df


if __name__ == "__main__":
    mkt_name = 'bitget'
    loader = validation_fetchFundingRates(mkt_name=mkt_name)
