import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import matplotlib.pyplot as plt
from scipy.stats import gmean

raw_data=pd.read_pickle('data/USDT_KRW_20250311.pkl')


SMA_window = 60 * 24 *1  # 하루
SD_window = SMA_window
LB_threshold = 1
UB_threshold = 1
############################################################
transaction_cost = 0.0004
############################################################
SMA = raw_data['K_prem(%)'].rolling(SMA_window).mean()
SD = raw_data['K_prem(%)'].rolling(SD_window).std()
raw_data['SMA'] = SMA                    #premium SMA
raw_data['LB']= SMA - LB_threshold*SD    #premium lower band
raw_data['UB']= SMA + UB_threshold*SD    #premium upper band
raw_data_dropna=raw_data.dropna().copy()

raw_data_dropna['close_LB']= raw_data_dropna['MID_PRICE'] * (1+raw_data_dropna['LB']/100)
raw_data_dropna['close_LB_floor']= np.floor(raw_data_dropna['close_LB'])
raw_data_dropna['close_LB_ceil']= np.ceil(raw_data_dropna['close_LB'])

raw_data_dropna['close_UB']= raw_data_dropna['MID_PRICE'] * (1+raw_data_dropna['UB']/100)
raw_data_dropna['close_UB_floor']= np.floor(raw_data_dropna['close_UB'])
raw_data_dropna['close_UB_ceil']= np.ceil(raw_data_dropna['close_UB'])

# 1. 매매 시그널 생성
    #seq1: buy_breakdown_signal
    # low[t-1]이 close_LB_floor[t-1] 높거나 같은 상태에서
    # low[t0]가 close_LB_floor[t-1] 보다 낮다면
    # close_LB_floor[t-1] 가격에 진입확정, t0에 시그널 생성 -> [t-1진입 t0 부터 리턴계산]
buy_breakdown_signal= np.where( ((raw_data_dropna['low'].shift(1)>=raw_data_dropna['close_LB_floor'].shift(1)).fillna(False)& # 터치하는건 체결가정 X
                                (raw_data_dropna['low']<raw_data_dropna['close_LB_floor'].shift(1)) ).fillna(False), 1, 0)    # 매수 체결

    #seq3: sell_breakup_signal
    # high[t-1]가 close_UB_ceil[t-1] 낮거나 같은 상태에서
    # high[t0]가 close_UB_ceil[t-1] 보다 높다면
    # close_UB_ceil[t-1] 가격에 청산확정, t0에 시그널 생성 -> [t-1진입 t0 부터 리턴계산]
sell_breakup_signal= np.where( ((raw_data_dropna['high'].shift(1)<=raw_data_dropna['close_UB_ceil'].shift(1)).fillna(False)& # 터치하는건 체결가정 X
                                (raw_data_dropna['high']>raw_data_dropna['close_UB_ceil'].shift(1)) ).fillna(False), -1, 0)  # 매도 체결


    #seq2: buy_breakup_signal: low의 지지가 확인되는 봉이 완성되면 시장가 매수
    # low[t-2]가 close_LB[t-2] 보다 낮은 상태에서
    # low[t-1]이 close_LB_ceil[t-1] 보다 같거나 높다면
    # close[t-1]+1 가격에 진입확정, t0에 시그널 생성 -> [t-1진입 t0 부터 리턴계산]
buy_breakup_signal= np.where( ((raw_data_dropna['low'].shift(2)<raw_data_dropna['close_LB'].shift(2)).fillna(False)&          # 아래있는 신호
                                (raw_data_dropna['low'].shift(1)>=raw_data_dropna['close_LB_ceil'].shift(1)).fillna(False) ), 1, 0)# 지지 확인후 매수 체결

    #seq4: sell_breakdown_signal: high의 저항이 확인되는 봉이 완성되면 시장가 매도
    # high[t-2]가 close_UB[t-2] 보다 높은 상태에서
    # high[t-1]이 close_UB ceil[t-1] 보다 같거나 낮다면
    # close[t-1]-1 가격에 청산확정,t0에 시그널 생성 -> [t-1진입 t0 부터 리턴계산]
sell_breakdown_signal= np.where( ((raw_data_dropna['high'].shift(2)>raw_data_dropna['close_UB'].shift(2)).fillna(False)&          # 위에 있는 신호
                                (raw_data_dropna['high'].shift(1)<=raw_data_dropna['close_UB_floor'].shift(1)).fillna(False) ), -1, 0)  # 저항 확인후 매도 체결

# 2. 포지션 계산 [ 1과 0 ]
    # 신호를 기반으로 거래 이벤트 생성 (Buy: +1, Sell: -1)
raw_data_dropna['signal_DownUp'] = buy_breakdown_signal + sell_breakup_signal
raw_data_dropna['signal_DownDown'] = buy_breakdown_signal + sell_breakdown_signal
raw_data_dropna['signal_UpDown'] = buy_breakup_signal + sell_breakup_signal
raw_data_dropna['signal_UpUp'] = buy_breakup_signal + sell_breakdown_signal

    #upup
raw_data_dropna['Position_DownUp'] = np.where(raw_data_dropna['signal_DownUp'] == 1, 
                                       1,
                                       np.where(raw_data_dropna['signal_DownUp'] == -1, 
                                                0,
                                                np.nan
                                                )
                                      )
raw_data_dropna['Position_DownUp'] = raw_data_dropna['Position_DownUp'].ffill().fillna(0).infer_objects(copy=False).astype(int)

    #downdown
raw_data_dropna['Position_DownDown'] = 0
raw_data_dropna['Position_DownDown'] = np.where(raw_data_dropna['signal_DownDown'] == 1, 
                                       1,
                                       np.where(raw_data_dropna['signal_DownDown'] == -1, 
                                                0,
                                                np.nan
                                                )
                                      )
raw_data_dropna['Position_DownDown'] = raw_data_dropna['Position_DownDown'].ffill().fillna(0).infer_objects(copy=False).astype(int)
    #updown
raw_data_dropna['Position_UpDown'] = 0
raw_data_dropna['Position_UpDown'] = np.where(raw_data_dropna['signal_UpDown'] == 1, 
                                       1,
                                       np.where(raw_data_dropna['signal_UpDown'] == -1, 
                                                0,
                                                np.nan
                                                )
                                      )
raw_data_dropna['Position_UpDown'] = raw_data_dropna['Position_UpDown'].ffill().fillna(0).infer_objects(copy=False).astype(int)
    #upup
raw_data_dropna['Position_UpUp'] = 0
raw_data_dropna['Position_UpUp'] = np.where(raw_data_dropna['signal_UpUp'] == 1, 
                                       1,
                                       np.where(raw_data_dropna['signal_UpUp'] == -1, 
                                                0,
                                                np.nan
                                                )
                                      )
raw_data_dropna['Position_UpUp'] = raw_data_dropna['Position_UpUp'].ffill().fillna(0).infer_objects(copy=False).astype(int)

# 3. 수익률 계산
ret_minute=raw_data_dropna['close'].pct_change()
ret_minute.iloc[0] = 0
    #DownUp
return_DownUp_before_TransactionCost = raw_data_dropna['Position_DownUp']*ret_minute
return_DownUp_after_TransactionCost= return_DownUp_before_TransactionCost.copy()
        #1 seq1: close_LB_floor[t-1] 가격에 진입확정, t0에 시그널 생성
position_DownUp_open_boolean=(raw_data_dropna['signal_DownUp']==1)&(raw_data_dropna['Position_DownUp'].shift(1)==0)
position_DownUp_open_return_with_TransactionCost=raw_data_dropna['close']/raw_data_dropna['close_LB_floor'].shift(1)-1-transaction_cost
return_DownUp_after_TransactionCost[position_DownUp_open_boolean]=position_DownUp_open_return_with_TransactionCost[position_DownUp_open_boolean]
        # seq3: close_UB_ceil[t-1] 가격에 청산확정, t0에 시그널 생성
position_DownUp_close_boolean=(raw_data_dropna['signal_DownUp']==-1)&(raw_data_dropna['Position_DownUp'].shift(1)==1)
position_DownUp_close_return_with_TransactionCost=raw_data_dropna['close_UB_ceil']/raw_data_dropna['close'].shift(1)-1-transaction_cost
return_DownUp_after_TransactionCost[position_DownUp_close_boolean]=position_DownUp_close_return_with_TransactionCost[position_DownUp_close_boolean]
        # monthly sharpe ratio
portfolio_value_monthly_return_DownUp=(return_DownUp_after_TransactionCost+1).resample('ME').prod()-1
CAGR_DownUp=((portfolio_value_monthly_return_DownUp+1).prod()-1)
std_DownUp=portfolio_value_monthly_return_DownUp.std()*np.sqrt(12)
monthly_sharpe_ratio_DownUp=CAGR_DownUp/std_DownUp

    #DownDown
return_DownDown_before_TransactionCost = raw_data_dropna['Position_DownDown']*ret_minute
return_DownDown_after_TransactionCost= return_DownDown_before_TransactionCost.copy()
        # seq1: close_LB_floor[t-1] 가격에 진입확정, t0에 시그널 생성
position_DownDown_open_boolean=(raw_data_dropna['signal_DownDown']==1)&(raw_data_dropna['Position_DownDown'].shift(1)==0)
position_DownDown_open_return_with_TransactionCost=raw_data_dropna['close']/raw_data_dropna['close_LB_floor'].shift(1)-1-transaction_cost
return_DownDown_after_TransactionCost[position_DownDown_open_boolean]=position_DownDown_open_return_with_TransactionCost[position_DownDown_open_boolean]
        # seq4: close[t-1]-1 가격에 청산확정,t0에 시그널 생성
position_DownDown_close_boolean=(raw_data_dropna['signal_DownDown']==-1)&(raw_data_dropna['Position_DownDown'].shift(1)==1)
position_DownDown_close_return_with_TransactionCost=(raw_data_dropna['close'].shift(1)-1)/raw_data_dropna['close'].shift(1)-1-transaction_cost
return_DownDown_after_TransactionCost[position_DownDown_close_boolean]=position_DownDown_close_return_with_TransactionCost[position_DownDown_close_boolean]
        # monthly sharpe ratio
portfolio_value_monthly_return_DownDown=(return_DownDown_after_TransactionCost+1).resample('ME').prod()-1
CAGR_DownDown=((portfolio_value_monthly_return_DownDown+1).prod()-1)
std_DownDown=portfolio_value_monthly_return_DownDown.std()*np.sqrt(12)
monthly_sharpe_ratio_DownDown=CAGR_DownDown/std_DownDown

    #UpDown
return_UpDown_before_TransactionCost = raw_data_dropna['Position_UpDown']*ret_minute
return_UpDown_after_TransactionCost= return_UpDown_before_TransactionCost.copy()
        #seq2: buy_breakup_signal: low의 지지가 확인되는 봉이 완성되면 시장가 매수
        # low[t-2]가 close_LB_ceil[t-2] 보다 낮은 상태에서
        # low[t-1]이 close_LB_ceil[t-1] 보다 높다면
        # close[t-1]+1 가격에 진입확정, t0에 시그널 생성 -> [t-1진입 t0 부터 리턴계산]
position_UpDown_open_boolean=(raw_data_dropna['signal_UpDown']==1)&(raw_data_dropna['Position_UpDown'].shift(1)==0)
position_UpDown_open_return_with_TransactionCost=raw_data_dropna['close']/(raw_data_dropna['close'].shift(1)+1)-1-transaction_cost
return_UpDown_after_TransactionCost[position_UpDown_open_boolean]=position_UpDown_open_return_with_TransactionCost[position_UpDown_open_boolean]
        # seq4: close[t-1]-1 가격에 청산확정,t0에 시그널 생성
position_UpDown_close_boolean=(raw_data_dropna['signal_UpDown']==-1)&(raw_data_dropna['Position_UpDown'].shift(1)==1)
position_UpDown_close_return_with_TransactionCost=(raw_data_dropna['close'].shift(1)-1)/raw_data_dropna['close'].shift(1)-1-transaction_cost
return_UpDown_after_TransactionCost[position_UpDown_close_boolean]=position_UpDown_close_return_with_TransactionCost[position_UpDown_close_boolean]
        # monthly sharpe ratio
portfolio_value_monthly_return_UpDown=(return_UpDown_after_TransactionCost+1).resample('ME').prod()-1
CAGR_UpDown=((portfolio_value_monthly_return_UpDown+1).prod()-1)
std_UpDown=portfolio_value_monthly_return_UpDown.std()*np.sqrt(12)
monthly_sharpe_ratio_UpDown=CAGR_UpDown/std_UpDown

        #UpUp
return_UpUp_before_TransactionCost = raw_data_dropna['Position_UpUp']*ret_minute
return_UpUp_after_TransactionCost= return_UpUp_before_TransactionCost.copy()
        # seq2: close_LB_ceil[t-1] 가격에 진입확정, t0에 시그널 생성
position_UpUp_open_boolean=(raw_data_dropna['signal_UpUp']==1)&(raw_data_dropna['Position_UpUp'].shift(1)==0)
position_UpUp_open_return_with_TransactionCost=raw_data_dropna['close']/(raw_data_dropna['close'].shift(1)+1)-1-transaction_cost
return_UpUp_after_TransactionCost[position_UpUp_open_boolean]=position_UpUp_open_return_with_TransactionCost[position_UpUp_open_boolean]
        # seq3: close_UB_ceil[t-1] 가격에 청산확정, t0에 시그널 생성
position_UpUp_close_boolean=(raw_data_dropna['signal_UpUp']==-1)&(raw_data_dropna['Position_UpUp'].shift(1)==1)
position_UpUp_close_return_with_TransactionCost=raw_data_dropna['close_UB_ceil']/raw_data_dropna['close'].shift(1)-1-transaction_cost
return_UpUp_after_TransactionCost[position_UpUp_close_boolean]=position_UpUp_close_return_with_TransactionCost[position_UpUp_close_boolean]
        # monthly sharpe ratio
portfolio_value_monthly_return_UpUp=(return_UpUp_after_TransactionCost+1).resample('ME').prod()-1
CAGR_UpUp=((portfolio_value_monthly_return_UpUp+1).prod()-1)
std_UpUp=portfolio_value_monthly_return_UpUp.std()*np.sqrt(12)
monthly_sharpe_ratio_UpUp=CAGR_UpUp/std_UpUp

CAGR_DownUp
CAGR_DownDown
CAGR_UpDown
CAGR_UpUp

std_DownUp
std_DownDown
std_UpDown
std_UpUp

monthly_sharpe_ratio_DownUp
monthly_sharpe_ratio_DownDown
monthly_sharpe_ratio_UpDown
monthly_sharpe_ratio_UpUp

position_DownUp_open_boolean.sum()
position_UpDown_open_boolean.sum()
position_DownDown_open_boolean.sum()
position_UpUp_open_boolean.sum()


position_DownUp_close_boolean.sum()
position_UpDown_close_boolean.sum()
position_DownDown_close_boolean.sum()
position_UpUp_close_boolean.sum()