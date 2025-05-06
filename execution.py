import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import time
import matplotlib.pyplot as plt
from datetime import datetime
import warnings
import re
import ccxt.pro
import asyncio
import os
import json
from pathlib import Path
from data_manager import KimchiPremiumDataManager
warnings.filterwarnings('ignore')

class KimchiPremiumTrader:
    def __init__(self, data_dir=None, sma_window=60*24, lb_threshold=1, ub_threshold=1, 
                 transaction_cost=0.0004, buffer=4, max_data_points=None, api_config=None):
        # 설정 파라미터
        self.sma_window = sma_window
        self.sd_window = sma_window
        self.lb_threshold = lb_threshold
        self.ub_threshold = ub_threshold
        self.transaction_cost = transaction_cost
        self.buffer = buffer
        # 최대 데이터 포인트 수 (기본값은 SMA_WINDOW의 3배)
        self.max_data_points = max_data_points if max_data_points else sma_window * 3
        
        # 데이터 및 상태 저장
        self.data = pd.DataFrame()
        self.position = 0
        self.entry_price = None
        self.trades = []
        self.last_signal = None
        
        # 데이터 저장 경로
        self.data_dir = Path(data_dir) if data_dir else Path('data')
        self.data_dir.mkdir(exist_ok=True)
        
        # API 키 설정
        self.api_config = api_config or self.load_api_config()
        
        # 데이터 매니저 초기화
        self.data_manager = KimchiPremiumDataManager(data_dir=self.data_dir)
        
        # 거래용 거래소 객체 (API 키 적용)
        self.trading_exchange = None

    def load_api_config(self):
        """API 키 설정 불러오기 (환경 변수 또는 config.json 파일에서)"""
        # 1. 환경 변수에서 API 키 불러오기 시도
        api_key = os.environ.get('BITHUMB_API_KEY')
        secret_key = os.environ.get('BITHUMB_SECRET_KEY')
        
        # 환경 변수에 설정되어 있으면 그대로 사용
        if api_key and secret_key:
            print("환경 변수에서 API 키 설정을 불러왔습니다.")
            return {
                'apiKey': api_key,
                'secret': secret_key
            }
        
        # 2. config.json 파일에서 API 키 불러오기 시도
        config_path = 'trading_config.json'
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                if 'bithumb' in config and 'apiKey' in config['bithumb'] and 'secret' in config['bithumb']:
                    print("config.json 파일에서 API 키 설정을 불러왔습니다.")
                    return config['bithumb']
            except Exception as e:
                print(f"config.json 파일 읽기 오류: {e}")
        
        # 3. 설정 파일이 없으면 경고 메시지 출력
        print("""
        ===== 경고: API 키 설정이 필요합니다 =====
        다음 두 가지 방법 중 하나로 API 키를 설정해주세요:
        
        1. 환경 변수 설정:
           - BITHUMB_API_KEY
           - BITHUMB_SECRET_KEY
           
        2. config.json 파일 생성:
           {
               "bithumb": {
                   "apiKey": "your_api_key_here",
                   "secret": "your_secret_key_here"
               }
           }
        
        API 키 없이는 실제 거래가 불가능합니다.
        """)
        return None
    
    async def load_recent_data(self, required_data_points=None):
        """데이터 매니저에서 최근 데이터 로드"""
        if required_data_points is None:
            required_data_points = self.sma_window * 2  # 충분한 데이터 로드 (SMA 계산 + 추가 여유)
        
        try:
            print(f"데이터 매니저에서 최근 {required_data_points}개의 데이터 로드 중...")
            
            # 모든 pickle 데이터 로드
            all_data = self.data_manager.load_all_market_history_pickle()
            
            if len(all_data) == 0:
                print("데이터 매니저에 저장된 데이터가 없습니다.")
                return False
                
            # 전처리
            all_data = self.data_manager.preprocess_data(all_data)
            
            # 가장 최근 데이터 선택
            recent_data = all_data.sort_values('timestamp').tail(required_data_points)
            
            # 데이터 변환
            self.data = pd.DataFrame({
                'timestamp': recent_data['timestamp'],
                'exchange_rate': recent_data['exchange_rate'],
                'USDT_PRICE': recent_data['usdt_price'],
                'K_prem(%)': recent_data['kimchi_premium'],
                'close': recent_data['usdt_price']
            })
            
            print(f"총 {len(self.data)}개의 데이터 포인트 로드 완료 (필요: {required_data_points}, 사용 가능: {len(all_data)})")
            
            # 데이터가 충분한지 확인
            if len(self.data) < self.sma_window:
                print(f"경고: 로드된 데이터가 SMA 계산에 필요한 양보다 적습니다. (로드됨: {len(self.data)}, 필요: {self.sma_window})")
                return False
                
            return True
            
        except Exception as e:
            print(f"데이터 로드 오류: {e}")
            return False
            
    def calculate_bands(self):
        """이동평균 및 밴드 계산"""
        try:
            # 충분한 데이터가 있는지 확인
            if len(self.data) < self.sma_window:
                print(f"밴드 계산을 위한 충분한 데이터가 없습니다. (현재: {len(self.data)}/{self.sma_window})")
                return
                
            # SMA 및 표준편차 계산
            self.data['SMA'] = self.data['K_prem(%)'].rolling(self.sma_window).mean()
            sd = self.data['K_prem(%)'].rolling(self.sd_window).std()
            
            # 밴드 계산
            self.data['LB'] = self.data['SMA'] - self.lb_threshold * sd
            self.data['UB'] = self.data['SMA'] + self.ub_threshold * sd
            
            # 가격 밴드 계산 
            self.data['close_LB'] = self.data['USDT_PRICE'] * (1 + self.data['LB']/100)
            self.data['close_UB'] = self.data['USDT_PRICE'] * (1 + self.data['UB']/100)
            
            # 마지막 행에 값이 계산되었는지 확인
            last_row = self.data.iloc[-1]
            if not pd.isna(last_row['close_LB']) and not pd.isna(last_row['close_UB']):
                print(f"밴드 계산 완료 - LB: {last_row['close_LB']}, UB: {last_row['close_UB']}")
        except Exception as e:
            print(f"밴드 계산 오류: {e}")
            # 오류 추적을 위한 추가 정보
            print(f"데이터 크기: {len(self.data)}, SMA 윈도우: {self.sma_window}")
            if len(self.data) > 0:
                print(f"첫 번째 행: {self.data.iloc[0]['K_prem(%)']}, 마지막 행: {self.data.iloc[-1]['K_prem(%)']}")

    def generate_signals(self):
        """매매 신호 생성"""
        try:
            idx = len(self.data) - 1
            if idx < 0:  # 최소 1개의 데이터 필요
                return
                
            # 필요한 컬럼이 모두 있는지 확인
            required_columns = ['close', 'close_LB', 'close_UB']
            for col in required_columns:
                if col not in self.data.columns:
                    print(f"매매 신호 생성에 필요한 컬럼 '{col}'이 없습니다.")
                    return
            
            # 현재 행 가져오기
            curr = self.data.iloc[idx]
            
            # NaN 값 확인
            for col in required_columns:
                if pd.isna(curr[col]):
                    print(f"매매 신호 생성에 필요한 컬럼 '{col}'에 NaN 값이 있습니다.")
                    return
            
            # 매수 신호: 현재 close가 close_LB 보다 낮거나 같을 때
            buy_signal = (curr['close'] <= curr['close_LB']+self.buffer)
            
            # 매도 신호: 현재 close가 close_UB 보다 높거나 같을 때
            sell_signal = (curr['close'] >= curr['close_UB']-self.buffer)
            
            if buy_signal:
                self.last_signal = "BUY"
                print(f"[{curr['timestamp']}] 매수 신호 발생: 김치 프리미엄 {curr['K_prem(%)']}%")
            elif sell_signal:
                self.last_signal = "SELL"
                print(f"[{curr['timestamp']}] 매도 신호 발생: 김치 프리미엄 {curr['K_prem(%)']}%")
            else:
                self.last_signal = None
                
        except Exception as e:
            print(f"매매 신호 생성 오류: {e}")
            self.last_signal = None
            
    async def update_data_from_manager(self):
        """데이터 매니저에서 최근 데이터를 가져와서 업데이트"""
        try:
            # 데이터 매니저에서 데이터 수집 (한 번만 데이터 수집)
            result = await self.data_manager.collect_data_once()
            
            if not result:
                print("데이터 매니저에서 새 데이터를 수집할 수 없습니다.")
                return False
                
            # 새 데이터 행 생성
            new_row = pd.DataFrame({
                'timestamp': [result['timestamp']],
                'exchange_rate': [result['exchange_rate']],
                'USDT_PRICE': [result['usdt_price']],
                'K_prem(%)': [result['kimchi_premium']],
                'close': [result['usdt_price']]
            })
            
            # 새로운 데이터 추가
            if len(self.data) == 0:
                self.data = new_row
            else:
                # 데이터 크기 제한 - 최대 데이터 포인트 수를 초과하면 오래된 데이터부터 삭제
                if len(self.data) >= self.max_data_points:
                    # 마지막 윈도우 크기의 2배 데이터만 유지 (충분한 데이터 유지하면서 메모리 관리)
                    self.data = self.data.iloc[-(self.sma_window * 2):].reset_index(drop=True)
                
                # 새 행 추가 (효율적인 방법)
                self.data = pd.concat([self.data, new_row], ignore_index=True)
            
            print(f"데이터 매니저에서 새 데이터 수집 완료. 총 데이터: {len(self.data)}")
            return True
            
        except Exception as e:
            print(f"데이터 매니저에서 데이터 업데이트 오류: {e}")
            return False
            
    def save_trades_to_csv(self, filename='trades_history.csv'):
        """거래 내역을 CSV 파일로 저장"""
        if not self.trades:
            print("저장할 거래 내역이 없습니다.")
            return
        
        try:
            trades_df = pd.DataFrame(self.trades)
            trades_df.to_csv(filename, index=False)
            print(f"거래 내역이 {filename}에 저장되었습니다.")
        except Exception as e:
            print(f"거래 내역 저장 오류: {e}")
    
    async def run(self, interval_seconds=60):
        """실시간 거래 시그널 생성 및 실행"""
        print(f"김치 프리미엄 거래 시작 (간격: {interval_seconds}초)")
        print(f"설정: SMA={self.sma_window}, LB={self.lb_threshold}, UB={self.ub_threshold}, 최대 데이터={self.max_data_points}")
        
        # 초기 데이터 로드
        success = await self.load_recent_data()
        if not success:
            print("초기 데이터 로드에 실패했습니다. 데이터 매니저가 충분한 데이터를 수집할 때까지 기다리세요.")
            return
        
        # 밴드 계산 (초기 데이터로)
        self.calculate_bands()
        
        # 매매 신호 생성 (초기 데이터로)
        self.generate_signals()
        
        try:
            # 데이터 조회용 거래소 초기화
            await self.data_manager.initialize_exchange()
            
            # 거래용 거래소 초기화
            trading_success = await self.initialize_trading_exchange()
            if not trading_success:
                print("주의: 거래용 거래소 초기화 실패. 시뮬레이션 모드로 동작합니다.")
            
            import gc
            last_memory_cleanup = datetime.now()
            
            while True:
                # 데이터 매니저에서 새로운 데이터 업데이트
                updated = await self.update_data_from_manager()
                
                if updated:
                    # 밴드 계산 업데이트
                    self.calculate_bands()
                    
                    # 매매 신호 업데이트 
                    self.generate_signals()
                    
                    # 포지션 업데이트 (거래 실행)
                    await self.update_position()
                    
                    # 마지막 데이터 출력
                    if len(self.data) > 0:
                        last_row = self.data.iloc[-1]
                        print(f"[{last_row['timestamp']}] 김치 프리미엄: {last_row['K_prem(%)']}%, "
                              f"USDT 가격: {last_row['USDT_PRICE']}, 환율: {last_row['exchange_rate']}")
                        
                        # 밴드 정보 출력 (계산되었다면)
                        if 'close_LB' in self.data.columns and 'close_UB' in self.data.columns:
                            if not pd.isna(last_row['close_LB']) and not pd.isna(last_row['close_UB']):
                                print(f"밴드 범위: LB({last_row['close_LB']}) ~ UB({last_row['close_UB']})")
                        
                        # 포지션 상태 출력
                        if self.position == 1:
                            unrealized_profit = (last_row['USDT_PRICE'] / self.entry_price - 1) * 100
                            print(f"현재 포지션: 롱, 진입가: {self.entry_price}, 미실현 손익: {unrealized_profit:.2f}%")
                    
                    # 미체결 주문 확인 및 관리
                    await self.check_orders()
                
                # 거래 내역 저장 (1시간에 한 번)
                timestamp = datetime.now()
                if timestamp.minute == 0 and timestamp.second < interval_seconds:
                    self.save_trades_to_csv()
                    
                # 한 시간마다 메모리 정리
                if (timestamp - last_memory_cleanup).seconds > 3600:
                    gc.collect()
                    last_memory_cleanup = timestamp
                    print(f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] 주기적 메모리 정리 완료")
                
                # 인터벌 대기
                await asyncio.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n거래 종료...")
            self.save_trades_to_csv()
        except Exception as e:
            print(f"오류 발생: {e}")
            self.save_trades_to_csv()
        finally:
            # 거래소 연결 종료
            await self.data_manager.close_exchange()
            await self.close_trading_exchange()

    async def execute_trade(self, action, price, symbol='USDT/KRW', amount=None):
        """빗썸 거래소에서 지정가 주문 실행"""
        try:
            # 거래소 객체 확인
            if not self.trading_exchange:
                # 거래소 초기화 시도
                success = await self.initialize_trading_exchange()
                if not success:
                    # API 키가 없는 경우 시뮬레이션 모드
                    print(f"API 키가 설정되지 않아 {action} 주문을 실행할 수 없습니다.")
                    return None
                
            # 가격이 유효한지 확인
            if not price or price <= 0:
                print(f"유효하지 않은 가격: {price}")
                return None
                
            # 수량이 유효한지 확인
            if not amount or amount <= 0:
                print(f"유효하지 않은 수량: {amount}")
                return None
                
            if action == 'BUY':
                # 지정가 매수 주문
                order = await self.trading_exchange.create_limit_buy_order(symbol, amount, price)
                print(f"매수 지정가 주문 실행: {symbol}, 수량: {amount}, 가격: {price}")
                return order
            elif action == 'SELL':
                # 지정가 매도 주문
                order = await self.trading_exchange.create_limit_sell_order(symbol, amount, price)
                print(f"매도 지정가 주문 실행: {symbol}, 수량: {amount}, 가격: {price}")
                return order
            else:
                print(f"지원하지 않는 주문 유형: {action}")
                return None
        except Exception as e:
            print(f"거래 실행 오류: {e}")
            return None

    async def update_position(self):
        """포지션 업데이트 및 거래 실행"""
        try:
            if not self.last_signal:
                return
                
            idx = len(self.data) - 1
            if idx < 0:  # 최소 1개의 데이터 필요
                return
                
            # 필요한 컬럼이 있는지 확인
            required_columns = ['close_LB', 'close_UB', 'K_prem(%)']
            for col in required_columns:
                if col not in self.data.columns:
                    print(f"포지션 업데이트에 필요한 컬럼 '{col}'이 없습니다.")
                    return
                    
            # 현재 행 가져오기
            curr = self.data.iloc[idx]
            
            # NaN 값 확인
            for col in required_columns:
                if pd.isna(curr[col]):
                    print(f"포지션 업데이트에 필요한 컬럼 '{col}'에 NaN 값이 있습니다.")
                    return
            
            # 테스트용 거래 수량 (실제로는 자금 관리에 맞게 조정해야 함)
            trade_amount = 10  # USDT
            
            # 오더북 데이터 가져오기
            orderbook = await self.fetch_orderbook('USDT/KRW')
            best_bid = orderbook['best_bid']  # 매수 1호가
            best_ask = orderbook['best_ask']  # 매도 1호가
            
            if not best_bid or not best_ask:
                print("오더북 데이터를 가져올 수 없어 거래를 진행할 수 없습니다.")
                return
            
            if self.last_signal == "BUY" and self.position == 0:
                # 목표 매수 가격 (LB)
                buy_price = np.floor(curr['close_LB'])
                print(f"매수 신호 감지: LB 가격 {buy_price}, 매수 1호가 {best_bid}")
                
                # 매수 주문 전략 결정
                if buy_price > best_bid:
                    # LB가 매수 1호가보다 높은 경우: 일반 매수 주문
                    print(f"LB({buy_price})가 매수 1호가({best_bid})보다 높아 일반 매수 주문 실행")
                    order = await self.execute_trade('BUY', buy_price, 'USDT/KRW', trade_amount)
                elif buy_price == best_bid:
                    # LB가 매수 1호가와 정확히 일치: 일반 매수 주문
                    print(f"LB({buy_price})가 매수 1호가({best_bid})와 일치하여 일반 매수 주문 실행")
                    order = await self.execute_trade('BUY', buy_price, 'USDT/KRW', trade_amount)
                else:
                    # LB가 매수 1호가보다 낮은 경우: Chase 매수 주문
                    print(f"LB({buy_price})가 매수 1호가({best_bid})보다 낮아 Chase 매수 주문 실행")
                    order = await self.chase_limit_order('BUY', None, buy_price, trade_amount)
                
                if order:
                    # 실제 체결 가격 가져오기
                    actual_price = float(order['price']) if 'price' in order else buy_price
                    self.position = 1
                    self.entry_price = actual_price
                    
                    # 거래 기록
                    trade = {
                        'timestamp': curr['timestamp'],
                        'action': 'BUY',
                        'price': self.entry_price,
                        'premium': curr['K_prem(%)'],
                        'order_id': order['id'] if 'id' in order else None,
                        'amount': trade_amount,
                        'strategy': 'Chase' if buy_price < best_bid else 'Limit'
                    }
                    self.trades.append(trade)
                    
                    print(f"[{curr['timestamp']}] 매수 포지션 진입: 가격 {self.entry_price}, 수량 {trade_amount}, 김치 프리미엄 {curr['K_prem(%)']}%")
                
            elif self.last_signal == "SELL" and self.position == 1:
                # 목표 매도 가격 (UB)
                sell_price = np.ceil(curr['close_UB'])
                print(f"매도 신호 감지: UB 가격 {sell_price}, 매도 1호가 {best_ask}")
                
                # 매도 주문 전략 결정
                if sell_price < best_ask:
                    # UB가 매도 1호가보다 낮은 경우: 일반 매도 주문
                    print(f"UB({sell_price})가 매도 1호가({best_ask})보다 낮아 일반 매도 주문 실행")
                    order = await self.execute_trade('SELL', sell_price, 'USDT/KRW', trade_amount)
                elif sell_price == best_ask:
                    # UB가 매도 1호가와 정확히 일치: 일반 매도 주문
                    print(f"UB({sell_price})가 매도 1호가({best_ask})와 일치하여 일반 매도 주문 실행")
                    order = await self.execute_trade('SELL', sell_price, 'USDT/KRW', trade_amount)
                else:
                    # UB가 매도 1호가보다 높은 경우: Chase 매도 주문
                    print(f"UB({sell_price})가 매도 1호가({best_ask})보다 높아 Chase 매도 주문 실행")
                    order = await self.chase_limit_order('SELL', None, sell_price, trade_amount)
                
                if order:
                    # 실제 체결 가격 가져오기
                    actual_price = float(order['price']) if 'price' in order else sell_price
                    exit_price = actual_price
                    profit_pct = (exit_price / self.entry_price - 1 - self.transaction_cost) * 100
                    
                    # 거래 기록
                    trade = {
                        'timestamp': curr['timestamp'],
                        'action': 'SELL',
                        'price': exit_price,
                        'entry_price': self.entry_price,
                        'profit_pct': profit_pct,
                        'premium': curr['K_prem(%)'],
                        'order_id': order['id'] if 'id' in order else None,
                        'amount': trade_amount,
                        'strategy': 'Chase' if sell_price > best_ask else 'Limit'
                    }
                    self.trades.append(trade)
                    
                    print(f"[{curr['timestamp']}] 매도 포지션 청산: 가격 {exit_price}, 수량 {trade_amount}, 수익률 {profit_pct:.2f}%, 김치 프리미엄 {curr['K_prem(%)']}%")
                    
                    # 포지션 초기화
                    self.position = 0
                    self.entry_price = None
        except Exception as e:
            print(f"포지션 업데이트 오류: {e}")
            # 오류 추적을 위한 추가 정보
            if 'curr' in locals():
                print(f"curr 데이터: {curr['timestamp']}")
            else:
                print("데이터 접근 오류")

    async def cancel_order(self, order_id, symbol='USDT/KRW'):
        """주문 취소"""
        # 거래소 객체 확인
        if not self.trading_exchange:
            # 거래소 초기화 시도
            success = await self.initialize_trading_exchange()
            if not success:
                # 시뮬레이션 모드에서는 취소 성공으로 처리
                if order_id.startswith('sim_'):
                    print(f"시뮬레이션 주문 취소: {order_id}")
                    return {'id': order_id, 'status': 'canceled'}
                print(f"API 키가 설정되지 않아 주문 {order_id}를 취소할 수 없습니다.")
                return None
            
        try:
            # 시뮬레이션 주문인 경우
            if order_id.startswith('sim_'):
                print(f"시뮬레이션 주문 취소: {order_id}")
                return {'id': order_id, 'status': 'canceled'}
                
            # 실제 주문 취소
            result = await self.trading_exchange.cancel_order(order_id, symbol)
            print(f"주문 취소 성공: {order_id}")
            return result
        except Exception as e:
            print(f"주문 취소 오류: {e}")
            # API 키 문제인 경우 시뮬레이션 모드로 전환
            if "apiKey" in str(e):
                print("API 키 오류로 시뮬레이션 모드로 전환합니다.")
                return {'id': order_id, 'status': 'canceled', 'info': {'simulated': True, 'error': str(e)}}
            return None
            
    async def check_orders(self):
        """미체결 주문 확인 및 관리 - 1분마다 실행
        
        미체결 주문이 있을 때:
        - 매수 주문: LB와 매수 1호가를 비교해 필요시 Chase 전략 적용
        - 매도 주문: UB와 매도 1호가를 비교해 필요시 Chase 전략 적용
        """
        try:
            # 시장 정보 없이는 진행 불가
            if len(self.data) < self.sma_window:
                print(f"충분한 데이터가 없습니다. 현재 {len(self.data)}/{self.sma_window} 데이터 포인트")
                return []
                
            # 밴드 계산이 완료되었는지 확인 (필요한 컬럼이 있는지 확인)
            required_columns = ['close_LB', 'close_UB']
            for col in required_columns:
                if col not in self.data.columns:
                    print(f"필요한 컬럼 '{col}'이 아직 계산되지 않았습니다.")
                    return []
                
            # 현재 최신 데이터
            curr = self.data.iloc[-1]
            
            # NaN 값 확인
            for col in required_columns:
                if pd.isna(curr[col]):
                    print(f"필요한 컬럼 '{col}'에 NaN 값이 있습니다. 계산이 완료될 때까지 기다립니다.")
                    return []
            
            # 현재 밴드 가격
            lb_price = np.floor(curr['close_LB'])
            ub_price = np.ceil(curr['close_UB'])
            
            # 오더북 데이터 가져오기
            orderbook = await self.fetch_orderbook('USDT/KRW')
            best_bid = orderbook['best_bid']  # 매수 1호가
            best_ask = orderbook['best_ask']  # 매도 1호가
            
            if not best_bid or not best_ask:
                print("오더북 데이터를 가져올 수 없어 주문 관리를 진행할 수 없습니다.")
                return []
            
            # 거래소 객체 확인
            if not self.trading_exchange:
                # 거래소 초기화 시도
                success = await self.initialize_trading_exchange()
                if not success:
                    print("거래용 거래소 초기화 실패: API 키가 설정되지 않았습니다.")
                    return []
            
            # 열린 주문 조회
            try:
                open_orders = await self.trading_exchange.fetch_open_orders('USDT/KRW')
            except Exception as e:
                print(f"미체결 주문 조회 오류: {e}")
                # API 키 문제인 경우 시뮬레이션 모드 안내
                if "apiKey" in str(e):
                    print("API 키 오류로 시뮬레이션 모드로 전환합니다.")
                return []
            
            if open_orders:
                print(f"미체결 주문 수: {len(open_orders)}")
                
                for order in open_orders:
                    order_price = float(order['price'])
                    order_type = order['side'].upper()  # 'buy' 또는 'sell'
                    order_id = order['id']
                    order_amount = float(order['amount'])
                    
                    print(f"주문 ID: {order_id}, 타입: {order_type}, 가격: {order_price}, 수량: {order_amount}, 상태: {order['status']}")
                    
                    # 매수 주문의 경우
                    if order_type == 'BUY':
                        # LB 가격이 변동되고 주문 가격과 다른 경우만 처리
                        if lb_price != order_price:
                            # 매수 주문 가격이 매수 1호가와 정확히 일치하면 유지
                            if order_price == best_bid:
                                print(f"현재 매수 주문이 매수 1호가({best_bid})와 일치하므로 유지합니다.")
                                continue
                                
                            if lb_price > best_bid:
                                # LB > 매수 1호가인 경우: 일반 주문으로 업데이트
                                print(f"LB({lb_price})가 매수 1호가({best_bid})보다 높아 일반 주문으로 갱신")
                                await self.cancel_order(order_id, 'USDT/KRW')
                                new_order = await self.execute_trade('BUY', lb_price, 'USDT/KRW', order_amount)
                                if new_order:
                                    print(f"새 매수 주문 생성: 가격 {lb_price}, 수량 {order_amount}")
                            else:
                                # LB < 매수 1호가인 경우: Chase 주문으로 업데이트
                                print(f"LB({lb_price})가 매수 1호가({best_bid})보다 낮아 Chase 주문으로 갱신")
                                await self.cancel_order(order_id, 'USDT/KRW')
                                new_order = await self.chase_limit_order('BUY', None, lb_price, order_amount)
                                if new_order:
                                    print(f"새 Chase 매수 주문 생성: 수량 {order_amount}")
                    
                    # 매도 주문의 경우
                    elif order_type == 'SELL':
                        # UB 가격이 변동되고 주문 가격과 다른 경우만 처리
                        if ub_price != order_price:
                            # 매도 주문 가격이 매도 1호가와 정확히 일치하면 유지
                            if order_price == best_ask:
                                print(f"현재 매도 주문이 매도 1호가({best_ask})와 일치하므로 유지합니다.")
                                continue
                                
                            if ub_price < best_ask:
                                # UB < 매도 1호가인 경우: 일반 주문으로 업데이트
                                print(f"UB({ub_price})가 매도 1호가({best_ask})보다 낮아 일반 주문으로 갱신")
                                await self.cancel_order(order_id, 'USDT/KRW')
                                new_order = await self.execute_trade('SELL', ub_price, 'USDT/KRW', order_amount)
                                if new_order:
                                    print(f"새 매도 주문 생성: 가격 {ub_price}, 수량 {order_amount}")
                            else:
                                # UB > 매도 1호가인 경우: Chase 주문으로 업데이트
                                print(f"UB({ub_price})가 매도 1호가({best_ask})보다 높아 Chase 주문으로 갱신")
                                await self.cancel_order(order_id, 'USDT/KRW')
                                new_order = await self.chase_limit_order('SELL', None, ub_price, order_amount)
                                if new_order:
                                    print(f"새 Chase 매도 주문 생성: 수량 {order_amount}")
            
            return open_orders
        except Exception as e:
            print(f"주문 조회/관리 오류: {e}")
            return []

    async def fetch_orderbook(self, symbol='USDT/KRW'):
        """오더북 데이터 가져오기"""
        try:
            # 데이터 매니저의 거래소 객체로 시세 데이터 가져오기
            # (API 키가 필요없는 공개 API)
            if not self.data_manager.bithumb:
                await self.data_manager.initialize_exchange()
                
            orderbook = await self.data_manager.bithumb.fetch_order_book(symbol)
            
            # 매수 1호가 (최고 매수가)
            best_bid = orderbook['bids'][0][0] if len(orderbook['bids']) > 0 else None
            
            # 매도 1호가 (최저 매도가)
            best_ask = orderbook['asks'][0][0] if len(orderbook['asks']) > 0 else None
            
            return {
                'best_bid': best_bid,  # 매수 1호가
                'best_ask': best_ask   # 매도 1호가
            }
        except Exception as e:
            print(f"오더북 데이터 가져오기 오류: {e}")
            return {'best_bid': None, 'best_ask': None}

    async def chase_limit_order(self, action, order_id, target_price, amount, symbol='USDT/KRW', max_attempts=10):
        """Chase Limit Order - 시장 가격 변동에 따라 주문 가격을 조정
        
        Args:
            action: 'BUY' 또는 'SELL'
            order_id: 기존 주문 ID
            target_price: 목표 가격
            amount: 주문 수량
            symbol: 거래 심볼
            max_attempts: 최대 시도 횟수
        
        Returns:
            최종 주문 정보 또는 None
        """
        try:
            attempts = 0
            last_best_price = None
            
            while attempts < max_attempts:
                # 오더북 데이터 가져오기
                orderbook = await self.fetch_orderbook(symbol)
                
                if not orderbook['best_bid'] or not orderbook['best_ask']:
                    print("오더북 데이터를 가져올 수 없습니다. 재시도 중...")
                    await asyncio.sleep(0.1)
                    attempts += 1
                    continue
                
                if action == 'BUY':
                    best_price = orderbook['best_bid']
                    
                    # 현재 주문 가격이 매수 1호가와 일치하면 유지
                    if order_id and last_best_price is not None and last_best_price == best_price:
                        print(f"현재 Chase 매수 주문이 매수 1호가({best_price})와 계속 일치합니다.")
                        await asyncio.sleep(0.1)
                        attempts += 1
                        continue
                    
                    # 매수 1호가가 LB보다 낮은 경우에만 chase 적용
                    if best_price < target_price:
                        # 매수 1호가가 변동된 경우에만 주문 갱신
                        if best_price != last_best_price:
                            # 기존 주문 취소
                            if order_id:
                                await self.cancel_order(order_id, symbol)
                            
                            # 새 가격으로 주문
                            new_order = await self.execute_trade('BUY', best_price, symbol, amount)
                            
                            if new_order:
                                print(f"Chase 매수 주문 갱신: {best_price} (목표: {target_price})")
                                order_id = new_order['id'] if 'id' in new_order else None
                                last_best_price = best_price
                    elif best_price == target_price:
                        # 매수 1호가가 LB와 일치하는 경우, 기존 주문이 없으면 한 번만 생성
                        if not order_id or last_best_price is None:
                            new_order = await self.execute_trade('BUY', best_price, symbol, amount)
                            if new_order:
                                print(f"Chase 매수 주문 생성 (LB와 매수 1호가 일치): {best_price}")
                                order_id = new_order['id'] if 'id' in new_order else None
                                last_best_price = best_price
                        else:
                            print(f"현재 매수 1호가({best_price})가 LB와 일치하므로 기존 주문 유지")
                    else:
                        # 매수 1호가가 LB보다 높아진 경우, 원래 계획대로 진행
                        if order_id:
                            await self.cancel_order(order_id, symbol)
                        return await self.execute_trade('BUY', target_price, symbol, amount)
                
                elif action == 'SELL':
                    best_price = orderbook['best_ask']
                    
                    # 현재 주문 가격이 매도 1호가와 일치하면 유지
                    if order_id and last_best_price is not None and last_best_price == best_price:
                        print(f"현재 Chase 매도 주문이 매도 1호가({best_price})와 계속 일치합니다.")
                        await asyncio.sleep(0.1)
                        attempts += 1
                        continue
                    
                    # 매도 1호가가 UB보다 높은 경우에만 chase 적용
                    if best_price > target_price:
                        # 매도 1호가가 변동된 경우에만 주문 갱신
                        if best_price != last_best_price:
                            # 기존 주문 취소
                            if order_id:
                                await self.cancel_order(order_id, symbol)
                            
                            # 새 가격으로 주문
                            new_order = await self.execute_trade('SELL', best_price, symbol, amount)
                            
                            if new_order:
                                print(f"Chase 매도 주문 갱신: {best_price} (목표: {target_price})")
                                order_id = new_order['id'] if 'id' in new_order else None
                                last_best_price = best_price
                    elif best_price == target_price:
                        # 매도 1호가가 UB와 일치하는 경우, 기존 주문이 없으면 한 번만 생성
                        if not order_id or last_best_price is None:
                            new_order = await self.execute_trade('SELL', best_price, symbol, amount)
                            if new_order:
                                print(f"Chase 매도 주문 생성 (UB와 매도 1호가 일치): {best_price}")
                                order_id = new_order['id'] if 'id' in new_order else None
                                last_best_price = best_price
                        else:
                            print(f"현재 매도 1호가({best_price})가 UB와 일치하므로 기존 주문 유지")
                    else:
                        # 매도 1호가가 UB보다 낮아진 경우, 원래 계획대로 진행
                        if order_id:
                            await self.cancel_order(order_id, symbol)
                        return await self.execute_trade('SELL', target_price, symbol, amount)
                
                # 주문 상태 확인
                if order_id:
                    try:
                        order = await self.data_manager.bithumb.fetch_order(order_id, symbol)
                        
                        # 주문이 체결된 경우
                        if order['status'] == 'closed':
                            print(f"Chase 주문 체결 완료: {order['price']}")
                            return order
                    except Exception as e:
                        print(f"주문 상태 확인 오류: {e}")
                
                # 0.1초 간격으로 체크
                await asyncio.sleep(0.1)
                attempts += 1
            
            # 최대 시도 횟수 초과 시 마지막 주문 정보 반환
            print(f"Chase 최대 시도 횟수 초과. 마지막 주문으로 진행: {action}")
            return await self.data_manager.bithumb.fetch_order(order_id, symbol) if order_id else None
            
        except Exception as e:
            print(f"Chase Limit Order 오류: {e}")
            return None

    async def initialize_trading_exchange(self):
        """거래소 초기화 (API 키 적용한 거래용)"""
        try:
            # 기존 인스턴스가 있으면 닫기
            if self.trading_exchange:
                await self.close_trading_exchange()
                
            # API 키 확인
            if self.api_config:
                # API 키로 거래소 인스턴스 생성
                self.trading_exchange = ccxt.pro.bithumb(self.api_config)
                print("빗썸 거래소에 API 키로 연결되었습니다 (거래용)")
                return True
            else:
                print("API 키가 설정되지 않아 거래용 거래소 연결이 불가능합니다.")
                return False
        except Exception as e:
            print(f"거래용 거래소 초기화 오류: {e}")
            self.trading_exchange = None
            return False
            
    async def close_trading_exchange(self):
        """거래용 거래소 연결 종료"""
        if self.trading_exchange:
            try:
                await self.trading_exchange.close()
                print("거래용 거래소 연결이 종료되었습니다.")
            except Exception as e:
                print(f"거래용 거래소 연결 종료 중 오류: {e}")
            finally:
                self.trading_exchange = None

# 메인 실행 코드
if __name__ == "__main__":
    # 전략 파라미터 설정
    SMA_WINDOW = 5  # 테스트용으로 짧게 잡음
    LB_THRESHOLD = 1
    UB_THRESHOLD = 1
    TRANSACTION_COST = 0.0004
    BUFFER = 4
    # 최대 데이터 포인트 수 설정 (기본값: SMA_WINDOW의 3배)
    MAX_DATA_POINTS = SMA_WINDOW * 3 
    
    # 데이터 경로 설정
    data_dir = 'data'
    
    # 거래 객체 생성
    trader = KimchiPremiumTrader(
        data_dir=data_dir,
        sma_window=SMA_WINDOW,
        lb_threshold=LB_THRESHOLD,
        ub_threshold=UB_THRESHOLD,
        transaction_cost=TRANSACTION_COST,
        buffer=BUFFER,
        max_data_points=MAX_DATA_POINTS
    )
    
    # asyncio 이벤트 루프로 실행
    async def main():
        await trader.run(interval_seconds=60)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("프로그램이 사용자에 의해 종료되었습니다.")
