import asyncio
import ccxt.pro as ccxtpro
from datetime import datetime
import sqlite3
import time
from collections import defaultdict

class OHLCVCollector:
    def __init__(self, symbol: str, db_path: str = 'ohlcv.db'):
        self.symbol = symbol
        self.db_path = db_path
        self.current_candle = defaultdict(float)
        self.current_candle['timestamp'] = None
        self.initialize_db()
        
    def initialize_db(self):
        """데이터베이스 및 테이블 초기화"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 테이블 생성 (심볼별로 테이블 생성)
        table_name = self.symbol.replace('/', '_').replace(':', '_')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                timestamp INTEGER PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
        ''')
        
        conn.commit()
        conn.close()
        
    def update_candle(self, price: float, volume: float, timestamp: int):
        """현재 캔들 데이터 업데이트"""
        if self.current_candle['timestamp'] is None:
            # 새로운 캔들 시작
            self.current_candle = {
                'timestamp': timestamp,
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': volume
            }
        else:
            # 기존 캔들 업데이트
            self.current_candle['high'] = max(self.current_candle['high'], price)
            self.current_candle['low'] = min(self.current_candle['low'], price)
            self.current_candle['close'] = price
            self.current_candle['volume'] += volume
            
    def save_candle(self):
        """현재 캔들을 DB에 저장"""
        if self.current_candle['timestamp'] is None:
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        table_name = self.symbol.replace('/', '_').replace(':', '_')
        cursor.execute(f'''
            INSERT OR REPLACE INTO {table_name} 
            (timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            self.current_candle['timestamp'],
            self.current_candle['open'],
            self.current_candle['high'],
            self.current_candle['low'],
            self.current_candle['close'],
            self.current_candle['volume']
        ))
        
        conn.commit()
        conn.close()
        
        # 새로운 캔들 준비
        self.current_candle = defaultdict(float)
        self.current_candle['timestamp'] = None

async def collect_ohlcv(symbol: str):
    """1초봉 OHLCV 데이터 수집"""
    collector = OHLCVCollector(symbol)
    exchange = ccxtpro.binance({
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap',
            'adjustForTimeDifference': True
        }
    })
    
    try:
        print(f"{symbol} 1초봉 데이터 수집을 시작합니다...")
        last_save_time = None
        
        while True:
            try:
                # 실시간 거래 데이터 구독
                trades = await exchange.watch_trades(symbol)
                if not trades:
                    continue
                
                current_time = int(time.time())
                
                # 새로운 초가 시작되면 이전 캔들 저장
                if last_save_time is not None and current_time > last_save_time:
                    collector.save_candle()
                
                # 현재 거래로 캔들 업데이트
                for trade in trades:
                    price = float(trade['price'])
                    volume = float(trade['amount'])
                    timestamp = int(trade['timestamp'] / 1000)  # 밀리초를 초로 변환
                    
                    collector.update_candle(price, volume, timestamp)
                    last_save_time = timestamp
                
            except Exception as e:
                print(f"에러 발생: {str(e)}")
                await asyncio.sleep(1)
                continue
                
    except Exception as e:
        print(f"치명적 에러 발생: {str(e)}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    # 사용 예시
    symbol = 'MOODENG/USDT:USDT'  # 모니터링할 심볼
    asyncio.run(collect_ohlcv(symbol))
