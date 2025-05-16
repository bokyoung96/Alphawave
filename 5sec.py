import asyncio
import ccxt.pro as ccxtpro
from datetime import datetime
import time
from collections import deque
import asyncio

async def monitor_returns(symbol: str, span: int = 5):
    """
    실시간 가격 모니터링 및 수익률 계산
    
    Args:
        symbol (str): 모니터링할 심볼 (예: 'MOODENG/USDT:USDT')
        span (int): 수익률 계산 기간(초), 기본값 5초
    """
    # 거래소 초기화 (선물 거래 설정)
    exchange = ccxtpro.binance({
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap',  # 선물 거래 설정
            'adjustForTimeDifference': True
        }
    })
    
    try:
        # 초기 가격 저장용 변수
        price_history = deque()  # 가격 히스토리
        
        # 웹소켓 구독 시작
        while True:
            try:
                # 웹소켓으로 실시간 거래 구독
                trades = await exchange.watch_trades(symbol)
                print(trades)
                if not trades:
                    continue
                    
                # 가장 최근 거래 가격 사용
                current_price = trades[-1]['price']
                if not current_price or current_price <= 0:
                    continue
                    
                current_time = datetime.now()
                
                # 가격과 시간 기록
                price_history.append((current_time, current_price))
                
                # span 초 이전 데이터 제거
                while price_history and (current_time - price_history[0][0]).total_seconds() > span:
                    price_history.popleft()
                
                # 첫 가격 기록
                if len(price_history) < 2:
                    print(f"초기 가격 설정: {current_price:.8f} USDT")
                    continue
                
                # span 초 수익률 계산 (가장 오래된 가격과 현재 가격 비교)
                oldest_price = price_history[0][1]
                if not oldest_price or oldest_price <= 0:
                    continue
                    
                returns = ((current_price - oldest_price) / oldest_price) * 100
                
                # 실시간 결과 출력
                print(f"\r[{current_time.strftime('%H:%M:%S')}] {symbol} | "
                      f"현재가: {current_price:.8f} | "
                      f"{span}초수익률: {returns:+.4f}%", end='', flush=True)
                
            except Exception as e:
                print(f"\n웹소켓 에러 발생: {str(e)}")
                await asyncio.sleep(1)  # 에러 발생시 1초 대기 후 재시도
                continue
            
    except Exception as e:
        print(f"\n에러 발생: {str(e)}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    symbol = 'MOODENG/USDT:USDT'
    span = 5  # 5초 수익률
    print(f"실시간 모니터링을 시작합니다... (심볼: {symbol}, 기간: {span}초)")
    asyncio.run(monitor_returns(symbol, span))
