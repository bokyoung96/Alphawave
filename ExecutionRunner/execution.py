import asyncio
import ccxt.pro
import time
import nest_asyncio
import asyncio

class UniversalOrderManager:
    def __init__(self, primary_exchange_name, primary_api_key, primary_api_secret, primary_password=None, secondary_exchange_name=None, secondary_api_key=None, secondary_api_secret=None, secondary_password=None):
        """
        주문 관리자 초기화
        :param primary_exchange_name: 주요 거래소 이름
        :param primary_api_key: 주요 거래소 API 키
        :param primary_api_secret: 주요 거래소 API 시크릿
        :param primary_password: 주요 거래소 API 비밀번호 (선택사항)
        :param secondary_exchange_name: 보조 거래소 이름 (선택사항)
        :param secondary_api_key: 보조 거래소 API 키 (선택사항)
        :param secondary_api_secret: 보조 거래소 API 시크릿 (선택사항)
        :param secondary_password: 보조 거래소 API 비밀번호 (선택사항)
        """
        self.primary_exchange_name = primary_exchange_name
        self.primary_api_key = primary_api_key
        self.primary_api_secret = primary_api_secret
        self.primary_password = primary_password
        self.secondary_exchange_name = secondary_exchange_name
        self.secondary_api_key = secondary_api_key
        self.secondary_api_secret = secondary_api_secret
        self.secondary_password = secondary_password

        self.primary_exchange = getattr(ccxt.pro, self.primary_exchange_name)({
            "apiKey": self.primary_api_key,
            "secret": self.primary_api_secret,
            "password": self.primary_password,
            "enableRateLimit": True,
        })

        self.secondary_exchange = None
        if self.secondary_exchange_name and self.secondary_api_key and self.secondary_api_secret:
            self.secondary_exchange = getattr(ccxt.pro, self.secondary_exchange_name)({
                "apiKey": self.secondary_api_key,
                "secret": self.secondary_api_secret,
                "password": self.secondary_password,
                "enableRateLimit": True,
            })

        self.active_orders = {"spot": [], "futures": []}  # 시장 유형별 활성 주문

        # 실시간 가격 저장 변수
        self.prices = {
            "primary": {},
            "secondary": {}
        }

    async def update_prices(self, symbols):
        """
        실시간으로 가격 업데이트
        :param symbols: 주요 및 보조 거래소 심볼 딕셔너리 {"primary": [심볼1, 심볼2], "secondary": [심볼1, 심볼2]}
        """
        while True:
            try:
                for symbol in symbols["primary"]:
                    primary_ticker = await self.primary_exchange.watch_ticker(symbol)
                    self.prices["primary"][symbol] = primary_ticker['last']
                    print(f"[가격 업데이트] 주요 거래소 {symbol} 가격: {self.prices['primary'][symbol]}")

                if self.secondary_exchange:
                    for symbol in symbols["secondary"]:
                        secondary_ticker = await self.secondary_exchange.watch_ticker(symbol)
                        self.prices["secondary"][symbol] = secondary_ticker['last']
                        print(f"[가격 업데이트] 보조 거래소 {symbol} 가격: {self.prices['secondary'][symbol]}")

                await asyncio.sleep(1)  # 가격 업데이트 주기
            except Exception as e:
                print(f"가격 업데이트 중 에러 발생: {e}")

    async def place_order(self, exchange, symbol, price, amount, market_type, side, order_type="limit"):
        """
        주문 생성 함수
        :param exchange: ccxt.pro 거래소 인스턴스
        :param symbol: 거래 심볼
        :param price: 지정가 주문 가격 (시장가 주문에서는 무시됨)
        :param amount: 거래 수량
        :param market_type: "spot" 또는 "futures"
        :param side: "buy" 또는 "sell"
        :param order_type: "limit" 또는 "market"
        :return: 주문 객체
        """
        try:
            if order_type == "limit":
                order_func = exchange.create_limit_buy_order if side == "buy" else exchange.create_limit_sell_order
            elif order_type == "market":
                order_func = exchange.create_market_buy_order if side == "buy" else exchange.create_market_sell_order
            else:
                raise ValueError("order_type은 'limit' 또는 'market'이어야 합니다.")

            order = await order_func(symbol, amount, price) if order_type == "limit" else await order_func(symbol, amount)
            print(f"[{market_type.upper()}] {side.upper()} {order_type.upper()} 주문 생성 완료: {order}")
            return order
        except Exception as e:
            print(f"주문 생성 중 에러 발생: {e}")
            return None

    async def monitor_orders(self, exchange, market_type):
        """
        활성 주문의 상태를 주기적으로 확인
        :param exchange: ccxt.pro 거래소 인스턴스
        :param market_type: "spot" 또는 "futures"
        """
        try:
            # 활성 주문 확인
            open_orders = await exchange.fetch_open_orders()
            print(f"[{market_type.upper()}] 활성 주문: {open_orders}")

            # 주문 상태 업데이트
            for order in self.active_orders[market_type]:
                # 활성 주문에서 해당 주문을 찾기
                matching_orders = [o for o in open_orders if o['id'] == order['id']]
                
                if matching_orders:
                    order_status = matching_orders[0]
                    print(f"[{market_type.upper()}] 주문 상태 (활성): {order_status['status']}")
                else:
                    # 주문이 더 이상 활성 상태가 아니면 체결 확인
                    closed_orders = await exchange.fetch_closed_orders(symbol=order['symbol'])
                    matching_closed_orders = [o for o in closed_orders if o['id'] == order['id']]
                    
                    if matching_closed_orders:
                        order_status = matching_closed_orders[0]
                        print(f"[{market_type.upper()}] 주문 상태 (완료): {order_status['status']}")
                        self.active_orders[market_type].remove(order)

        except Exception as e:
            print(f"주문 상태 확인 중 에러 발생: {e}")

    async def execute_twap(self, primary_config, secondary_config, total_amount, duration_minutes, interval_seconds):
        """
        TWAP 전략 실행 (실시간 가격 사용, default_price 제거)
        """
        try:
            num_orders = int((duration_minutes * 60) / interval_seconds)  # 주문 횟수
            order_amount = total_amount / num_orders  # 주문당 수량

            for i in range(num_orders):
                # 주요 거래소의 현재 호가 정보 가져오기
                orderbook_primary = await self.primary_exchange.fetch_order_book(primary_config['symbol'])
                if not orderbook_primary or not orderbook_primary['bids']:
                    print("[TWAP] 주요 거래소의 매수 1호가 정보를 가져올 수 없습니다.")
                    break

                # TWAP 매수 주문일 경우 매수 1호가(Best Bid), 매도 주문일 경우 매도 1호가(Best Ask)
                if primary_config['side'] == "buy":
                    primary_price = orderbook_primary['bids'][0][0]  # 매수 1호가
                elif primary_config['side'] == "sell":
                    primary_price = orderbook_primary['asks'][0][0]  # 매도 1호가
                else:
                    print("[TWAP] 유효하지 않은 주문 방향입니다.")
                    break

                print(f"[TWAP] 주요 거래소 {primary_config['side']} 주문 가격: {primary_price}")

                # 주요 거래소 주문 생성 (지정가)
                primary_order = await self.place_order(
                    self.primary_exchange,
                    primary_config['symbol'],
                    primary_price,  # 매수 1호가 또는 매도 1호가
                    order_amount,
                    primary_config['market_type'],
                    primary_config['side'],
                    "limit"  # 지정가 주문
                )

                if primary_order:
                    self.active_orders[primary_config['market_type']].append(primary_order)
                    await self.monitor_orders(self.primary_exchange, primary_config['market_type'])

                # 보조 거래소 주문 생성 (시장가)
                if self.secondary_exchange:
                    print(f"[TWAP] 보조 거래소 {secondary_config['side']} 시장가 주문 실행")
                    secondary_order = await self.place_order(
                        self.secondary_exchange,
                        secondary_config['symbol'],
                        None,  # 시장가는 가격 필요 없음
                        order_amount,
                        secondary_config['market_type'],
                        secondary_config['side'],
                        "market"  # 시장가 주문
                    )

                    if secondary_order:
                        self.active_orders[secondary_config['market_type']].append(secondary_order)
                        await self.monitor_orders(self.secondary_exchange, secondary_config['market_type'])

                await asyncio.sleep(interval_seconds)

            print(f"[TWAP] 총 {num_orders}개의 주문 완료.")
        except Exception as e:
            print(f"TWAP 실행 중 에러 발생: {e}")
nest_asyncio.apply()

if __name__ == "__main__":
    PRIMARY_EXCHANGE_NAME = "bybit"
    PRIMARY_API_KEY = "비밀"
    PRIMARY_API_SECRET = "비밀"
    PRIMARY_PASSWORD = None

    SECONDARY_EXCHANGE_NAME = "bitget"
    SECONDARY_API_KEY = "비밀"
    SECONDARY_API_SECRET = "비밀"
    SECONDARY_PASSWORD = "비밀"
    
    SYMBOL = "DOGE/USDT"
    TOTAL_AMOUNT = 500
    DURATION_MINUTES = 30
    INTERVAL_SECONDS = 60

    PRIMARY_CONFIG = {
        "symbol": SYMBOL,
        "market_type": "spot",
        "side": "buy",
        "order_type": "limit",
    }

    SECONDARY_CONFIG = {
        "symbol": "DOGE/USDT",
        "market_type": "futures",
        "side": "sell",
        "order_type": "limit",
    }

    manager = UniversalOrderManager(
        PRIMARY_EXCHANGE_NAME,
        PRIMARY_API_KEY,
        PRIMARY_API_SECRET,
        SECONDARY_EXCHANGE_NAME,
        SECONDARY_API_KEY,
        SECONDARY_API_SECRET
    )

    async def main():
        # 가격 업데이트 태스크 시작
        asyncio.create_task(manager.update_prices({"primary": [PRIMARY_CONFIG['symbol']], "secondary": [SECONDARY_CONFIG['symbol']]}))

        # TWAP 실행
        await manager.execute_twap(PRIMARY_CONFIG, SECONDARY_CONFIG, TOTAL_AMOUNT, DURATION_MINUTES, INTERVAL_SECONDS)

    # Jupyter에서 실행
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())