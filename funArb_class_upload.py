import asyncio
import datetime
import ccxt.pro as ccxt
import time
import math

class ExchangeManager:

    API_KEY = ''
    PRIVATE_KEY = ''

    BITGET_API_KEY = ''
    BITGET_SECRET_KEY = ''
    BITGET_PASSWORD = ''

    BYBIT_API_KEY = ''
    BYBIT_SECRET_KEY = ''  

    BINANCE_API_KEY = ''
    BINANCE_SECRET_KEY = ''

    GATEIO_API_KEY = ''
    GATEIO_SECRET_KEY = ''

    EXCHANGE_KEYS = {
        "hyperliquid": {"walletAddress": API_KEY, "privateKey": PRIVATE_KEY},
        "bitget": {"apiKey": BITGET_API_KEY, "secret": BITGET_SECRET_KEY, "password": BITGET_PASSWORD},
        "bybit": {"apiKey": BYBIT_API_KEY, "secret": BYBIT_SECRET_KEY},
        "binance": {"apiKey": BINANCE_API_KEY, "secret": BINANCE_SECRET_KEY},
        "gateio": {"apiKey": GATEIO_API_KEY, "secret": GATEIO_SECRET_KEY}
    }

    def __init__(self, exchange_name):
        self.exchange_name = exchange_name
        self.exchange = None
        # API 키가 제대로 로드되었는지 확인
        if not hasattr(self, 'EXCHANGE_KEYS') or not self.EXCHANGE_KEYS:
            print("Warning: EXCHANGE_KEYS not properly initialized")
            self.EXCHANGE_KEYS = {
                'binance': {
                    'apiKey': 'your_api_key',
                    'secret': 'your_secret',
                    'enableRateLimit': True,
                    'options': {'defaultType': 'future'}
                },
                'bitget': {
                    'apiKey': 'your_api_key',
                    'secret': 'your_secret',
                    'password': 'your_password',  # Bitget requires password
                    'enableRateLimit': True
                }
            }

    async def create_exchange(self):
        try:
            print(f"Initializing {self.exchange_name} exchange...")
            if self.exchange_name not in self.EXCHANGE_KEYS:
                raise Exception(f"No API keys found for {self.exchange_name}")

            config = self.EXCHANGE_KEYS[self.exchange_name].copy()
            exchange_class = getattr(ccxt, self.exchange_name)
            
            if not exchange_class:
                raise Exception(f"Exchange {self.exchange_name} not found in ccxt")

            exchange = exchange_class(config)
            
            # Exchange specific configurations
            if self.exchange_name == "binance":
                exchange.options['defaultType'] = 'future'

            elif self.exchange_name == "bitget":
                exchange.options['defaultType'] = 'swap'

            # Test connection
            try:
                await exchange.load_markets()
                print(f"Successfully connected to {self.exchange_name}")
            except Exception as e:
                print(f"Error loading markets: {str(e)}")
                raise

            return exchange

        except Exception as e:
            print(f"Detailed error creating {self.exchange_name} exchange: {str(e)}")
            print(f"Exchange keys available: {list(self.EXCHANGE_KEYS.keys())}")
            return None

    async def close(self):
        """거래소 연결을 안전하게 종료"""
        if self.exchange is not None:
            try:
                await self.exchange.close()
                if hasattr(self.exchange, 'session') and self.exchange.session is not None:
                    await self.exchange.session.close()
                print(f"Closed {self.exchange_name} exchange connection")
            except Exception as e:
                print(f"Error closing exchange {self.exchange_name}: {e}")
            finally:
                self.exchange = None

    async def set_leverage(self, symbol, leverage):
        try:
            if self.exchange_name == "bybit":
                # 간단하게 CCXT 표준 메서드 사용
                await self.exchange.set_leverage(leverage, symbol)
                print(f"Set leverage to {leverage}x for {symbol} on {self.exchange_name}")
                
                # 로그 추가
                try:
                    formatted_symbol = symbol.replace('/', '').replace(':USDT', '')
                    positions = await self.exchange.fetch_positions([symbol])
                    if positions and len(positions) > 0:
                        current_leverage = positions[0]['leverage']
                        print(f"Verified {symbol} leverage on {self.exchange_name}: {current_leverage}x")
                except Exception as e:
                    print(f"Error fetching positions: {e}")
                
            elif self.exchange_name == "bitget":
                # Bitget에서는 격리 마진 모드 설정을 명시적으로 함
                try:
                    formatted_symbol = symbol.split(':')[0].replace('/', '')  # DOGE/USDT:USDT -> DOGEUSDT
                    
                    # 마진 모드를 isolated로 설정
                    await self.exchange.set_margin_mode('isolated', symbol)
                    print(f"Set margin mode to isolated for {symbol} on {self.exchange_name}")
                    
                    # 레버리지 설정
                    await self.exchange.set_leverage(leverage, symbol, {
                        'marginCoin': 'USDT',
                        'symbol': formatted_symbol,
                        'marginMode': 'isolated'
                    })
                    print(f"Set leverage to {leverage}x for {symbol} on {self.exchange_name} (isolated mode)")
                    
                except Exception as e:
                    print(f"Error setting margin mode or leverage on Bitget: {e}")
                    try:
                        # 대체 방식 시도
                        await self.exchange.set_leverage(leverage, symbol, {
                            'marginCoin': 'USDT',
                            'holdSide': 'long_short'
                        })
                        print(f"Set leverage to {leverage}x for {symbol} on {self.exchange_name} (alternative method)")
                    except Exception as e2:
                        print(f"Error in alternative leverage setup: {e2}")
                    
            else:
                # 다른 거래소들
                await self.exchange.set_leverage(leverage, symbol)
                print(f"Set leverage to {leverage}x for {symbol} on {self.exchange_name}")
            
        except Exception as e:
            print(f"Error setting leverage on {self.exchange_name}: {e}")

    async def get_best_price(self, symbol, direction):
        try:
            # 기존 연결이 있다면 확실하게 닫기
            if self.exchange is not None:
                await self.close()
                self.exchange = None

            # 새로운 연결 생성
            self.exchange = await self.create_exchange()
            if not self.exchange:
                print(f"Failed to create {self.exchange_name} exchange instance in get_best_price")
                return None

            try:
                orderbook = await self.exchange.fetch_order_book(symbol)
                if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                    price = orderbook['bids'][0][0] if direction == 'buy' else orderbook['asks'][0][0]
                    print(f"Got best {direction} price for {symbol}: {price}")
                    return price
                else:
                    print(f"Invalid orderbook data received: {orderbook}")
                    return None
            except Exception as e:
                print(f"Error fetching orderbook: {e}")
                return None
            finally:
                # 연결 종료
                await self.close()
                self.exchange = None
            
        except Exception as e:
            print(f"Error in get_best_price: {e}")
            return None

    async def execute_trade(self, direction, symbol, order_type, amount, leverage, price=None, custom_params=None):
        try:
            if self.exchange is not None:
                await self.close()
                self.exchange = None
            
            self.exchange = await self.create_exchange()
            if not self.exchange:
                return None
            
            print(f"Setting up trade for {self.exchange_name}...")
            await self.exchange.load_markets()
            
            # 레버리지 설정
            await self.set_leverage(symbol, leverage)
            
            # Bybit 주문 처리
            if self.exchange_name == "bybit":
                params = {
                    'category': 'linear',
                    'position_idx': 0,
                    'marginMode': 'isolated'
                }
                
                print(f"[BYBIT EXECUTE_TRADE DEBUG] Amount: {amount}")
                
                order = await self.exchange.create_order(
                    symbol=symbol,
                    type=order_type,
                    side=direction,
                    amount=amount,
                    price=price if order_type == "limit" else None,
                    params=params
                )
                return order
            
            # 거래소별 파라미터 설정
            if self.exchange_name == "bitget":
                formatted_symbol = symbol.split(':')[0].replace('/', '')
                params = {
                    'marginCoin': 'USDT',
                    'symbol': formatted_symbol,
                    'marginMode': 'isolated',  # 격리 마진 명시
                    'leverage': leverage,      # 레버리지 명시
                    'tdMode': 'isolated'       # 거래 모드도 격리로 설정
                }
            else:
                params = {}
            
            # 커스텀 파라미터가 있으면 병합
            if custom_params:
                params.update(custom_params)
            
            if price is None and self.exchange:
                orderbook = await self.exchange.fetch_order_book(symbol)
                if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                    price = orderbook['bids'][0][0] if direction == 'buy' else orderbook['asks'][0][0]
            
            print(f"Executing {direction} order on {self.exchange_name} for {symbol}")
            print(f"Amount: {amount}, Price: {price}, Leverage: {leverage}")
            
            order = await self.exchange.create_order(
                symbol=symbol,
                type=order_type,
                side=direction,
                amount=amount,
                price=price if order_type == "limit" else None,
                params=params
            )
            
            print(f"{self.exchange_name.capitalize()} order executed: {order}")
            return order
        
        except Exception as e:
            print(f"Detailed error in {self.exchange_name} trade execution: {str(e)}")
            return None
        finally:
            if self.exchange:
                await self.close()
                self.exchange = None

    async def force_close_market(self, symbol, amount, direction, leverage):
        print(f"[{self.exchange_name}] Force closing {symbol} with market order...")
        await self.execute_trade(direction, symbol, "market", amount, leverage)  # 레버리지 유지


class TradeExecutor:
    def __init__(self, trade_info):
        self.trade_info = trade_info
        self.initial_amount = trade_info[list(trade_info.keys())[0]][3]
        self.remaining_amount = self.initial_amount
        self.exchange_managers = {}
        self.processed_orders = set()

    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        await self.close_all_exchanges()

    async def close_all_exchanges(self):
        """모든 거래소 인스턴스를 안전하게 닫음"""
        for manager in self.exchange_managers.values():
            await manager.close()
        self.exchange_managers.clear()

    async def execute_and_wait_for_order(self, exchange_manager, direction, symbol, order_type, amount, leverage, price=None):
        try:
            # 주문 생성
            order = await exchange_manager.execute_trade(direction, symbol, order_type, amount, leverage, price)
            if not order:
                return None
            
            # 주문 상태 모니터링
            start_time = time.time()
            while True:
                await asyncio.sleep(2)
                try:
                    exchange = await exchange_manager.create_exchange()
                    if not exchange:
                        print("Failed to create exchange instance for order status check")
                        continue

                    try:
                        # 거래소별 주문 상태 확인 로직
                        if exchange_manager.exchange_name == "bitget":
                            # Bitget은 fetch_order만 지원
                            order_status = await exchange.fetch_order(order['id'], symbol)
                        else:
                            # 다른 거래소들은 fetch_open_order와 fetch_closed_order를 먼저 시도
                            try:
                                order_status = await exchange.fetch_open_order(order['id'], symbol)
                                if order_status is None:
                                    order_status = await exchange.fetch_closed_order(order['id'], symbol)
                            except Exception as e:
                                print(f"Error checking order status with fetch_open/closed_order: {e}")
                                order_status = await exchange.fetch_order(order['id'], symbol)

                        if order_status and order_status['status'] == 'closed':
                            # 이미 처리된 주문인지 확인
                            if order['id'] in self.processed_orders:
                                print(f"Order {order['id']} already processed, skipping")
                                break
                            
                            # 처리된 주문 목록에 추가
                            self.processed_orders.add(order['id'])
                            
                            filled_amount = order_status['filled']
                            print(f"Filled {filled_amount} units at {order_status['average']} average price.")
                            return filled_amount
                        
                        # 30초 타임아웃 체크
                        if time.time() - start_time > 10:
                            print("Order not filled within 10 seconds. Cancelling order...")
                            try:
                                await exchange.cancel_order(order['id'], symbol)
                                print("Order cancelled successfully")
                                await asyncio.sleep(3)
                            except Exception as cancel_error:
                                print(f"Error cancelling order: {cancel_error}")
                            return None

                    finally:
                        await exchange.close()
                        
                except Exception as e:
                    print(f"Error in order status check loop: {e}")
                    try:
                        exchange = await exchange_manager.create_exchange()
                        if exchange:
                            await exchange.cancel_order(order['id'], symbol)
                            print("Order cancelled after error")
                            await exchange.close()
                    except Exception as cancel_error:
                        print(f"Error cancelling order after status check error: {cancel_error}")
                    return None
                
        except Exception as e:
            print(f"Error in execute_and_wait_for_order: {e}")
            return None

    async def open_position(self):
        exchange1, params1 = list(self.trade_info.items())[0]
        exchange2, params2 = list(self.trade_info.items())[1]
        
        if exchange1 not in self.exchange_managers:
            self.exchange_managers[exchange1] = ExchangeManager(exchange1)
        if exchange2 not in self.exchange_managers:
            self.exchange_managers[exchange2] = ExchangeManager(exchange2)

        exchange_manager1 = self.exchange_managers[exchange1]
        exchange_manager2 = self.exchange_managers[exchange2]
        
        has_filled_order = False
        consecutive_failures = 0
        
        try:
            # 거래소 연결 생성
            exchange1_instance = await exchange_manager1.create_exchange()
            if not exchange1_instance:
                print("Failed to create first exchange instance")
                return has_filled_order

            # 레버리지 설정
            leverage = params1[5]
            try:
                await exchange1_instance.set_leverage(leverage, params1[1])
                print(f"Set leverage to {leverage}x for {params1[1]} on {exchange1}")
            except Exception as e:
                print(f"Error setting leverage: {e}")

            while self.remaining_amount > 0:
                try:
                    if consecutive_failures >= 3:
                        print("Too many consecutive failures. Waiting for 1 minute before retrying...")
                        await asyncio.sleep(30)
                        consecutive_failures = 0

                    # get_best_price 사용하여 가격 정보 가져오기
                    best_price = await exchange_manager1.get_best_price(params1[1], params1[0])
                    if best_price is None:
                        print("Failed to get best price")
                        consecutive_failures += 1
                        continue

                    # orderbook은 여전히 필요 (거래량 확인용)
                    orderbook = await exchange1_instance.fetch_order_book(params1[1])
                    best_volume = orderbook['bids'][0][1] if params1[0] == 'buy' else orderbook['asks'][0][1]

                    min_order_value = 10  
                    min_trade_amount = math.ceil(min_order_value / best_price)
                    trade_amount = max(min(self.remaining_amount, best_volume * 0.5), min_trade_amount)

                    print(f"\n[New Order Attempt]")
                    print("--------------------------------")
                    print(f"Symbol: {params1[1]}")
                    print(f"Direction: {params1[0]}")
                    print(f"Amount: {trade_amount}")
                    print(f"Price: {best_price}")
                    print(f"Total Value: ${trade_amount * best_price:.2f}")
                    print("--------------------------------")


                    if trade_amount <= 0:
                        print("Trade amount too small. Stopping.")
                        break

                    # 주문 생성 시 레버리지 포함
                    # 어느 거래소든 bitget이 포함되어 있으면 isolated로 설정
                    if exchange1 == "bitget" or exchange2 == "bitget":
                        params = {
                            "marginMode": "isolated",
                            "leverage": leverage,
                            "tdMode": "isolated",
                            "marginCoin": "USDT"
                        }
                    else:
                        params = {
                            "marginMode": "isolated",
                            "leverage": leverage,
                            "tdMode": "isolated"
                        }
                    
                    order = await exchange1_instance.create_order(
                        symbol=params1[1],
                        type=params1[2],
                        side=params1[0],
                        amount=trade_amount,
                        price=best_price,
                        params=params
                    )

                    if order:
                        # 주문 상태 모니터링
                        start_time = time.time()
                        while True:
                            await asyncio.sleep(2)
                            try:
                                # 거래소별 주문 상태 확인 로직
                                if exchange_manager1.exchange_name == "bitget":
                                    # Bitget은 fetch_order만 지원
                                    order_status = await exchange1_instance.fetch_order(order['id'], params1[1])
                                else:
                                    # 다른 거래소들은 fetch_open_order와 fetch_closed_order를 먼저 시도
                                    try:
                                        order_status = await exchange1_instance.fetch_open_order(order['id'], params1[1])
                                        if order_status is None:
                                            order_status = await exchange1_instance.fetch_closed_order(order['id'], params1[1])
                                    except Exception as e:
                                        print(f"Error checking order status with fetch_open/closed_order: {e}")
                                        order_status = await exchange1_instance.fetch_order(order['id'], params1[1])

                                if order_status and order_status['status'] == 'closed':
                                    order_id = f"{exchange1}_{order['id']}"
                                    if order_id in self.processed_orders:
                                        break
                                    
                                    self.processed_orders.add(order_id)
                                    
                                    filled_amount = order_status['filled']
                                    print(f"Filled {filled_amount} units at {order_status['average']} average price.")
                                    has_filled_order = True
                                    consecutive_failures = 0
                                    self.remaining_amount -= filled_amount
                                    
                                    # 두 번째 거래소 주문 실행
                                    if exchange2 == "bybit":
                                        await exchange_manager2.execute_trade(
                                            "sell" if params1[0] == "buy" else "buy",
                                            params2[0],
                                            "market",
                                            filled_amount,
                                            params2[3]
                                        )
                                    break

                                if time.time() - start_time > 30:
                                    print("Order not filled within 30 seconds. Cancelling order...")
                                    try:
                                        await exchange1_instance.cancel_order(order['id'], params1[1])
                                        print("Order cancelled successfully")
                                        consecutive_failures += 1
                                        await asyncio.sleep(5)
                                    except Exception as cancel_error:
                                        print(f"Error cancelling order: {cancel_error}")
                                    break

                            except Exception as e:
                                print(f"Error in order status check loop: {e}")
                                try:
                                    await exchange1_instance.cancel_order(order['id'], params1[1])
                                except Exception as cancel_error:
                                    print(f"Error cancelling order after status check error: {cancel_error}")
                                consecutive_failures += 1
                                break

                except Exception as e:
                    print(f"Error in open_position loop: {e}")
                    consecutive_failures += 1
                    await asyncio.sleep(5)

        except Exception as e:
            print(f"Error in open_position: {e}")
        finally:
            # 모든 작업이 끝난 후에만 연결 종료
            await exchange_manager1.close()
        
        return has_filled_order

    async def monitor_and_close_position(self, signal_checker):
        while True:
            signal_result = await signal_checker.check_signal()
            
            # signal이 False로 변경된 경우 포지션 종료 시작
            if signal_result is False:
                print("Signal turned False. Starting position close...")
                await self.close_position(signal_checker)  # signal_checker를 전달
                break
                
            await asyncio.sleep(60)  # 1분 대기

    async def close_position(self, signal_checker):
        exchange1, params1 = list(self.trade_info.items())[0]
        exchange2, params2 = list(self.trade_info.items())[1]
        
        if exchange1 not in self.exchange_managers:
            self.exchange_managers[exchange1] = ExchangeManager(exchange1)
        if exchange2 not in self.exchange_managers:
            self.exchange_managers[exchange2] = ExchangeManager(exchange2)

        exchange_manager1 = self.exchange_managers[exchange1]
        exchange_manager2 = self.exchange_managers[exchange2]

        close_direction = "sell" if params1[0] == "buy" else "buy"
        self.remaining_amount = self.initial_amount
        close_start_time = time.time()
        
        try:
            # 거래소 연결 생성
            exchange1_instance = await exchange_manager1.create_exchange()
            if not exchange1_instance:
                print("Failed to create exchange instance for position closing")
                return
            
            # 레버리지 설정
            leverage = params1[5]
            try:
                await exchange1_instance.set_leverage(leverage, params1[1])
                print(f"Set leverage to {leverage}x for {params1[1]} on {exchange1}")
            except Exception as e:
                print(f"Error setting leverage: {e}")

            while self.remaining_amount > 0:
                # Signal 상태 체크
                signal_result = await signal_checker.check_signal()
                if signal_result is None:  # Signal이 True로 변경된 경우
                    print("Signal changed to True. Stopping position close.")
                    break

                # 1시간 초과 체크
                if time.time() - close_start_time > 3600:
                    print("Position closing exceeded 1 hour. Force closing remaining amount with market order...")
                    # 마켓 주문 시에도 레버리지 명시적 포함
                    if exchange1 == "bitget" or exchange2 == "bitget":
                        params = {
                            "marginMode": "isolated",
                            "leverage": leverage,
                            "tdMode": "isolated",
                            "marginCoin": "USDT"
                        }
                    elif exchange1 == "bybit" or exchange2 == "bybit":
                        params = {
                            "marginMode": "isolated",
                            "leverage": leverage,
                            "tdMode": "isolated",
                            "buy_leverage": leverage,
                            "sell_leverage": leverage
                        }
                    else:
                        params = {
                            "marginMode": "isolated",
                            "leverage": leverage,
                            "tdMode": "isolated"
                        }
                    
                    order = await exchange1_instance.create_order(
                        symbol=params1[1],
                        type="market",
                        side=close_direction,
                        amount=self.remaining_amount,
                        params=params
                    )
                    
                    # 두 번째 거래소 주문
                    if exchange2 == "bitget":
                        second_params = {
                            "marginMode": "isolated",
                            "leverage": params2[3],
                            "tdMode": "isolated",
                            "marginCoin": "USDT"
                        }
                        await exchange_manager2.execute_trade(
                            params1[0],
                            params2[0],
                            "market",
                            self.remaining_amount,
                            params2[3],
                            params2[2],
                            second_params
                        )
                    else:
                        await exchange_manager2.execute_trade(
                            params1[0],
                            params2[0],
                            "market",
                            self.remaining_amount,
                            params2[3],
                            params2[2]
                        )
                    break

                best_price = await exchange_manager1.get_best_price(params1[1], close_direction)
                if best_price is None:
                    print("Failed to get best price for closing")
                    await asyncio.sleep(5)
                    continue
                
                orderbook = await exchange1_instance.fetch_order_book(params1[1])
                best_volume = orderbook['bids'][0][1] if close_direction == 'buy' else orderbook['asks'][0][1]
                
                min_order_value = 10
                min_trade_amount = math.ceil(min_order_value / best_price)
                trade_amount = max(min(self.remaining_amount, best_volume * 0.5), min_trade_amount)

                if trade_amount <= 0:
                    print("Trade amount too small. Stopping.")
                    break
                
                print(f"\n[Close Position Attempt]")
                print("--------------------------------")
                print(f"Symbol: {params1[1]}")
                print(f"Direction: {close_direction}")
                print(f"Amount: {trade_amount}")
                print(f"Price: {best_price}")
                print(f"Total Value: ${trade_amount * best_price:.2f}")
                print(f"Leverage: {leverage}")
                print("--------------------------------")

                # 레버리지 명시적 포함
                if exchange1 == "bitget" or exchange2 == "bitget":
                    params = {
                        "marginMode": "isolated",
                        "leverage": leverage,
                        "tdMode": "isolated",
                        "marginCoin": "USDT"
                    }
                elif exchange1 == "bybit" or exchange2 == "bybit":
                    params = {
                        "marginMode": "isolated",
                        "leverage": leverage,
                        "tdMode": "isolated",
                        "buy_leverage": leverage,
                        "sell_leverage": leverage
                    }
                else:
                    params = {
                        "marginMode": "isolated",
                        "leverage": leverage,
                        "tdMode": "isolated"
                    }
                
                order = await exchange1_instance.create_order(
                    symbol=params1[1],
                    type=params1[2],
                    side=close_direction,
                    amount=trade_amount,
                    price=best_price,
                    params=params
                )

                if order:
                    # 주문 상태 모니터링
                    start_time = time.time()
                    filled_amount = None
                    
                    while True:
                        await asyncio.sleep(2)
                        try:
                            # 거래소별 주문 상태 확인 로직
                            if exchange_manager1.exchange_name == "bitget":
                                # Bitget은 fetch_order만 지원
                                order_status = await exchange1_instance.fetch_order(order['id'], params1[1])
                            else:
                                # 다른 거래소들은 fetch_open_order와 fetch_closed_order를 먼저 시도
                                try:
                                    order_status = await exchange1_instance.fetch_open_order(order['id'], params1[1])
                                    if order_status is None:
                                        order_status = await exchange1_instance.fetch_closed_order(order['id'], params1[1])
                                except Exception as e:
                                    print(f"Error checking order status with fetch_open/closed_order: {e}")
                                    order_status = await exchange1_instance.fetch_order(order['id'], params1[1])

                            if order_status and order_status['status'] == 'closed':
                                # 이미 처리된 주문인지 확인
                                order_id = f"{exchange1}_{order['id']}"
                                if order_id in self.processed_orders:
                                    break
                                    
                                # 처리된 주문 목록에 추가
                                self.processed_orders.add(order_id)
                                
                                filled_amount = order_status['filled']
                                print(f"Filled {filled_amount} units at {order_status['average']} average price.")
                                self.remaining_amount -= filled_amount
                                
                                # 두 번째 거래소 주문 (bitget 확인 및 파라미터 설정)
                                if exchange2 == "bitget":
                                    second_params = {
                                        "marginMode": "isolated",
                                        "leverage": params2[3],
                                        "tdMode": "isolated",
                                        "marginCoin": "USDT"
                                    }
                                    second_order = await exchange_manager2.execute_trade(
                                        params1[0],
                                        params2[0],
                                        params2[1],
                                        filled_amount,
                                        params2[3],
                                        params2[2],
                                        second_params
                                    )
                                else:
                                    second_order = await exchange_manager2.execute_trade(
                                        params1[0],
                                        params2[0],
                                        params2[1],
                                        filled_amount,
                                        params2[3],
                                        params2[2]
                                    )
                                break

                            if time.time() - start_time > 30:
                                print("Order not filled within 30 seconds. Cancelling order...")
                                try:
                                    await exchange1_instance.cancel_order(order['id'], params1[1])
                                    print("Order cancelled successfully")
                                    await asyncio.sleep(5)
                                except Exception as cancel_error:
                                    print(f"Error cancelling order: {cancel_error}")
                                break

                        except Exception as e:
                            print(f"Error in close position order status check: {e}")
                            break
        
        except Exception as e:
            print(f"Error in close_position: {e}")
        finally:
            # 모든 작업이 끝난 후에만 연결 종료
            if exchange1_instance:
                await exchange_manager1.close()

class SignalChecker:
    def __init__(self):
        self.last_check_time = None
        self.last_signal = True  # Initialize last_signal to True

    async def check_signal(self):
        current_time = datetime.datetime.now()

        if self.last_check_time is None or (current_time - self.last_check_time).total_seconds() >= 60:
            import random
            signal = random.choice([True, False])
            print(f"Signal check at {current_time}: {signal}")
            self.last_check_time = current_time
            
            if self.last_signal is False and signal is True:
                print("Signal changed from False to True. Stopping position close.")
                return None  # Stop closing position if signal changes to True
            
            self.last_signal = signal
            return signal
            
        return self.last_signal  # Return the last signal if not time to check

async def main():
    try:
        # 기존 코드
        trade_info = {"bitget": ["buy", "DOGE/USDT:USDT", "limit", 0.0000000000001, None, 1], 
                      "bybit": ["DOGE/USDT:USDT", "market", None, 1]}

        async with TradeExecutor(trade_info) as executor:
            try:
                has_filled_order = await executor.open_position()

                if has_filled_order:
                    print("Some orders were filled, monitoring signal for close position...")    
                    signal_checker = SignalChecker()
                    await executor.monitor_and_close_position(signal_checker)
                else:
                    print("No orders were filled. Skipping close position.")
            except Exception as e:
                print(f"Error in main: {e}")
    finally:
        # 추가적인 정리 로직
        await cleanup_resources()

async def cleanup_resources():
    """추가적인 리소스 정리를 위한 함수"""
    try:
        # aiohttp 세션이 아직 열려있는지 확인
        import asyncio
        import aiohttp
        for task in asyncio.all_tasks():
            if not task.done() and task != asyncio.current_task():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        # 명시적으로 가비지 컬렉션 수행
        import gc
        gc.collect()
    except Exception as e:
        print(f"Error during cleanup: {e}")

if __name__ == '__main__':
    asyncio.run(main())