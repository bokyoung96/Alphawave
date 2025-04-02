import asyncio
import datetime
import ccxt.pro as ccxt
import time
import math
import gc

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
            if self.exchange_name == "hyperliquid":
                exchange.options['defaultType'] = 'swap'
                exchange.options['defaultMarginMode'] = 'isolated'
                
                # 모든 가능한 옵션에 isolated margin 설정
                print("[INIT] Configuring Hyperliquid with forced isolated margin...")
                exchange.options['createMarketOrder'] = {'marginMode': 'isolated', 'isolated': True}
                exchange.options['createLimitOrder'] = {'marginMode': 'isolated', 'isolated': True}
                exchange.options['createOrder'] = {'marginMode': 'isolated', 'isolated': True}
                
                # 추가 옵션 설정
                if hasattr(exchange, 'options'):
                    # 내부 deep 옵션 설정 (객체 깊은 곳까지 설정)
                    try:
                        # 모든 가능한 API 설정에 isolated 설정 주입
                        if 'api' not in exchange.options:
                            exchange.options['api'] = {}
                            
                        if 'trading' not in exchange.options['api']:
                            exchange.options['api']['trading'] = {}
                            
                        exchange.options['api']['trading']['marginMode'] = 'isolated'
                        exchange.options['api']['trading']['isIsolated'] = True
                        
                        # 거래 관련 설정
                        if 'trading' not in exchange.options:
                            exchange.options['trading'] = {}
                        
                        exchange.options['trading']['marginMode'] = 'isolated'
                        exchange.options['trading']['isIsolated'] = True
                        
                        print("[INIT] ✅ Deep options configuration completed")
                    except Exception as e:
                        print(f"[INIT] Warning: Deep options configuration failed: {e}")
                
                # 래퍼 메서드 설정 - 모든 주문에 isolated 마진을 강제
                original_create_order = exchange.create_order
                
                async def create_order_with_isolated(symbol, type, side, amount, price=None, params=None):
                    # 항상 isolated 마진 파라미터 추가
                    if params is None:
                        params = {}
                    
                    # CRITICAL: 모든 가능한 isolated margin 파라미터 추가
                    isolated_params = {
                        'marginMode': 'isolated',
                        'tdMode': 'isolated',
                        'positionType': 'isolated',
                        'isolated': True,
                        'crossMargin': False,
                        'mgnMode': 'isolated',   # 다른 API 표기법
                        'marginType': 'isolated',
                        'margin_mode': 'isolated'
                    }
                    
                    # 파라미터 병합
                    for key, value in isolated_params.items():
                        params[key] = value
                    
                    # 주문 직전 마진 모드 강제 설정
                    try:
                        # 이 단계에서는 반드시 isolated margin을 적용
                        print("[WRAPPER] 🔒 CRITICAL - Forcing isolated margin directly before order...")
                        
                        # 1. 마진 모드를 다시 명시적으로 설정 (2번 연속 시도)
                        leverage = params.get('leverage', 1)
                        
                        for attempt in range(2):
                            try:
                                print(f"[WRAPPER] Setting margin mode attempt #{attempt+1}...")
                                await exchange.set_margin_mode('isolated', symbol, {
                                    'leverage': leverage,
                                    'force': True,
                                    'forceIsolated': True
                                })
                                print(f"[WRAPPER] ✅ Margin mode set: attempt #{attempt+1}")
                                break
                            except Exception as e:
                                print(f"[WRAPPER] Margin mode setting failed: {e}")
                        
                        # 2. 레버리지 다시 설정
                        for attempt in range(2):
                            try:
                                print(f"[WRAPPER] Setting leverage attempt #{attempt+1}...")
                                await exchange.set_leverage(leverage, symbol, {
                                    'marginMode': 'isolated',
                                    'force': True
                                })
                                print(f"[WRAPPER] ✅ Leverage set: attempt #{attempt+1}")
                                break
                            except Exception as e:
                                print(f"[WRAPPER] Leverage setting failed: {e}")
                        
                        # # 3. 직접 API 호출 시도
                        # try:
                        #     symbol_raw = symbol.split(':')[0].replace('/', '')
                            
                        #     print("[WRAPPER] Attempting direct API call...")
                        #     await exchange.private_post_position_switchIsolatedMargin({
                        #         'symbol': symbol_raw,
                        #         'marginMode': 'isolated',
                        #         'leverage': leverage
                        #     })
                        #     print("[WRAPPER] ✅ Direct API call succeeded!")
                        # except Exception as e:
                        #     print(f"[WRAPPER] Direct API call failed (expected): {e}")
                        
                        # 4. 마진 모드 최종 확인
                        positions = await exchange.fetch_positions([symbol])
                        
                        if positions and len(positions) > 0:
                            position = positions[0]
                            margin_mode = position.get('marginMode') or position.get('marginType')
                            print(f"[WRAPPER] Current margin mode: {margin_mode}")
                            
                            if margin_mode and margin_mode.lower() != 'isolated':
                                print(f"[WRAPPER CRITICAL] ❌ Margin mode is not isolated! Current: {margin_mode}")
                                print("[WRAPPER] Attempting emergency margin mode change...")
                                await exchange.set_margin_mode('isolated', symbol, {
                                    'leverage': leverage,
                                    'force': True
                                })
                                
                                # 한번 더 확인
                                positions = await exchange.fetch_positions([symbol])
                                if positions and len(positions) > 0:
                                    position = positions[0]
                                    margin_mode = position.get('marginMode') or position.get('marginType')
                                    if margin_mode and margin_mode.lower() == 'isolated':
                                        print("[WRAPPER] ✅ Successfully set isolated margin in emergency")
                                    else:
                                        print(f"[WRAPPER] ⚠️ WARNING: Could not set isolated margin! Mode: {margin_mode}")
                        else:
                            print("[WRAPPER INFO] No position data - likely first trade, continuing with isolated margin")
                    except Exception as e:
                        print(f"[WRAPPER ERROR] Error during pre-order margin verification: {e}")
                    
                    print(f"[WRAPPER] Creating order with forced isolated margin: {params}")
                    
                    # 원래 주문 생성 실행
                    try:
                        result = await original_create_order(symbol, type, side, amount, price, params)
                        
                        # 주문 후 즉시 마진 모드 확인
                        try:
                            print("[WRAPPER] Verifying margin mode after order creation...")
                            await asyncio.sleep(2)  # 약간 지연
                            
                            positions = await exchange.fetch_positions([symbol])
                            if positions and len(positions) > 0:
                                position = positions[0]
                                margin_mode = position.get('marginMode') or position.get('marginType')
                                
                                print(f"[WRAPPER] Post-order margin mode: {margin_mode}")
                                
                                if margin_mode and margin_mode.lower() == 'isolated':
                                    print("[WRAPPER] ✅ CONFIRMED: Position using isolated margin")
                                else:
                                    print(f"[WRAPPER] ⚠️ WARNING: Position using {margin_mode} margin!")
                            else:
                                print("[WRAPPER] No position data after order")
                        except Exception as e:
                            print(f"[WRAPPER] Error checking post-order margin mode: {e}")
                        
                        return result
                    except Exception as e:
                        print(f"[WRAPPER ERROR] Order creation failed: {e}")
                        # 주문 실패 시 마진 모드 재확인
                        try:
                            print("[WRAPPER] Checking margin mode after failed order...")
                            positions = await exchange.fetch_positions([symbol])
                            if positions and len(positions) > 0:
                                position = positions[0]
                                margin_mode = position.get('marginMode') or position.get('marginType')
                                print(f"[WRAPPER] Final margin mode after error: {margin_mode}")
                        except Exception as check_err:
                            print(f"[WRAPPER] Error checking position after failure: {check_err}")
                        raise e
                
                # 원래 메서드를 래퍼로 대체
                exchange.create_order = create_order_with_isolated
                
                # 마진 모드를 강제로 설정
                try:
                    exchange.options['createMarketOrder'] = {'marginMode': 'isolated'}
                    exchange.options['createLimitOrder'] = {'marginMode': 'isolated'}
                    exchange.options['createOrder'] = {'marginMode': 'isolated'}
                except Exception as e:
                    print(f"Error setting option defaults: {e}")

            elif self.exchange_name == "binance":
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
            if self.exchange_name == "hyperliquid":
                print("\n[DEBUG] Setting Hyperliquid Initial Configuration")
                try:
                    # 마진 모드 설정 먼저 시도
                    margin_params = {
                        'marginMode': 'isolated',
                        'tdMode': 'isolated',
                        'positionType': 'isolated'
                    }
                    
                    print("[DEBUG] Attempting to set margin mode...")
                    await self.exchange.set_margin_mode('isolated', symbol, margin_params)
                    
                    # 추가 확인: marginMode 설정이 가능한 다른 메서드 시도
                    try:
                        print("[DEBUG] Attempting additional margin mode configuration...")
                        await self.exchange.private_post_set_margin_mode({
                            'symbol': symbol.split(':')[0].replace('/', ''),
                            'marginMode': 'isolated'
                        })
                    except Exception as e:
                        print(f"[DEBUG] Additional margin mode configuration not available: {e}")
                    
                    # 레버리지 설정
                    leverage_params = {
                        'marginMode': 'isolated',
                        'tdMode': 'isolated',
                        'positionType': 'isolated',
                        'leverage': leverage
                    }
                    
                    print("[DEBUG] Attempting to set leverage...")
                    await self.exchange.set_leverage(leverage, symbol, leverage_params)
                    
                    # 설정 확인
                    print("[DEBUG] Verifying settings...")
                    positions = await self.exchange.fetch_positions([symbol])
                    
                    is_isolated = False
                    if positions and len(positions) > 0:
                        position = positions[0]
                        margin_mode = position.get('marginMode') or position.get('marginType')
                        current_leverage = position.get('leverage')
                        
                        print("\n[DEBUG] Current Position Configuration:")
                        print(f"  - Margin Mode: {margin_mode}")
                        print(f"  - Leverage: {current_leverage}")
                        print(f"  - Position Type: {position.get('marginType', 'unknown')}")
                        
                        # Margin 모드 확인
                        if margin_mode and margin_mode.lower() == 'isolated':
                            is_isolated = True
                            print("[DEBUG] ✅ Isolated margin mode confirmed")
                        else:
                            print(f"[ERROR] ❌ Failed to set isolated margin mode. Current mode: {margin_mode}")
                            
                        # 레버리지 확인
                        if current_leverage == leverage:
                            print(f"[DEBUG] ✅ Leverage set correctly to {leverage}x")
                        else:
                            print(f"[ERROR] ❌ Failed to set leverage. Current: {current_leverage}x, Target: {leverage}x")
                    else:
                        print("[DEBUG] No position data available for verification")
                    
                except Exception as e:
                    print(f"[ERROR] Failed to set initial configuration: {str(e)}")

            elif self.exchange_name == "bybit":
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
            if self.exchange_name == "hyperliquid":
                print("\n[DEBUG] ===== HYPERLIQUID TRADE EXECUTION =====")
                
                # 새로운 연결 생성
                self.exchange = await self.create_exchange()
                if not self.exchange:
                    print("Failed to create Hyperliquid exchange instance")
                    return None

                try:
                    # 극단적인 방법: 직접 API 통신 시도
                    print("\n[CRITICAL] HYPERLIQUID DIRECT API APPROACH")
                    
                    try:
                        # 이 단계는 CRITICAL: 거래소 객체의 내부 접근을 통한 직접 요청
                        print("[DIRECT API] Attempting to directly set isolated margin via internal API...")
                        
                        # 1. 직접 API 요청 준비
                        symbol_raw = symbol.split(':')[0].replace('/', '')  # DOGE/USDC:USDC -> DOGEUSDC
                        
                        # 2. 여러 API 엔드포인트 시도
                        api_attempts = [
                            # Attempt 1: POST /api/v1/private/position/switch-isolated-margin
                            lambda: self.exchange.private_post_position_switchIsolatedMargin({
                                'symbol': symbol_raw,
                                'marginMode': 'isolated',
                                'leverage': leverage
                            }),
                            
                            # Attempt 2: POST /api/v1/private/switch-position-mode
                            lambda: self.exchange.private_post_switchPositionMode({
                                'symbol': symbol_raw,
                                'mode': 'isolated_margin'
                            }),
                            
                            # Attempt 3: Directly use internal API transport
                            lambda: self.exchange.privatePostSetPositionMode({
                                'symbol': symbol_raw,
                                'positionMode': 'isolated_margin',
                                'leverage': leverage
                            })
                        ]
                        
                        # 3. 모든 API 시도
                        for i, api_attempt in enumerate(api_attempts):
                            try:
                                print(f"[DIRECT API] Attempt #{i+1}...")
                                result = await api_attempt()
                                print(f"[DIRECT API] Success! Response: {result}")
                                break
                            except Exception as e:
                                print(f"[DIRECT API] Attempt #{i+1} failed: {e}")
                                
                    except Exception as e:
                        print(f"[DIRECT API] All attempts failed: {e}")
                        print("[DIRECT API] Proceeding with regular approach")
                    
                    # 심플한 접근 방식 - Hyperliquid에서 작동하는 것으로 확인된 방법만 사용
                    print("\n[CRITICAL] Setting isolated margin with simplified approach...")
                    
                    # 레버리지와 함께 마진 모드 직접 설정 - 이 방법이 작동하는 것으로 확인됨
                    try:
                        print(f"[DEBUG] Setting margin mode with leverage parameter...")
                        
                        # 5번 반복 시도
                        for attempt in range(5):
                            try:
                                print(f"[CRITICAL] Attempt #{attempt+1} to set margin mode...")
                                await self.exchange.set_margin_mode('isolated', symbol, {
                                    'leverage': leverage,
                                    'force': True,
                                    'forceIsolated': True
                                })
                                print(f"[SUCCESS] ✅ Attempt #{attempt+1}: Isolated margin mode set")
                                break
                            except Exception as e:
                                print(f"[WARNING] Attempt #{attempt+1} failed: {e}")
                                await asyncio.sleep(1)
                        
                        # 성공적으로 설정된 후 추가 레버리지 설정
                        print(f"[DEBUG] Setting leverage to {leverage}x (multiple attempts)...")
                        
                        # 레버리지도 5번 반복 시도
                        for attempt in range(5):
                            try:
                                print(f"[CRITICAL] Attempt #{attempt+1} to set leverage...")
                                await self.exchange.set_leverage(leverage, symbol, {
                                    'marginMode': 'isolated',
                                    'force': True
                                })
                                print(f"[SUCCESS] ✅ Attempt #{attempt+1}: Leverage set to {leverage}x")
                                break
                            except Exception as e:
                                print(f"[WARNING] Attempt #{attempt+1} failed: {e}")
                                await asyncio.sleep(1)
                        
                        # 최종 설정 확인
                        try:
                            print("[CRITICAL] Verifying final settings...")
                            positions = await self.exchange.fetch_positions([symbol])
                            
                            if positions and len(positions) > 0:
                                position = positions[0]
                                margin_mode = position.get('marginMode') or position.get('marginType')
                                current_leverage = position.get('leverage')
                                
                                print(f"[VERIFICATION] Position margin mode: {margin_mode}")
                                print(f"[VERIFICATION] Position leverage: {current_leverage}")
                                
                                if margin_mode and margin_mode.lower() == 'isolated':
                                    print("[VERIFICATION] ✅ ISOLATED MARGIN CONFIRMED")
                                else:
                                    print(f"[VERIFICATION] ⚠️ WARNING: Margin mode is {margin_mode}")
                            else:
                                print("[VERIFICATION] No position data found (normal for first trade)")
                        except Exception as e:
                            print(f"[VERIFICATION] Could not verify settings: {e}")
                            
                        print("[SUCCESS] ✅ Configuration completed successfully")
                    except Exception as e:
                        print(f"[ERROR] ❌ Failed to set margin mode and leverage: {e}")
                        print("[CRITICAL] ❌ Cannot proceed without proper margin configuration")
                        return None
                    
                    # 주문 파라미터 설정 - 모든 가능한 isolated 마진 파라미터 포함
                    order_params = {
                        'leverage': leverage,
                        'marginMode': 'isolated',
                        'tdMode': 'isolated',
                        'positionType': 'isolated',
                        'isolated': True,
                        'crossMargin': False,
                        'mgnMode': 'isolated',
                        'marginType': 'isolated',
                        'margin_mode': 'isolated'
                    }
                    
                    # market 주문일 경우 price가 필요하면 가져오기
                    if order_type == "market" and price is None:
                        try:
                            print("[DEBUG] Getting best price for market order...")
                            orderbook = await self.exchange.fetch_order_book(symbol)
                            if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                                best_price = orderbook['bids'][0][0] if direction == 'buy' else orderbook['asks'][0][0]
                                price = best_price
                                print(f"[DEBUG] Using best price for market order: {price}")
                                # 추가 슬리피지 옵션 설정
                                order_params['price'] = price
                                order_params['slippage'] = 0.05  # 5% 슬리피지 허용
                            else:
                                print("[ERROR] Failed to get orderbook for market order price")
                        except Exception as e:
                            print(f"[ERROR] Error getting best price for market order: {e}")
                    
                    print("\n[DEBUG] Creating order with enhanced parameters:")
                    print(f"  - Symbol: {symbol}")
                    print(f"  - Direction: {direction}")
                    print(f"  - Amount: {amount}")
                    print(f"  - Price: {price}")
                    print(f"  - Parameters: {order_params}")
                    
                    order = await self.exchange.create_order(
                        symbol=symbol,
                        type=order_type,
                        side=direction,
                        amount=amount,
                        price=price,  # market 주문에도 price 항상 전달
                        params=order_params
                    )
                    
                    print("\n[DEBUG] Order created successfully")
                    print(f"Order details: {order}")
                    
                    # 주문 생성 후 마진 모드 검증
                    try:
                        print("\n[CRITICAL] Post-order margin mode verification...")
                        await asyncio.sleep(2)  # 약간의 지연 추가
                        
                        positions = await self.exchange.fetch_positions([symbol])
                        if positions and len(positions) > 0:
                            position = positions[0]
                            margin_mode = position.get('marginMode') or position.get('marginType')
                            
                            print(f"[POST-ORDER] Current margin mode: {margin_mode}")
                            
                            if margin_mode and margin_mode.lower() == 'isolated':
                                print("[POST-ORDER] ✅ CONFIRMED: Position using isolated margin")
                            else:
                                print(f"[POST-ORDER] ⚠️ WARNING: Position using {margin_mode} margin")
                                print("[POST-ORDER] Please verify margin mode manually!")
                        else:
                            print("[POST-ORDER] No position data found after order")
                    except Exception as e:
                        print(f"[POST-ORDER] Error verifying margin mode: {e}")
                    
                    return order
                
                except Exception as e:
                    print(f"\n[ERROR] Trade execution failed: {str(e)}")
                    print("Full error details:", str(e))
                    return None
                finally:
                    # 연결 종료
                    await self.close()
                    self.exchange = None

            # Bybit 주문 처리
            elif self.exchange_name == "bybit":
                print("\n[DEBUG] ===== BYBIT TRADE EXECUTION =====")
                
                # 새로운 연결 생성 - 명시적으로 초기화
                self.exchange = await self.create_exchange()
                if not self.exchange:
                    print("[CRITICAL ERROR] Failed to create Bybit exchange instance")
                    return None

                try:
                    # Bybit 전용 파라미터 설정
                    bybit_params = {
                        'category': 'linear',
                        'position_idx': 0,
                        'marginMode': 'isolated',
                        'tdMode': 'isolated',
                        'leverage': leverage
                    }
                    
                    print(f"\n[DEBUG] Creating Bybit order with parameters:")
                    print(f"  - Symbol: {symbol}")
                    print(f"  - Direction: {direction}")
                    print(f"  - Amount: {amount}")
                    print(f"  - Price: {price}")
                    print(f"  - Parameters: {bybit_params}")
                    
                    # 새 연결에서 주문 생성
                    order = await self.exchange.create_order(
                        symbol=symbol,
                        type=order_type,
                        side=direction,
                        amount=amount,
                        price=price if order_type == "limit" else None,
                        params=bybit_params
                    )
                    
                    print("\n[DEBUG] Bybit order created successfully")
                    print(f"Order details: {order}")
                    
                    return order
                    
                except Exception as e:
                    print(f"\n[ERROR] Bybit trade execution failed: {str(e)}")
                    print("Full error details:", str(e))
                    return None
                finally:
                    # 연결 종료
                    await self.close()
                    self.exchange = None
            
            # 다른 거래소들 처리
            else:
                # bitget 거래소인 경우 exchange 객체 확인 및 생성
                if self.exchange_name == "bitget":
                    # 연결 확인 및 생성
                    if not self.exchange:
                        self.exchange = await self.create_exchange()
                        if not self.exchange:
                            print(f"[CRITICAL ERROR] Failed to create {self.exchange_name} exchange instance")
                            return None
                            
                    formatted_symbol = symbol.split(':')[0].replace('/', '')
                    params = {
                        'marginCoin': 'USDT',
                        'symbol': formatted_symbol,
                        'marginMode': 'isolated',
                        'leverage': leverage,
                        'tdMode': 'isolated'
                    }
                else:
                    params = {
                        'marginMode': 'isolated',
                        'leverage': leverage,
                        'tdMode': 'isolated'
                    }
                
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
        self.prevent_cross_margin = True  # 안전 모드 활성화

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
            # Hyperliquid의 경우 margin mode 설정을 보존
            if exchange_manager.exchange_name == "hyperliquid":
                print("\n[DEBUG] Setting up Hyperliquid margin mode before order...")
                exchange = await exchange_manager.create_exchange()
                if not exchange:
                    print("Failed to create Hyperliquid exchange instance")
                    return None

                try:
                    # Margin mode 설정 로직 보존
                    print("[DEBUG] Setting margin mode with leverage parameter...")
                    for attempt in range(5):
                        try:
                            print(f"[CRITICAL] Attempt #{attempt+1} to set margin mode...")
                            await exchange.set_margin_mode('isolated', symbol, {
                                'leverage': leverage,
                                'force': True,
                                'forceIsolated': True
                            })
                            print(f"[SUCCESS] ✅ Attempt #{attempt+1}: Isolated margin mode set")
                            break
                        except Exception as e:
                            print(f"[WARNING] Attempt #{attempt+1} failed: {e}")
                            await asyncio.sleep(1)

                    # 레버리지 설정
                    print(f"[DEBUG] Setting leverage to {leverage}x...")
                    for attempt in range(5):
                        try:
                            print(f"[CRITICAL] Attempt #{attempt+1} to set leverage...")
                            await exchange.set_leverage(leverage, symbol, {
                                'marginMode': 'isolated',
                                'force': True
                            })
                            print(f"[SUCCESS] ✅ Attempt #{attempt+1}: Leverage set to {leverage}x")
                            break
                        except Exception as e:
                            print(f"[WARNING] Attempt #{attempt+1} failed: {e}")
                            await asyncio.sleep(1)

                    # 설정 확인
                    positions = await exchange.fetch_positions([symbol])
                    if positions and len(positions) > 0:
                        position = positions[0]
                        margin_mode = position.get('marginMode') or position.get('marginType')
                        print(f"[VERIFICATION] Current margin mode: {margin_mode}")
                        if margin_mode and margin_mode.lower() != 'isolated':
                            print(f"[ERROR] Failed to set isolated margin mode. Current: {margin_mode}")
                            return None
                finally:
                    await exchange.close()

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
                        if exchange_manager.exchange_name == "hyperliquid":
                            # Hyperliquid는 fetch_order만 사용
                            order_status = await exchange.fetch_order(order['id'], symbol)
                        elif exchange_manager.exchange_name == "bitget":
                            # bitget은 fetch_open_order/fetch_closed_order를 지원하지 않으므로 fetch_order만 사용
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
                            if order['id'] in self.processed_orders:
                                print(f"Order {order['id']} already processed, skipping")
                                return None
                            
                            self.processed_orders.add(order['id'])
                            filled_amount = order_status['filled']
                            print(f"Filled {filled_amount} units at {order_status['average']} average price.")
                            return filled_amount
                        
                            # 10초 타임아웃 체크 (30초에서 변경)
                            if time.time() - start_time > 10:
                                print("Order not filled within 10 seconds. Cancelling order...")
                                try:
                                    await exchange.cancel_order(order['id'], symbol)
                                    print("Order cancelled successfully. Will retry with a new order...")
                                    break  # 루프를 종료하고 새 주문 생성 시도
                                except Exception as cancel_error:
                                    print(f"Error cancelling order: {cancel_error}")
                                    break  # 에러 발생 시에도 루프 종료하고 새 주문 시도

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

                    # Hyperliquid에서는 단순화된 마진 설정 접근법 사용
                    if exchange1 == "hyperliquid":
                        try:
                            print("\n[DEBUG] Setting up isolated margin for Hyperliquid...")
                            # 마진 모드 명시적 설정 - 이 방법이 작동하는 것으로 확인됨
                            try:
                                print("[DEBUG] Setting margin mode with leverage parameter...")
                                await exchange1_instance.set_margin_mode('isolated', params1[1], {'leverage': leverage})
                                print("[SUCCESS] ✅ Isolated margin mode set with leverage parameter")
                                
                                print(f"[DEBUG] Setting leverage to {leverage}x...")
                                await exchange1_instance.set_leverage(leverage, params1[1])
                                print(f"[SUCCESS] ✅ Leverage set to {leverage}x")
                                
                                # 설정 성공 - 추가 검증 없이 진행
                                print("[INFO] Proceeding with order - margin settings applied")
                            except Exception as e:
                                print(f"[ERROR] Failed to set margin mode: {e}")
                                if self.prevent_cross_margin:
                                    print("[CRITICAL] Cannot proceed without confirmed margin settings")
                                    return False
                        except Exception as e:
                            print(f"[ERROR] Error during Hyperliquid margin setup: {e}")
                            if self.prevent_cross_margin:
                                print("[ERROR] Aborting due to margin setup error")
                                return False
                    
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
                        order_filled = False
                        
                        while True:
                            await asyncio.sleep(2)
                            try:
                                # 거래소별 주문 상태 확인 로직
                                if exchange_manager1.exchange_name == "hyperliquid":
                                    # Hyperliquid는 fetch_order만 사용
                                    order_status = await exchange1_instance.fetch_order(order['id'], params1[1])
                                elif exchange_manager1.exchange_name == "bitget":
                                    # bitget은 fetch_open_order/fetch_closed_order를 지원하지 않으므로 fetch_order만 사용
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
                                
                                # 주문이 체결된 경우
                                if order_status and order_status['status'] == 'closed':
                                    order_id = f"{exchange1}_{order['id']}"
                                    if order_id in self.processed_orders:
                                        break
                                    
                                    self.processed_orders.add(order_id)
                                    
                                    filled_amount = order_status['filled']
                                    avg_price = order_status['average'] or order_status['price'] or best_price
                                    print(f"Filled {filled_amount} units at {avg_price} average price.")
                                    
                                    # 성공적으로 체결된 양만큼 감소
                                    if filled_amount > 0:
                                        self.remaining_amount -= filled_amount
                                        order_filled = True
                                        has_filled_order = True  # has_filled_order 플래그 설정
                                        consecutive_failures = 0  # 실패 카운터 리셋
                                    
                                    # 포지션 업데이트 확인
                                    if exchange1 == "hyperliquid":
                                        try:
                                            positions = await exchange1_instance.fetch_positions([params1[1]])
                                            if positions and len(positions) > 0:
                                                position = positions[0]
                                                position_size = float(position['contracts'])
                                                print(f"Current position size after fill: {position_size} units")
                                                if position_size < self.remaining_amount:
                                                    self.remaining_amount = position_size  # 실제 포지션 크기로 업데이트
                                            else:
                                                print("Position fully closed")
                                                self.remaining_amount = 0
                                        except Exception as e:
                                            print(f"Error checking position status: {e}")
                                    
                                    # 두 번째 거래소 주문
                                    if exchange2 == "bybit":
                                        print("\n[DEBUG] Executing bybit side of the order...")
                                        # 새로운 Bybit 거래소 객체 생성 및 연결 확인
                                        bybit_manager = ExchangeManager("bybit")
                                        bybit_instance = await bybit_manager.create_exchange()
                                        
                                        if not bybit_instance:
                                            print("[CRITICAL ERROR] Failed to create Bybit instance for second order")
                                        else:
                                            try:
                                                # Bybit 주문 파라미터
                                                bybit_params = {
                                                    'category': 'linear',
                                                    'position_idx': 0,
                                                    'marginMode': 'isolated',
                                                    'tdMode': 'isolated',
                                                    'leverage': params2[3]
                                                }
                                                
                                                print(f"\n[DEBUG] Creating Bybit order:")
                                                print(f"  - Symbol: {params2[0]}")
                                                print(f"  - Direction: {'sell' if params1[0] == 'buy' else 'buy'}")
                                                print(f"  - Amount: {filled_amount}")
                                                print(f"  - Type: market")
                                                print(f"  - Parameters: {bybit_params}")
                                                
                                                bybit_order = await bybit_instance.create_order(
                                                    symbol=params2[0],
                                                    type="market",
                                                    side="sell" if params1[0] == "buy" else "buy",
                                                    amount=filled_amount,
                                                    params=bybit_params
                                                )
                                                
                                                print(f"\n[DEBUG] Bybit order result: {bybit_order}")
                                            except Exception as e:
                                                print(f"[ERROR] Bybit order execution failed: {e}")
                                            finally:
                                                # 연결 종료
                                                await bybit_manager.close()
                                    elif exchange2 == "bitget":
                                        print("\n[DEBUG] Executing bitget side of the order...")
                                        
                                        # bitget 거래소 인스턴스 확인 및 초기화
                                        if not hasattr(exchange_manager2, 'exchange') or exchange_manager2.exchange is None:
                                            exchange_manager2.exchange = await exchange_manager2.create_exchange()
                                            if not exchange_manager2.exchange:
                                                print(f"[CRITICAL ERROR] Failed to create bitget exchange instance")
                                                break
                                        
                                        # Bitget 거래소 파라미터
                                        bitget_params = {
                                            "marginMode": "isolated",
                                            "leverage": params2[3],
                                            "tdMode": "isolated",
                                            "marginCoin": "USDT"
                                        }
                                        
                                        print(f"\n[DEBUG] Creating Bitget order:")
                                        print(f"  - Symbol: {params2[0]}")
                                        print(f"  - Direction: {'sell' if params1[0] == 'buy' else 'buy'}")
                                        print(f"  - Amount: {filled_amount}")
                                        print(f"  - Type: market")
                                        print(f"  - Parameters: {bitget_params}")
                                        
                                        await exchange_manager2.execute_trade(
                                            "sell" if params1[0] == "buy" else "buy",
                                            params2[0],
                                            "market",
                                            filled_amount,
                                            params2[3],
                                            None,
                                            bitget_params
                                        )
                                    else:
                                        # 다른 거래소 처리
                                        await exchange_manager2.execute_trade(
                                            "sell" if params1[0] == "buy" else "buy",
                                            params2[0],
                                            "market",
                                            filled_amount,
                                            params2[3]
                                        )
                                    break
                                
                                # 10초 타임아웃 체크
                                if time.time() - start_time > 10:
                                    print("Order not filled within 10 seconds. Cancelling order...")
                                    try:
                                        await exchange1_instance.cancel_order(order['id'], params1[1])
                                        print("Order cancelled. Retrying with new price...")
                                        break
                                    except Exception as e:
                                        print(f"Error cancelling order: {e}")
                                        break
                            
                            except Exception as e:
                                print(f"Error in order status check loop: {e}")
                                try:
                                    await exchange1_instance.cancel_order(order['id'], params1[1])
                                    print("Order cancelled after error")
                                except Exception as cancel_error:
                                    print(f"Error cancelling order after error: {cancel_error}")
                                break
                        
                        # 주문이 체결되었을 경우에만 다음 루프 건너뛰기
                        if order_filled:
                            continue
                
                except Exception as e:
                    print(f"Error in open_position loop: {e}")
                    consecutive_failures += 1
                    await asyncio.sleep(5)

        except Exception as e:
            print(f"Error in open_position: {e}")
        finally:
            # 모든 작업이 끝난 후에만 연결 종료
            if 'exchange1_instance' in locals() and exchange1_instance:
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
        # 거래소 정보 추출
        exchange1, params1 = list(self.trade_info.items())[0]
        exchange2, params2 = list(self.trade_info.items())[1]
        
        # 거래소 매니저 생성
        if exchange1 not in self.exchange_managers:
            self.exchange_managers[exchange1] = ExchangeManager(exchange1)
        if exchange2 not in self.exchange_managers:
            self.exchange_managers[exchange2] = ExchangeManager(exchange2)

        exchange_manager1 = self.exchange_managers[exchange1]
        exchange_manager2 = self.exchange_managers[exchange2]

        # 클로즈 방향은 오픈 방향의 반대 (기본값)
        close_direction = "sell" if params1[0] == "buy" else "buy"
        # 두 번째 거래소의 클로즈 방향은 첫 번째와 동일 (이미 반대로 열려있음)
        second_exchange_direction = params1[0]  # 두 번째 거래소에서는 원래 방향과 동일하게 종료
        
        self.remaining_amount = self.initial_amount
        close_start_time = time.time()
        max_retry_count = 5  # 최대 재시도 횟수
        retry_count = 0
        
        try:
            # 거래소 연결 생성
            exchange1_instance = await exchange_manager1.create_exchange()
            if not exchange1_instance:
                print("Failed to create exchange instance for position closing")
                return
            
            # 레버리지 설정 (hyperliquid의 경우 오픈 포지션이 있으면 변경하지 않음)
            leverage = params1[5]
            has_open_position = False
            
            if exchange1 == "hyperliquid":
                # 포지션 정보 먼저 확인
                try:
                    positions = await exchange1_instance.fetch_positions([params1[1]])
                    has_open_position = positions and len(positions) > 0
                    
                    if has_open_position:
                        position = positions[0]
                        print(f"Open position exists for {params1[1]}, leverage: {position.get('leverage')}")
                        
                        # 현재 포지션 방향 확인 (buy/sell 또는 long/short)
                        position_side = position.get('side')
                        print(f"Current position side: {position_side}")
                        
                        # 포지션 방향에 기반하여 close_direction 설정
                        if position_side:
                            # 매우 중요: 포지션을 닫으려면 정확히 반대 방향으로 주문을 생성해야 함
                            if position_side.lower() in ['long', 'buy']:
                                close_direction = "sell"  # long 포지션은 sell로 닫음
                            elif position_side.lower() in ['short', 'sell']:
                                close_direction = "buy"   # short 포지션은 buy로 닫음
                            print(f"Setting close direction to {close_direction} (opposite of {position_side})")
                        
                        # 포지션 사이즈 확인 및 remaining_amount 업데이트
                        position_size = float(position['contracts'])
                        self.remaining_amount = position_size
                        print(f"Current position size: {position_size} units")
                    else:
                        print("No open position found to close")
                        self.remaining_amount = 0
                        return
                except Exception as e:
                    print(f"Error checking positions: {e}")
            else:
                # 다른 거래소는 기존 로직 유지
                try:
                    await exchange1_instance.set_leverage(leverage, params1[1])
                    print(f"Set leverage to {leverage}x for {params1[1]} on {exchange1}")
                except Exception as e:
                    print(f"Error setting leverage: {e}")

            # 남은 금액이 있거나 최대 재시도 횟수에 도달하지 않은 경우 계속 거래
            while self.remaining_amount > 0 and retry_count < max_retry_count:
                # Signal 상태 체크
                signal_result = await signal_checker.check_signal()
                if signal_result is None:  # Signal이 True로 변경된 경우
                    print("Signal changed to True. Stopping position close.")
                    break

                # 1시간 초과 체크
                if time.time() - close_start_time > 3600:
                    print("Position closing exceeded 1 hour. Force closing remaining amount with market order...")
                    
                    # 마켓 주문으로 강제 종료 시도
                    try:
                        # hyperliquid 파라미터
                        if exchange1 == "hyperliquid":
                            params = {
                                "marginMode": "isolated",
                                "tdMode": "isolated",
                                "positionType": "isolated",
                                "leverage": leverage,
                                "isolated": True,
                                "crossMargin": False,
                                "mgnMode": "isolated",
                                "marginType": "isolated",
                                "margin_mode": "isolated",
                                "reduceOnly": True  # 포지션 종료 전용
                            }
                        else:
                            params = {
                                "marginMode": "isolated",
                                "leverage": leverage,
                                "tdMode": "isolated",
                                "reduceOnly": True
                            }
                        
                        # 남은 포지션 사이즈 다시 확인
                        try:
                            if exchange1 == "hyperliquid":
                                positions = await exchange1_instance.fetch_positions([params1[1]])
                                if positions and len(positions) > 0:
                                    position = positions[0]
                                    position_size = float(position['contracts'])
                                    self.remaining_amount = position_size
                                    print(f"Updated position size before force close: {position_size} units")
                                    
                                    # 포지션 방향 확인
                                    position_side = position.get('side')
                                    if position_side:
                                        if position_side.lower() in ['long', 'buy']:
                                            close_direction = "sell"
                                        elif position_side.lower() in ['short', 'sell']:
                                            close_direction = "buy"
                                        print(f"Force close direction: {close_direction} (opposite of {position_side})")
                        except Exception as e:
                            print(f"Error updating position size: {e}")
                        
                        # 마켓 주문으로 강제 종료
                        print(f"Force closing with {close_direction} order, amount: {self.remaining_amount}")
                        order = await exchange1_instance.create_order(
                            symbol=params1[1],
                            type="market",
                            side=close_direction,
                            amount=self.remaining_amount,
                            params=params
                        )
                        
                        print(f"Force closed {self.remaining_amount} units with market order")
                        
                        # 두 번째 거래소 주문
                        await exchange_manager2.execute_trade(
                            second_exchange_direction,  # 원래 방향으로 종료
                            params2[0],
                            "market",
                            self.remaining_amount,
                            params2[3]
                        )
                    except Exception as e:
                        print(f"Error during force closing: {e}")
                        retry_count += 1
                        await asyncio.sleep(2)
                        continue
                    
                    break

                # 현재 최적 가격 가져오기
                best_price = await exchange_manager1.get_best_price(params1[1], close_direction)
                if best_price is None:
                    print("Failed to get best price for closing")
                    retry_count += 1
                    await asyncio.sleep(5)
                    continue
                
                # 주문량 계산
                orderbook = await exchange1_instance.fetch_order_book(params1[1])
                best_volume = orderbook['bids'][0][1] if close_direction == 'buy' else orderbook['asks'][0][1]
                
                min_order_value = 10
                min_trade_amount = math.ceil(min_order_value / best_price)
                trade_amount = max(min(self.remaining_amount, best_volume * 0.5), min_trade_amount)

                if trade_amount <= 0:
                    print("Trade amount too small. Stopping.")
                    break
                
                print(f"\n[Close Position Attempt #{retry_count+1}]")
                print("--------------------------------")
                print(f"Symbol: {params1[1]}")
                print(f"Direction: {close_direction}")
                print(f"Amount: {trade_amount}")
                print(f"Price: {best_price}")
                print(f"Total Value: ${trade_amount * best_price:.2f}")
                print(f"Remaining: {self.remaining_amount} units")
                print("--------------------------------")

                # Margin 모드 확인 (Hyperliquid의 경우) - 간소화된 로직
                if exchange1 == "hyperliquid":
                    try:
                        # 포지션 정보 확인
                        positions = await exchange1_instance.fetch_positions([params1[1]])
                        
                        if positions and len(positions) > 0:
                            position = positions[0]
                            margin_mode = position.get('marginMode') or position.get('marginType')
                            
                            # 포지션 방향 확인 및 close_direction 업데이트
                            position_side = position.get('side')
                            if position_side:
                                if position_side.lower() in ['long', 'buy']:
                                    correct_close_direction = "sell"
                                elif position_side.lower() in ['short', 'sell']:
                                    correct_close_direction = "buy"
                                
                                if close_direction != correct_close_direction:
                                    print(f"Correcting close direction from {close_direction} to {correct_close_direction}")
                                    close_direction = correct_close_direction
                            
                            if margin_mode and margin_mode.lower() != 'isolated':
                                print(f"Warning: Position is not using isolated margin. Current: {margin_mode}")
                            else:
                                print(f"Confirmed position is using isolated margin")
                    except Exception as e:
                        print(f"Error checking margin mode: {e}")

                # 레버리지 명시적 포함
                if exchange1 == "hyperliquid":
                    params = {
                        "marginMode": "isolated",
                        "leverage": leverage,
                        "tdMode": "isolated",
                        "positionType": "isolated",
                        "isolated": True,
                        "crossMargin": False,
                        "mgnMode": "isolated",
                        "marginType": "isolated",
                        "margin_mode": "isolated",
                        "reduceOnly": True  # 포지션 종료 전용
                    }
                elif exchange1 == "bitget" or exchange2 == "bitget":
                    params = {
                        "marginMode": "isolated",
                        "leverage": leverage,
                        "tdMode": "isolated",
                        "marginCoin": "USDT",
                        "reduceOnly": True
                    }
                elif exchange1 == "bybit" or exchange2 == "bybit":
                    params = {
                        "marginMode": "isolated",
                        "leverage": leverage,
                        "tdMode": "isolated",
                        "buy_leverage": leverage,
                        "sell_leverage": leverage,
                        "reduceOnly": True
                    }
                else:
                    params = {
                        "marginMode": "isolated",
                        "leverage": leverage,
                        "tdMode": "isolated",
                        "reduceOnly": True
                    }
                
                # 주문 생성 및 처리
                try:
                    print(f"Creating {close_direction} order to close position")
                    order = await exchange1_instance.create_order(
                        symbol=params1[1],
                        type=params1[2],
                        side=close_direction,
                        amount=trade_amount,
                        price=best_price,
                        params=params
                    )
                    
                    # 주문 모니터링
                    if order:
                        start_time = time.time()
                        order_filled = False
                        
                        # 주문 상태 모니터링 루프
                        while True:
                            await asyncio.sleep(2)
                            try:
                                # 주문 상태 확인
                                if exchange1 == "hyperliquid":
                                    order_status = await exchange1_instance.fetch_order(order['id'], params1[1])
                                elif exchange1 == "bitget":
                                    # bitget은 fetch_open_order/fetch_closed_order를 지원하지 않으므로 fetch_order만 사용
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
                                
                                # 주문이 체결된 경우
                                if order_status and order_status['status'] == 'closed':
                                    order_id = f"{exchange1}_{order['id']}"
                                    if order_id in self.processed_orders:
                                        break
                                    
                                    self.processed_orders.add(order_id)
                                    
                                    filled_amount = order_status['filled']
                                    avg_price = order_status['average'] or order_status['price'] or best_price
                                    print(f"Filled {filled_amount} units at {avg_price} average price.")
                                    
                                    # 성공적으로 체결된 양만큼 감소
                                    if filled_amount > 0:
                                        self.remaining_amount -= filled_amount
                                        order_filled = True
                                        has_filled_order = True  # has_filled_order 플래그 설정
                                        consecutive_failures = 0  # 실패 카운터 리셋
                                    
                                    # 포지션 업데이트 확인
                                    if exchange1 == "hyperliquid":
                                        try:
                                            positions = await exchange1_instance.fetch_positions([params1[1]])
                                            if positions and len(positions) > 0:
                                                position = positions[0]
                                                position_size = float(position['contracts'])
                                                print(f"Current position size after fill: {position_size} units")
                                                if position_size < self.remaining_amount:
                                                    self.remaining_amount = position_size  # 실제 포지션 크기로 업데이트
                                            else:
                                                print("Position fully closed")
                                                self.remaining_amount = 0
                                        except Exception as e:
                                            print(f"Error checking position status: {e}")
                                    
                                    # 두 번째 거래소 주문
                                    if exchange2 == "bybit":
                                        print("\n[DEBUG] Executing bybit side of the order...")
                                        bybit_manager = ExchangeManager("bybit")
                                        bybit_instance = await bybit_manager.create_exchange()
                                        
                                        if bybit_instance:
                                            try:
                                                bybit_params = {
                                                    'category': 'linear',
                                                    'position_idx': 0,
                                                    'marginMode': 'isolated',
                                                    'tdMode': 'isolated',
                                                    'leverage': params2[3]
                                                }
                                                
                                                bybit_order = await bybit_instance.create_order(
                                                    symbol=params2[0],
                                                    type=params2[1],
                                                    side=second_exchange_direction,  # 두 번째 거래소는 원래 방향으로 종료
                                                    amount=filled_amount,
                                                    price=params2[2] if params2[1] == "limit" else None,
                                                    params=bybit_params
                                                )
                                                print(f"Bybit close order executed: {bybit_order}")
                                            except Exception as e:
                                                print(f"Error executing Bybit order: {e}")
                                            finally:
                                                # 연결 종료
                                                await bybit_manager.close()
                                    elif exchange2 == "bitget":
                                        second_params = {
                                            "marginMode": "isolated",
                                            "leverage": params2[3],
                                            "tdMode": "isolated",
                                            "marginCoin": "USDT"
                                        }
                                        await exchange_manager2.execute_trade(
                                            second_exchange_direction,  # 두 번째 거래소는 원래 방향으로 종료
                                            params2[0],
                                            params2[1],
                                            filled_amount,
                                            params2[3],
                                            params2[2],
                                            second_params
                                        )
                                    else:
                                        await exchange_manager2.execute_trade(
                                            second_exchange_direction,  # 두 번째 거래소는 원래 방향으로 종료
                                            params2[0],
                                            params2[1],
                                            filled_amount,
                                            params2[3],
                                            params2[2]
                                        )
                                    break
                                
                                # 10초 타임아웃 체크
                                if time.time() - start_time > 10:
                                    print("Order not filled within 10 seconds. Cancelling order...")
                                    try:
                                        await exchange1_instance.cancel_order(order['id'], params1[1])
                                        print("Order cancelled. Retrying with new price...")
                                        break
                                    except Exception as e:
                                        print(f"Error cancelling order: {e}")
                                        break
                            
                            except Exception as e:
                                print(f"Error in order status check: {e}")
                                try:
                                    await exchange1_instance.cancel_order(order['id'], params1[1])
                                    print("Order cancelled after error")
                                except Exception as cancel_error:
                                    print(f"Error cancelling order after error: {cancel_error}")
                                break
                        
                        # 체결된 경우 계속 진행
                        if order_filled:
                            retry_count = 0  # 성공적으로 체결되면 재시도 카운터 리셋
                            continue
                        else:
                            retry_count += 1
                
                except Exception as e:
                    print(f"Error creating order: {e}")
                    retry_count += 1
                    await asyncio.sleep(5)
            
            # 루프 종료 후 포지션 상태 확인
            if self.remaining_amount > 0:
                print(f"Warning: Could not close entire position. Remaining: {self.remaining_amount} units.")
            else:
                print("Successfully closed entire position.")
        
        except Exception as e:
            print(f"Error in close_position: {e}")
        finally:
            # 모든 작업이 완료된 후 반드시 연결 종료
            if 'exchange1_instance' in locals() and exchange1_instance:
                await exchange_manager1.close()

class SignalChecker:
    def __init__(self):
        self.last_check_time = None
        self.last_signal = True  # Initialize last_signal to True
        self.check_interval = 30  # 30초마다 체크 (기존 60초에서 변경)

    async def check_signal(self):
        current_time = datetime.datetime.now()

        if self.last_check_time is None or (current_time - self.last_check_time).total_seconds() >= self.check_interval:
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
    
    # 포지션 모니터링에 사용할 추가 메서드
    async def monitor_position(self, exchange, symbol):
        """포지션 정보를 주기적으로 확인하고 출력"""
        try:
            positions = await exchange.fetch_positions([symbol])
            if positions and len(positions) > 0:
                position = positions[0]
                position_size = float(position['contracts'])
                entry_price = position.get('entryPrice') or position.get('entryPx')
                leverage = position.get('leverage')
                pnl = position.get('unrealizedPnl')
                
                print(f"\n[Position Status] {symbol}")
                print(f"Size: {position_size} units")
                print(f"Entry price: {entry_price}")
                print(f"Leverage: {leverage}x")
                if pnl is not None:
                    print(f"Unrealized PnL: {pnl}")
                return position
            else:
                print(f"No open position found for {symbol}")
                return None
        except Exception as e:
            print(f"Error checking position: {e}")
            return None

async def monitor_and_close_position(executor, signal_checker, exchange1, symbol):
    """포지션 모니터링 및 종료를 위한 별도 함수"""
    print("\n[DEBUG] Monitoring position for close signal...")
    
    # 거래소 매니저 생성
    exchange_manager = executor.exchange_managers.get(exchange1)
    if not exchange_manager:
        exchange_manager = ExchangeManager(exchange1)
        executor.exchange_managers[exchange1] = exchange_manager
    
    check_interval = 30  # 30초마다 체크
    last_position_check = 0
    
    while True:
        try:
            # 1. 시그널 확인
            signal_result = await signal_checker.check_signal()
            
            # 2. 주기적으로 포지션 상태 출력
            current_time = time.time()
            if current_time - last_position_check >= check_interval:
                exchange = await exchange_manager.create_exchange()
                if exchange:
                    try:
                        await signal_checker.monitor_position(exchange, symbol)
                        last_position_check = current_time
                    finally:
                        await exchange_manager.close()
            
            # 3. Signal이 False로 변경된 경우 포지션 종료 시작
            if signal_result is False:
                print("\n[SIGNAL] Signal turned False. Starting position close...")
                await executor.close_position(signal_checker)
                break
                
            await asyncio.sleep(5)  # 5초 대기 (더 자주 확인)
        except Exception as e:
            print(f"Error during position monitoring: {e}")
            await asyncio.sleep(5)  # 오류 발생 시에도 계속 시도

async def main():
    # 거래 정보 설정
    trade_info = {"bitget": ["buy", "DOGE/USDT:USDT", "limit", 0.0000000000001, None, 1], 
                  "bybit": ["DOGE/USDT:USDT", "market", None, 1]}
    
    try:
        async with TradeExecutor(trade_info) as executor:
            try:
                print("\n[DEBUG] Starting open position process...")
                exchange1, params1 = list(trade_info.items())[0]
                
                # 주문 실행
                has_filled_order = await executor.open_position()

                if has_filled_order:
                    print("\n[DEBUG] Orders filled, setting up position monitoring...")    
                    signal_checker = SignalChecker()
                    
                    # 개선된 모니터링 함수 사용
                    await monitor_and_close_position(executor, signal_checker, exchange1, params1[1])
                else:
                    print("\n[ERROR] No orders were filled. Skipping close position.")
            except Exception as e:
                print(f"\n[CRITICAL ERROR] Error in main execution: {e}")
                import traceback
                traceback.print_exc()
    finally:
        # 추가적인 정리 로직
        await cleanup_resources()

async def cleanup_resources():
    """모든 비동기 리소스를 적극적으로 정리"""
    try:
        print("Cleaning up resources...")
        
        # 1. aiohttp 세션 정리
        import aiohttp
        for session in [obj for obj in gc.get_objects() if isinstance(obj, aiohttp.ClientSession)]:
            try:
                if not session.closed:
                    await session.close()
                    print("Closed an unclosed ClientSession")
            except Exception as e:
                print(f"Error closing session: {e}")
        
        # 2. CCXT 거래소 연결 정리
        for exchange_name in ['hyperliquid', 'bitget', 'bybit', 'binance']:
            try:
                exchange_class = getattr(ccxt, exchange_name, None)
                if exchange_class:
                    exchange = exchange_class()
                    if hasattr(exchange, 'close') and callable(exchange.close):
                        await exchange.close()
                        print(f"Closed {exchange_name} connections")
            except Exception as e:
                print(f"Error closing {exchange_name}: {e}")
        
        # 3. 현재 실행 중인 모든 작업 취소
        import asyncio
        for task in asyncio.all_tasks():
            if task != asyncio.current_task() and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        
        # 4. 명시적 가비지 컬렉션 실행
        gc.collect()
        
        print("Cleanup completed successfully.")
    except Exception as e:
        print(f"Error during cleanup: {e}")

if __name__ == '__main__':
    asyncio.run(main())