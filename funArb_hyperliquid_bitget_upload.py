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

    EXCHANGE_KEYS = {
        "hyperliquid": {"walletAddress": API_KEY, "privateKey": PRIVATE_KEY},
        "bitget": {"apiKey": BITGET_API_KEY, "secret": BITGET_SECRET_KEY, "password": BITGET_PASSWORD}
    }

    def __init__(self, exchange_name):
        self.exchange_name = exchange_name
        self.exchange = None

    def format_symbol(self, symbol):
        """Convert symbol to exchange-specific format"""
        # For Hyperliquid, we'll no longer convert the symbol format
        # We'll use the original symbol as provided in trade_info
        if self.exchange_name == "hyperliquid":
            return symbol  # Use the symbol as is
        elif self.exchange_name == "bitget":
            # Bitget uses the :USDT format
            if ':' not in symbol:
                return f"{symbol}:USDT"
            return symbol
        return symbol

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
                exchange.options['createMarketOrder'] = {'marginMode': 'isolated', 'isolated': True}
                exchange.options['createLimitOrder'] = {'marginMode': 'isolated', 'isolated': True}
                exchange.options['createOrder'] = {'marginMode': 'isolated', 'isolated': True}
                
            elif self.exchange_name == "bitget":
                exchange.options['defaultType'] = 'swap'

            # Test connection and load markets
            try:
                await exchange.load_markets()
                print(f"Successfully connected to {self.exchange_name}")
                
            except Exception as e:
                print(f"Error loading markets: {str(e)}")
                raise

            return exchange

        except Exception as e:
            print(f"Error creating {self.exchange_name} exchange: {str(e)}")
            return None

    async def close(self):
        """명시적으로 거래소 연결 종료"""
        if self.exchange is not None:
            try:
                print(f"Closing {self.exchange_name} exchange connection...")
                await self.exchange.close()
                print(f"✓ Closed {self.exchange_name} exchange connection")
            except Exception as e:
                print(f"Error closing exchange {self.exchange_name}: {e}")
            finally:
                self.exchange = None

    async def set_leverage(self, symbol, leverage):
        try:
            if self.exchange_name == "hyperliquid":
                print("\n[DEBUG] Setting Hyperliquid Initial Configuration")
                try:
                    # 먼저 레버리지만 설정
                    print(f"[DEBUG] Setting leverage to {leverage}")
                    await self.exchange.set_leverage(leverage, symbol)
                    
                    # 그 다음 마진 모드 설정 (레버리지 파라미터 포함)
                    print(f"[DEBUG] Setting margin mode to isolated with leverage {leverage}")
                    await self.exchange.set_margin_mode('isolated', symbol, {
                        'leverage': leverage,  # Critical: include leverage parameter
                        'marginMode': 'isolated',
                        'tdMode': 'isolated',
                        'positionType': 'isolated'
                    })
                    
                    print("[DEBUG] Successfully configured margin mode and leverage")
                    
                except Exception as e:
                    print(f"[ERROR] Failed to set initial configuration: {str(e)}")
                    print("[WARNING] Continuing with the trade despite configuration error")

            elif self.exchange_name == "bitget":
                try:
                    formatted_symbol = symbol.split(':')[0].replace('/', '')
                    
                    # 마진 모드를 isolated로 설정
                    await self.exchange.set_margin_mode('isolated', symbol)
                    
                    # 레버리지 설정
                    await self.exchange.set_leverage(leverage, symbol, {
                        'marginCoin': 'USDT',
                        'symbol': formatted_symbol,
                        'marginMode': 'isolated'
                    })
                    
                except Exception as e:
                    print(f"Error setting margin mode or leverage on Bitget: {e}")
                    print("[WARNING] Continuing with the trade despite configuration error")
                    
            else:
                await self.exchange.set_leverage(leverage, symbol)
                
        except Exception as e:
            print(f"Error setting leverage on {self.exchange_name}: {e}")
            # Don't throw an error, continue with the trade
            print("[WARNING] Continuing with the trade despite leverage setting error")

    async def get_orderbook_depth(self, symbol):
        """Get the second level orderbook depth"""
        exchange = None
        try:
            if not self.exchange:
                self.exchange = await self.create_exchange()
                if not self.exchange:
                    print(f"Failed to create {self.exchange_name} exchange")
                    return None
            
            exchange = self.exchange  # Keep a reference for cleanup
            
            # Use symbol directly for Hyperliquid - no formatting
            formatted_symbol = self.format_symbol(symbol)
            print(f"Using symbol for {self.exchange_name}: {formatted_symbol}")
            
            try:
                orderbook = await exchange.fetch_order_book(formatted_symbol)
                
                if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                    if len(orderbook['bids']) >= 2 and len(orderbook['asks']) >= 2:
                        second_bid = orderbook['bids'][1]  # [price, amount]
                        second_ask = orderbook['asks'][1]  # [price, amount]
                        return {
                            'bid': {'price': second_bid[0], 'amount': second_bid[1]},
                            'ask': {'price': second_ask[0], 'amount': second_ask[1]}
                        }
                    else:
                        print(f"Orderbook for {formatted_symbol} doesn't have enough depth")
                        # Return first level if second level not available
                        if len(orderbook['bids']) >= 1 and len(orderbook['asks']) >= 1:
                            first_bid = orderbook['bids'][0]
                            first_ask = orderbook['asks'][0]
                            print(f"Using first level orderbook depth instead")
                            return {
                                'bid': {'price': first_bid[0], 'amount': first_bid[1]},
                                'ask': {'price': first_ask[0], 'amount': first_ask[1]}
                            }
                else:
                    print(f"Invalid orderbook data format for {formatted_symbol}")
                    return None
                    
            except Exception as e:
                print(f"Error fetching orderbook for {formatted_symbol}: {e}")
                return None
                
        except Exception as e:
            print(f"Error in get_orderbook_depth: {e}")
            return None

    async def execute_trade(self, direction, symbol, order_type, amount, leverage, price=None):
        try:
            if not self.exchange:
                self.exchange = await self.create_exchange()
                if not self.exchange:
                    print(f"[ERROR] Failed to create {self.exchange_name} exchange")
                    return None

            # Use original symbol for all exchanges
            formatted_symbol = self.format_symbol(symbol)
            print(f"Using symbol for {self.exchange_name}: {formatted_symbol}")

            # Set leverage and margin mode
            await self.set_leverage(formatted_symbol, leverage)

            # Reconnect exchange if needed (in case it was closed during set_leverage)
            if not self.exchange:
                print(f"[DEBUG] Reconnecting {self.exchange_name} exchange after set_leverage")
                self.exchange = await self.create_exchange()
                if not self.exchange:
                    print(f"[ERROR] Failed to reconnect {self.exchange_name} exchange")
                    return None

            # If price is None and it's a limit order, get the best price
            if price is None and order_type == "limit":
                try:
                    # Use our get_best_price method to get the best price
                    price = await self.get_best_price(symbol, direction)
                    if not price:
                        print(f"Failed to get price for {symbol}. Cannot create limit order.")
                        return None
                except Exception as e:
                    print(f"Error getting best price: {e}")
                    return None
                    
            # Reconnect again if needed (in case it was closed during get_best_price)
            if not self.exchange:
                print(f"[DEBUG] Reconnecting {self.exchange_name} exchange after get_best_price")
                self.exchange = await self.create_exchange()
                if not self.exchange:
                    print(f"[ERROR] Failed to reconnect {self.exchange_name} exchange")
                    return None

            # Prepare order parameters
            params = {
                "marginMode": "isolated",
                "leverage": leverage,
                "tdMode": "isolated"
            }

            if self.exchange_name == "bitget":
                params.update({
                    "marginCoin": "USDT"
                })
            elif self.exchange_name == "hyperliquid":
                params.update({
                    "positionType": "isolated",
                    "isolated": True,
                    "crossMargin": False,
                    "mgnMode": "isolated",
                    "marginType": "isolated",
                    "margin_mode": "isolated",
                    "leverage": leverage  # Add leverage here too
                })

            # Create order
            print(f"[DEBUG] Creating {order_type} {direction} order for {amount} {formatted_symbol} at price {price}")
            order = await self.exchange.create_order(
                symbol=formatted_symbol,
                type=order_type,
                side=direction,
                amount=amount,
                price=price if order_type == "limit" else None,
                params=params
            )

            print(f"Order created: {order}")
            return order

        except Exception as e:
            print(f"Error executing trade: {e}")
            return None
        finally:
            # Don't close the exchange here, we might need it again
            pass

    async def cancel_order(self, order_id, symbol):
        try:
            if not self.exchange:
                self.exchange = await self.create_exchange()
                if not self.exchange:
                    print(f"[ERROR] Failed to create {self.exchange_name} exchange for cancelling order")
                    return None
            
            # Use original symbol for all exchanges
            formatted_symbol = self.format_symbol(symbol)
            
            print(f"[DEBUG] Cancelling order {order_id} for {formatted_symbol}")
            result = await self.exchange.cancel_order(order_id, formatted_symbol)
            print(f"Order cancelled: {result}")
            return result
        except Exception as e:
            print(f"Error cancelling order: {e}")
            return None
        finally:
            # Don't close the exchange here, we might need it again
            pass

    async def get_best_price(self, symbol, direction):
        """Get the best bid or ask price from orderbook"""
        try:
            # Create a new connection if needed
            if not self.exchange:
                print(f"[DEBUG] Creating new exchange connection for get_best_price")
                self.exchange = await self.create_exchange()
                if not self.exchange:
                    print(f"Failed to create {self.exchange_name} exchange instance in get_best_price")
                    return None
            
            # Use symbol directly for Hyperliquid - no formatting
            formatted_symbol = self.format_symbol(symbol)
            print(f"Getting best price for {formatted_symbol} on {self.exchange_name}")
            
            try:
                print(f"[DEBUG] Fetching orderbook for {formatted_symbol}")
                orderbook = await self.exchange.fetch_order_book(formatted_symbol)
                if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                    if len(orderbook['bids']) > 0 and len(orderbook['asks']) > 0:
                        # For buying, we need to look at asks (sell offers)
                        # For selling, we need to look at bids (buy offers)
                        if direction == 'sell':
                            price = orderbook['bids'][0][0]  # Best bid price
                        else:  # direction == 'buy'
                            price = orderbook['asks'][0][0]  # Best ask price
                        
                        print(f"Got best {direction} price for {formatted_symbol}: {price}")
                        return price
                    else:
                        print(f"[ERROR] Orderbook for {formatted_symbol} is empty")
                        return None
                else:
                    print(f"[ERROR] Invalid orderbook data received: {orderbook}")
                    return None
            except Exception as e:
                print(f"[ERROR] Error fetching orderbook: {e}")
                return None
                
        except Exception as e:
            print(f"[ERROR] Error in get_best_price: {e}")
            return None

class TradeExecutor:
    def __init__(self, trade_info):
        self.trade_info = trade_info
        self.exchange_managers = {}
        self.processed_orders = set()

    async def open_position(self):
        """주문 실행 및 모니터링"""
        # Initialize exchange managers
        for exchange_name in self.trade_info.keys():
            if exchange_name not in self.exchange_managers:
                self.exchange_managers[exchange_name] = ExchangeManager(exchange_name)

        try:
            filled_orders = []
            
            # Get the exchange names in order
            exchange_names = list(self.trade_info.keys())
            if len(exchange_names) < 2:
                print("Error: At least two exchanges are required")
                return None
                
            # First exchange (primary)
            exchange1_name = exchange_names[0]
            exchange1_params = self.trade_info[exchange1_name]
            exchange1_manager = self.exchange_managers[exchange1_name]
            
            # Second exchange (secondary, for hedging)
            exchange2_name = exchange_names[1]
            exchange2_params = self.trade_info[exchange2_name]
            exchange2_manager = self.exchange_managers[exchange2_name]
            
            print(f"\n[STEP 1] Executing order on primary exchange: {exchange1_name}")
            
            # Get orderbook depth for first exchange
            depth = await exchange1_manager.get_orderbook_depth(exchange1_params[1])
            if not depth:
                print(f"Failed to get orderbook depth for {exchange1_name}")
                return None

            # Calculate trade amount based on second level orderbook depth
            depth_key = 'ask' if exchange1_params[0] == 'buy' else 'bid'
            if depth_key in depth and 'amount' in depth[depth_key]:
                second_level_amount = depth[depth_key]['amount'] / 2
                print(f"\n{exchange1_name} Trade Setup:")
                print(f"Second level amount: {second_level_amount}")
                
                # Make sure exchange1_params[3] is not None before using min()
                if exchange1_params[3] is not None:
                    trade_amount = min(second_level_amount, exchange1_params[3])
                else:
                    trade_amount = second_level_amount
            else:
                print(f"Invalid depth data for {exchange1_name}, using provided amount")
                trade_amount = exchange1_params[3]
                if trade_amount is None:
                    print(f"No amount specified for {exchange1_name}, cannot proceed")
                    return None
                    
            print(f"Trade amount: {trade_amount}")

            # Execute trade on first exchange
            order1 = await exchange1_manager.execute_trade(
                exchange1_params[0],    # direction
                exchange1_params[1],    # symbol
                exchange1_params[2],    # order type
                trade_amount,           # amount
                exchange1_params[5],    # leverage
                exchange1_params[4]     # price
            )

            if not order1:
                print(f"Failed to create order on {exchange1_name}")
                return None

            # Monitor order for 1 minute
            print(f"\n[STEP 2] Monitoring order on {exchange1_name} for up to 1 minute")
            start_time = time.time()
            order_filled = False
            filled_amount = 0
            exchange = None

            while time.time() - start_time < 60:  # 1 minute timeout
                try:
                    if exchange is None:
                        exchange = await exchange1_manager.create_exchange()
                    if not exchange:
                        print("Failed to create exchange, retrying...")
                        await asyncio.sleep(1)
                        continue

                    # Make sure to use the correct symbol format when checking order status
                    formatted_symbol = exchange1_manager.format_symbol(exchange1_params[1])
                    order_status = await exchange.fetch_order(order1['id'], formatted_symbol)
                    
                    if order_status['status'] == 'closed':
                        print(f"{exchange1_name} order filled: {order_status}")
                        filled_orders.append((exchange1_name, order_status))
                        order_filled = True
                        filled_amount = order_status.get('filled', 0)
                        if filled_amount <= 0:
                            filled_amount = order_status.get('amount', trade_amount)  # Fallback
                        break
                    elif order_status.get('filled', 0) > 0:
                        # Partially filled
                        print(f"{exchange1_name} order partially filled: {order_status.get('filled', 0)}/{order_status.get('amount', 0)}")
                        filled_amount = order_status.get('filled', 0)
                    
                    await asyncio.sleep(2)  # Check every 2 seconds
                    
                except Exception as e:
                    print(f"Error checking order status: {e}")
                    # If error occurs, close and reset exchange
                    if exchange:
                        try:
                            await exchange.close()
                        except:
                            pass
                    exchange = None
                    await asyncio.sleep(2)
            
            # Make sure to close exchange when done with it
            if exchange:
                try:
                    await exchange.close()
                except:
                    pass

            if not order_filled:
                # Cancel order if not filled within 1 minute
                print(f"Cancelling unfilled {exchange1_name} order after 1 minute")
                formatted_symbol = exchange1_manager.format_symbol(exchange1_params[1])
                await exchange1_manager.cancel_order(order1['id'], formatted_symbol)
                
                # If partially filled, proceed with that amount
                if filled_amount > 0:
                    print(f"Order was partially filled with {filled_amount}, proceeding with hedging")
                else:
                    print("Order was not filled, aborting")
                    return filled_orders
            
            # If we reach here, either the order is fully or partially filled
            if filled_amount <= 0:
                print("No amount was filled, aborting")
                return filled_orders
                
            # STEP 3: Execute a hedge order on the second exchange with the opposite direction
            print(f"\n[STEP 3] Executing hedge order on {exchange2_name} with amount {filled_amount}")
            
            # Get the opposite direction for the hedge
            hedge_direction = 'sell' if exchange1_params[0] == 'buy' else 'buy'
            
            # Execute the hedge trade on the second exchange
            hedge_order = await exchange2_manager.execute_trade(
                hedge_direction,        # opposite direction
                exchange2_params[1],    # symbol
                exchange2_params[2],    # order type
                filled_amount,          # use the filled amount from the first exchange
                exchange2_params[5],    # leverage
                exchange2_params[4]     # price
            )

            if hedge_order:
                # Monitor hedge order for 1 minute
                print(f"\n[STEP 4] Monitoring hedge order on {exchange2_name} for up to 1 minute")
                start_time = time.time()
                hedge_filled = False
                exchange = None

                while time.time() - start_time < 60:  # 1 minute timeout
                    try:
                        if exchange is None:
                            exchange = await exchange2_manager.create_exchange()
                        if not exchange:
                            print("Failed to create exchange, retrying...")
                            await asyncio.sleep(1)
                            continue

                        # Make sure to use the correct symbol format when checking order status
                        formatted_symbol = exchange2_manager.format_symbol(exchange2_params[1])
                        order_status = await exchange.fetch_order(hedge_order['id'], formatted_symbol)
                        
                        if order_status['status'] == 'closed':
                            print(f"{exchange2_name} hedge order filled: {order_status}")
                            filled_orders.append((exchange2_name, order_status))
                            hedge_filled = True
                            break
                        
                        await asyncio.sleep(2)  # Check every 2 seconds
                        
                    except Exception as e:
                        print(f"Error checking hedge order status: {e}")
                        # If error occurs, close and reset exchange
                        if exchange:
                            try:
                                await exchange.close()
                            except:
                                pass
                        exchange = None
                        await asyncio.sleep(2)
                
                # Make sure to close exchange when done with it
                if exchange:
                    try:
                        await exchange.close()
                    except:
                        pass

                if not hedge_filled:
                    # Cancel hedge order if not filled within 1 minute
                    print(f"Cancelling unfilled {exchange2_name} hedge order after 1 minute")
                    formatted_symbol = exchange2_manager.format_symbol(exchange2_params[1])
                    await exchange2_manager.cancel_order(hedge_order['id'], formatted_symbol)
                    print("⚠️ WARNING: Primary position is open but hedge position failed!")
            else:
                print(f"Failed to create hedge order on {exchange2_name}")
                print("⚠️ WARNING: Primary position is open but hedge position failed!")

            return filled_orders

        except Exception as e:
            print(f"Error in open_position: {e}")
            return None
        finally:
            # Close all exchange connections
            print("[DEBUG] Closing all exchange connections")
            for manager in self.exchange_managers.values():
                try:
                    await manager.close()
                except Exception as e:
                    print(f"[WARNING] Error closing {manager.exchange_name} connection: {e}")

async def main():
    exchanges = []  # Keep track of all exchange instances
    try:
        print("\n===== Starting Arbitrage Trading Bot =====")
        
        # Example trade_info - use appropriate symbol formats for each exchange
        trade_info = {
            "hyperliquid": ["sell", "DOGE/USDC:USDC", "limit", 70, None, 1],  # Use original symbol format
            "bitget": ["buy", "DOGE/USDT:USDT", "market", 70, None, 1],
        }

        print(f"\nTrade Setup:")
        print(f"Hyperliquid: {trade_info['hyperliquid'][0]} {trade_info['hyperliquid'][1]} {trade_info['hyperliquid'][2]} order, amount: {trade_info['hyperliquid'][3]}, leverage: {trade_info['hyperliquid'][5]}")
        print(f"Bitget: {trade_info['bitget'][0]} {trade_info['bitget'][1]} {trade_info['bitget'][2]} order, amount: {trade_info['bitget'][3]}, leverage: {trade_info['bitget'][5]}")

        executor = TradeExecutor(trade_info)
        filled_orders = await executor.open_position()
        
        if filled_orders:
            print("\nFilled orders:")
            for exchange_name, order in filled_orders:
                print(f"{exchange_name}: {order}")
        else:
            print("\nNo orders were filled")
            
    except Exception as e:
        print(f"\n[CRITICAL ERROR] An unexpected error occurred in main function: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n===== Cleaning up resources =====")
        
        # 모든 거래소 연결 명시적으로 닫기
        await cleanup_resources()
        
        print("Trading bot execution completed")

async def cleanup_resources():
    """모든 거래소 리소스 확실히 정리"""
    try:
        # 1. CCXT 거래소 연결 정리
        for exchange_name in ['hyperliquid', 'bitget']:
            try:
                # 각 거래소 타입별로 명시적으로 인스턴스 생성하고 닫기
                exchange_class = getattr(ccxt, exchange_name, None)
                if exchange_class:
                    config = None
                    
                    if exchange_name == 'hyperliquid':
                        config = {"walletAddress": ExchangeManager.API_KEY, "privateKey": ExchangeManager.PRIVATE_KEY}
                    elif exchange_name == 'bitget':
                        config = {
                            "apiKey": ExchangeManager.BITGET_API_KEY, 
                            "secret": ExchangeManager.BITGET_SECRET_KEY, 
                            "password": ExchangeManager.BITGET_PASSWORD
                        }
                    
                    if config:
                        try:
                            print(f"Explicitly closing {exchange_name} connections...")
                            exchange = exchange_class(config)
                            await exchange.close()
                            print(f"✓ Closed {exchange_name} connections")
                        except Exception as e:
                            print(f"Error while explicitly closing {exchange_name}: {e}")
            except Exception as e:
                print(f"Error closing {exchange_name}: {e}")
        
        # 2. 가비지 컬렉션 강제 실행
        gc.collect()
        
        print("Cleanup completed")
    except Exception as e:
        print(f"Error during cleanup: {e}")

if __name__ == "__main__":
    asyncio.run(main())
