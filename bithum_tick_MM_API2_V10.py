import json
import websocket
import asyncio
import time
from dotenv import load_dotenv
load_dotenv()
import os
import platform
import jwt
import uuid
import aiohttp
import hashlib
from urllib.parse import urlencode
from tools import get_timestamp_now, COLORS
import pandas as pd
import threading
import signal
import math
import numpy as np

# Windows에서 SelectorEventLoop 사용
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# 전역 변수로 잔고 정보 저장
current_balances = {}
current_unexecuted_quantity = {} # 예시 {('KRW-KAITO', 1840): 0.0025}

# ctrl+c로 웹소켓 종료하기 위한 코드 
current_ws = None
def signal_handler(signum, frame):
    print(f"\n[{get_timestamp_now()}] Received signal to terminate")
    if current_ws:
        print(f"[{get_timestamp_now()}] Closing WebSocket connection...")
        current_ws.close()
    exit(0)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# generate headers
def generate_headers(query=None):
    accessKey = os.getenv('BITHUM_MM_API2_KEY')
    secretKey = os.getenv('BITHUM_MM_API2_SECRET')
    payload = {
        'access_key': accessKey,
        'nonce': str(uuid.uuid4()),
        'timestamp': round(time.time() * 1000)
    }
    
    if query:
        hash = hashlib.sha512()
        hash.update(query.encode())
        query_hash = hash.hexdigest()
        payload['query_hash'] = query_hash
        payload['query_hash_alg'] = 'SHA512'

    jwt_token = jwt.encode(payload, secretKey)
    authorization_token = 'Bearer {}'.format(jwt_token)
    headers = {
        'Authorization': authorization_token,
        'Content-Type': 'application/json'
    }
    return headers

async def place_order(market, side, order_type, price, volume):
    api_url = 'https://api.bithumb.com/v2/orders'
    request_body = {
        'market': market,
        'side': side,
        'order_type': order_type,
        'price': price,
        'volume': volume
    }
    query = urlencode(request_body)
    headers = generate_headers(query=query)
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(api_url, json=request_body, headers=headers) as response:
                status_code = response.status
                response_json = await response.json()
                print(f"[{get_timestamp_now()}] Order Response - Status Code: {status_code}")
                print(json.dumps(response_json, indent=2))
                return response_json
        except Exception as err:
            print(f"[{get_timestamp_now()}] Order Error: {str(err)}")
            return None

def on_message(ws, message, trading_params):
    global current_balance
    try:
        data = json.loads(message)
        if data.get("status") == "UP":
            pass
        elif data.get("type") == "myOrder":
            print(f"[{get_timestamp_now()}] Received myOrder data:")
            data_handled = json.dumps(data, indent=2)
            print(data_handled)
            
            # 매수 체결 시 매도 주문 실행
            if (data.get("ask_bid") == "BID") & ((data.get("state") == "trade") or (data.get("state") == "done")):
                market = data.get("code")
                market_params = trading_params.get(market, trading_params['default'])
                market_BasisPlus = market_params['BasisPlus']
                buy_price = float(data.get("price"))
                sell_price = math.ceil(buy_price * (1 + market_BasisPlus/10000))
                executed_volume = float(data.get("executed_volume"))

                if (sell_price * executed_volume >= 5000):
                    print(f"[{get_timestamp_now()}] Buy order executed. Placing sell order: "
                          f"Market={market}, Price={sell_price}, Volume={executed_volume}")
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(
                            place_order(
                                market=market,
                                side='ask',
                                order_type='limit',
                                price=sell_price,
                                volume=executed_volume
                            )
                        )
                        if result:
                            print(f"[{get_timestamp_now()}] Sell order for executed volume sent")
                        else:
                            print(f"[{get_timestamp_now()}] Failed to place sell order")
                    except Exception as e:
                        print(f"[{get_timestamp_now()}] Error placing sell order: {str(e)}")
                    finally:
                        loop.close()
                else:
                    print(f"[{get_timestamp_now()}] Transaction Value not exceeding 5000 KRW. Checking balance.")
                    if (sell_price * current_balances[market]) >= 5000:
                        print(f"[{get_timestamp_now()}] Balance is sufficient. Placing sell order."
                              f"Market={market}, Price={sell_price}, Volume={current_balances[market]}")
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            result = loop.run_until_complete(
                                place_order(
                                    market=market,
                                    side='ask',
                                    order_type='limit',
                                    price=sell_price,
                                    volume=current_balances[market]
                                )
                            )
                            if result:
                                print(f"[{get_timestamp_now()}] Sell order for current balance sent")
                        except Exception as e:
                            print(f"[{get_timestamp_now()}] Error placing sell order: {str(e)}")
                        finally:
                            loop.close()
                    else:
                        print(f"[{get_timestamp_now()}] Balance is insufficient. Skipping sell order.")

            # 매도 체결 시 매수 주문 실행
            elif (data.get("ask_bid") == "ASK") & ((data.get("state") == "trade") or (data.get("state") == "done")):
                market = data.get("code")
                market_params = trading_params.get(market, trading_params['default'])
                market_BasisPlus = market_params['BasisPlus']
                buy_price = math.floor(float(data.get("price")) * (1 - market_BasisPlus/10000))
                executed_funds = float(data.get("executed_funds"))
                executing_volume = np.floor(executed_funds/buy_price*10000)/10000

                if (executed_funds >= 5000):
                    print(f"[{get_timestamp_now()}] Sell order executed. Placing buy order: "
                          f"Market={market}, Price={buy_price}, Volume={executing_volume}")
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(
                            place_order(
                                market=market,
                                side='bid',
                                order_type='limit',
                                price=buy_price,
                                volume=executing_volume
                            )
                        )
                        if result:
                            print(f"[{get_timestamp_now()}] Buy order for executed volume sent")
                        else:
                            print(f"[{get_timestamp_now()}] Failed to place buy order")
                    except Exception as e:
                        print(f"[{get_timestamp_now()}] Error placing buy order: {str(e)}")
                    finally:
                        loop.close()
                else:
                    print(f"[{get_timestamp_now()}] Transaction Value not exceeding 5000 KRW. Checking unexcecuted quantity.")
                    ticker_price_tuple = (market, buy_price)
                    try:
                        current_unexecuted_quantity[ticker_price_tuple] = current_unexecuted_quantity[ticker_price_tuple]+ executing_volume
                    except:
                        current_unexecuted_quantity[ticker_price_tuple] = executing_volume
                    if (buy_price * current_unexecuted_quantity[ticker_price_tuple]) >= 5000:
                        print(f"[{get_timestamp_now()}] unexcecuted quantity is sufficient. Placing buy order."
                              f"Market={market}, Price={buy_price}, Volume={current_unexecuted_quantity[ticker_price_tuple]}")
                        
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            result = loop.run_until_complete(
                                place_order(
                                    market=market,
                                    side='bid',
                                    order_type='limit',
                                    price=buy_price,
                                    volume=current_unexecuted_quantity[ticker_price_tuple]
                                )
                            )
                            current_unexecuted_quantity[ticker_price_tuple] = 0
                            if result:
                                print(f"[{get_timestamp_now()}] Buy order for unexcecuted quantity sent")
                        except Exception as e:
                            print(f"[{get_timestamp_now()}] Error placing buy order: {str(e)}")
                        finally:
                            loop.close()
                        

        elif data.get("type") == "myAsset":
            print(f"[{get_timestamp_now()}] Received myAsset data:")
            data_assets = data.get("assets")[0]
            if data_assets:
                changed_currency= data_assets.get("currency", {})
                changed_balance= data_assets.get("balance", {})
                current_balances[changed_currency] = changed_balance
                print(f"[{get_timestamp_now()}] Balance updated: {json.dumps(current_balances, indent=2)}")
        else:
            print(f"[{get_timestamp_now()}] Received data:")
            data_handled = json.dumps(data, indent=2)
            print(data_handled)
            # status가 down이면 웹소켓 연결 종료후 재연결
                
    except json.JSONDecodeError:
        print(f"[{get_timestamp_now()}] Non-JSON message: {message}")
    except Exception as e:
        print(f"[{get_timestamp_now()}] Error in on_message: {str(e)}")

def on_error(ws, error):
    print(f"[{get_timestamp_now()}] WebSocket Error: {str(error)}")

def on_close(ws, close_status_code, close_msg):
    print(f"[{get_timestamp_now()}] WebSocket Closed - Code: {close_status_code}, Message: {close_msg}")
    ws.connected = False

def on_open(ws, trading_params):
    print(f"[{get_timestamp_now()}] Opening WebSocket")
    try:
        subscribe_message = [
            {"ticket": "TopQuant_KSK"},
            {"type": "myOrder", "codes": []},
            {"type": "myAsset"}
        ]
        ws.send(json.dumps(subscribe_message))
        print(f"[{get_timestamp_now()}] Subscribed myOrder and myAsset")
        ws.send("PING")
        print(f"[{get_timestamp_now()}] Sent initial PING")
        ws.connected = True
        ws.trading_params = trading_params
    except Exception as e:
        print(f"[{get_timestamp_now()}] Error in on_open: {str(e)}")
        ws.connected = False

async def run_websocket(trading_params):
    global current_ws
    websocket_url = "wss://ws-api.bithumb.com/websocket/v1/private"
    max_attempts = 10
    base_delay = 1
    max_delay = 30

    def run_ws_loop(ws):
        try:
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"[{get_timestamp_now()}] WebSocket loop error: {str(e)}")
        finally:
            if ws.sock:
                ws.close()

    async def attempt_connection(attempt):
        global current_ws
        headers = generate_headers()
        ws = websocket.WebSocketApp(
            websocket_url,
            header=headers,
            on_open=lambda ws: on_open(ws, trading_params),
            on_message=lambda ws, msg: on_message(ws, msg, trading_params),
            on_error=on_error,
            on_close=on_close
        )
        current_ws = ws
        ws.connected = False
        print(f"[{get_timestamp_now()}] Attempting connection (Attempt {attempt}/{max_attempts})")

        thread = threading.Thread(target=run_ws_loop, args=(ws,), daemon=True)
        thread.start()

        timeout = 5
        start_time = time.time()
        while time.time() - start_time < timeout:
            if getattr(ws, 'connected', False) and ws.sock and ws.sock.connected:
                print(f"[{get_timestamp_now()}] Connection successful")
                return ws
            await asyncio.sleep(0.1)

        print(f"[{get_timestamp_now()}] Connection attempt timed out")
        if ws.sock:
            ws.close()
        return ws

    for attempt in range(1, max_attempts + 1):
        ws = await attempt_connection(attempt)
        if getattr(ws, 'connected', False) and ws.sock and ws.sock.connected:
            return ws
        if attempt < max_attempts:
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            print(f"[{get_timestamp_now()}] Reconnection attempt in {delay} seconds...")
            await asyncio.sleep(delay)
    
    print(f"[{get_timestamp_now()}] Max reconnection attempts reached. Giving up.")
    return None

async def main():
    try:
        TRADING_PARAMS = {
            'default': {
                'BasisPlus': 16,
            },
            # 'KRW-KAITO': {
            #     'BasisPlus': 16,
            #     'VOLUME': 100,
            #     'PRICE': 1840
            # },                                                                                                                                             
            # 'KRW-MOODENG': {
            #     'BasisPlus': 16,
            #     'VOLUME': 100,
            #     'PRICE': 200
            # },            
        }

        ws = await run_websocket(TRADING_PARAMS)
        if ws is None:
            print(f"[{get_timestamp_now()}] Failed to establish WebSocket connection. Exiting.")
            return
        if len(TRADING_PARAMS.items()) <= 1:
            print(f"[{get_timestamp_now()}] No orders to execute, start monitoring orders.")

        else:
            await asyncio.sleep(2)
            print(f"[{get_timestamp_now()}] Placing multiple orders in parallel...")
            
            order_tasks = []
            for market, params in dict(list(TRADING_PARAMS.items())[1:]).items():
                order_tasks.append(
                    place_order(
                        market=market,
                        side='bid',
                        order_type='limit',
                        price=params['PRICE'],
                        volume=params['VOLUME']
                    )
                )
            
            await asyncio.gather(*order_tasks)
            print(f"[{get_timestamp_now()}] All orders placed.")

        while getattr(ws, 'connected', False):
            await asyncio.sleep(2)
        
    except Exception as e:
        print(f"[{get_timestamp_now()}] Main Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())