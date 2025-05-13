from datetime import datetime
import math

# 초 단위 타임스탬프 반환 함수
def get_timestamp_now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ANSI 색상 코드 정의
COLORS = {
    'USDT/KRW': '\033[92m',  # 녹색
    'USDC/KRW': '\033[94m',  # 파란색
    'PUNDIX/KRW': '\033[93m',  # 노란색
    'RESET': '\033[0m'  # 색상 초기화
}

def get_price_unit(price):
    if price < 1:     #0.3이하 -> 30bp 호가 갭
        unit = 0.0001
    elif price < 10:
        unit = 0.001
    elif price < 100:
        unit = 0.01
    elif price < 5000: #300이하 -> 30bp 호가 갭
        unit = 1
    elif price < 10000:
        unit = 5
    elif price < 50000:
        unit = 10
    elif price < 100000:
        unit = 50
    elif price < 500000:
        unit = 100
    elif price < 1000000:
        unit = 500
    else:
        unit = 1000
    return unit

def price_precision_rounding(price, roundup=True):
    """
    가격 정밀도에 맞춰 반올림하는 함수
    
    Args:
        price (float): 반올림할 가격
        roundup (bool): True면 올림, False면 내림
    
    Returns:
        float: 호가단위에 맞춰 반올림된 가격
    """
    # 호가단위 정의
    unit = get_price_unit(price)
    # 올림/내림 처리
    if roundup:
        # 올림: 현재 가격을 unit으로 나눈 후 올림하고 다시 unit을 곱함
        return round(math.ceil(price / unit) * unit, 4)
    else:
        # 내림: 현재 가격을 unit으로 나눈 후 내림하고 다시 unit을 곱함
        return round(math.floor(price / unit) * unit, 4)