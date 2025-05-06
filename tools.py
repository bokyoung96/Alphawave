from datetime import datetime

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
