import sqlite3
import pandas as pd

# 데이터 조회
conn = sqlite3.connect('ohlcv.db')
table_name = 'MOODENG_USDT_USDT'  # 심볼에 맞게 변환

# SQL 쿼리 결과를 DataFrame으로 변환
query = f'''
    SELECT timestamp, open, high, low, close, volume 
    FROM {table_name}
    ORDER BY timestamp DESC
    LIMIT 10
'''
df = pd.read_sql_query(query, conn)

# timestamp를 datetime으로 변환하고 index로 설정 (UTC+0)
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
df.set_index('timestamp', inplace=True)

# 컬럼명 한글로 변경
df.columns = ['시가', '고가', '저가', '종가', '거래량']

# 소수점 자리수 설정
pd.set_option('display.float_format', lambda x: '%.5f' % x)

# DataFrame 출력
print("\n=== 최근 10개 캔들 (UTC+0) ===")
print(df)

conn.close()