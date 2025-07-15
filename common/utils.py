import pandas as pd
import numpy as np

def calculate_indicators(df):
    """주어진 데이터프레임에 보조지표를 계산하여 추가합니다."""
    if df.empty:
        return df

    df = df.sort_values('timestamp').reset_index(drop=True)

    # --- 데이터 타입 변환 (Decimal -> float) ---
    # TypeError를 방지하기 위해 숫자 연산이 필요한 모든 컬럼을 float 형태로 변환합니다.
    cols_to_convert = ['open', 'high', 'low', 'close', 'volume', 'trading_value']
    for col in cols_to_convert:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])
    # ---------------------------------------------

    # 이동평균
    df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()
    df['ma20'] = df['close'].rolling(window=20, min_periods=1).mean()

    # RSI (0으로 나누기 오류 수정)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()

    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    df.loc[loss == 0, 'rsi'] = 100
    df['rsi'] = df['rsi'].fillna(50) # gain과 loss가 모두 0일 경우, 중간값인 50으로 처리 (FutureWarning 해결)

    # MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()
    df['macd'] = macd - signal

    # 볼린저 밴드
    df['bollinger_upper'] = df['ma20'] + 2 * df['close'].rolling(window=20, min_periods=1).std()
    df['bollinger_lower'] = df['ma20'] - 2 * df['close'].rolling(window=20, min_periods=1).std()

    # VWAP (0으로 나누기 오류 수정)
    if 'volume' in df.columns and 'close' in df.columns:
        cum_vol = df['volume'].cumsum()
        df['vwap'] = (df['close'] * df['volume']).cumsum() / cum_vol.replace(0, 1e-10)

    return df