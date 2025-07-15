import pandas as pd

def calculate_indicators(df):
    """주어진 데이터프레임에 보조지표를 계산하여 추가합니다."""
    if df.empty:
        return df
    
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # 이동평균
    df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()
    df['ma20'] = df['close'].rolling(window=20, min_periods=1).mean()

    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))

    # MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()
    df['macd'] = macd - signal

    # 볼린저 밴드
    df['bollinger_upper'] = df['ma20'] + 2 * df['close'].rolling(window=20, min_periods=1).std()
    df['bollinger_lower'] = df['ma20'] - 2 * df['close'].rolling(window=20, min_periods=1).std()
    
    # VWAP
    if 'volume' in df.columns and 'close' in df.columns:
        df['vwap'] = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
    
    return df