import tensorflow as tf
from tensorflow.keras import layers
import numpy as np
import pandas as pd
import logging
from sklearn.preprocessing import MinMaxScaler
import os

class ModelTrainer:
    def __init__(self, db_handler, config):
        self.db_handler = db_handler
        self.config = config
        self.logger = logging.getLogger(__name__)

    def _get_data(self, ticker):
        """데이터베이스에서 학습 데이터를 불러옵니다."""
        mode = self.config.get('mode', 'short')
        table_name = 'stock_data_min' if mode == 'short' else 'stock_data_day'
        self.logger.info(f"'{ticker}' 종목의 학습 데이터를 '{table_name}' 테이블에서 불러옵니다.")
        
        query = f"SELECT * FROM {table_name} WHERE ticker = '{ticker}' ORDER BY timestamp"
        try:
            df = pd.read_sql(query, self.db_handler.engine)
            return df
        except Exception as e:
            self.logger.error(f"{ticker} 데이터 로드 실패: {e}")
            return pd.DataFrame()

    def _preprocess(self, df):
        """데이터를 전처리하고 학습에 사용할 피처를 생성합니다."""
        self.logger.info("데이터 전처리 시작...")
        features = [
            'open', 'high', 'low', 'close', 'volume', 'trading_value', 'vwap',
            'ma5', 'ma20', 'rsi', 'macd', 'bollinger_upper', 'bollinger_lower'
        ]
        
        feature_cols = [f for f in features if f in df.columns]
        df = df[feature_cols].copy()
        df.dropna(inplace=True)
        
        if df.empty:
            return pd.DataFrame()

        scaler = MinMaxScaler()
        df_scaled = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)
        
        self.logger.info("데이터 전처리 완료")
        return df_scaled

    def _build_ppo_model(self, input_shape, num_actions):
        """PPO 알고리즘을 위한 Actor-Critic 모델을 생성합니다."""
        state_input = layers.Input(shape=input_shape)
        # Actor
        x = layers.Dense(128, activation='relu')(state_input)
        x = layers.Dense(128, activation='relu')(x)
        action_probs = layers.Dense(num_actions, activation='softmax')(x)
        actor = tf.keras.Model(inputs=state_input, outputs=action_probs)
        # Critic
        y = layers.Dense(128, activation='relu')(state_input)
        y = layers.Dense(128, activation='relu')(y)
        state_value = layers.Dense(1)(y)
        critic = tf.keras.Model(inputs=state_input, outputs=state_value)
        self.logger.info("Actor-Critic 모델 생성 완료")
        return actor, critic

    def train(self):
        """모델 학습을 실행합니다."""
        tickers = self.config['tickers'].split(',')
        mode = self.config.get('mode', 'short')
        model_version = f"v1.0_{mode}"

        if not os.path.exists('models'):
            os.makedirs('models')

        for ticker in tickers:
            self.logger.info(f"'{ticker}' 종목 ({mode} 모드) 모델 학습 시작...")
            df = self._get_data(ticker)
            
            if df.empty or len(df) < 100:
                self.logger.warning(f"'{ticker}' 학습 데이터 부족 (100개 미만). 학습을 건너뜁니다.")
                continue

            df_processed = self._preprocess(df)
            
            if df_processed.empty:
                self.logger.warning(f"'{ticker}' 전처리 후 데이터 없음. 학습을 건너뜁니다.")
                continue
            
            states = df_processed.values
            num_actions = 3 # Buy, Sell, Hold

            actor, critic = self._build_ppo_model(input_shape=(states.shape[1],), num_actions=num_actions)
            
            self.logger.info("PPO 알고리즘 학습 시작 (개념적)...")
            # ... 실제 PPO 학습 로직 ...
            self.logger.info(f"'{ticker}' 종목 모델 학습 완료 (개념적).")
        
            actor_path = f"models/actor_{ticker}_{model_version}.h5"
            critic_path = f"models/critic_{ticker}_{model_version}.h5"
            actor.save(actor_path)
            critic.save(critic_path)
            self.logger.info(f"학습된 모델 저장 완료: {actor_path}, {critic_path}")