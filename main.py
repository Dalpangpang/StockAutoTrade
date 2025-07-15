import configparser
import schedule
import time
from datetime import datetime, timedelta, date
import pandas as pd
import logging

from database.db_handler import DBHandler
from api.kis_api import KISApi
from core.model_trainer import ModelTrainer
from core.trader import Trader
from common.logger import setup_logger
from common.utils import calculate_indicators


def _is_domestic(ticker):
    """종목 코드가 국내 주식인지 확인하는 헬퍼 함수"""
    return ticker.isdigit()


def collect_data_job(db, api, tickers):
    """일봉과 분봉 데이터를 모두 수집하여 각 테이블에 저장하는 함수"""
    logger.info("=" * 50)
    logger.info(f"데이터 수집 작업 시작: {datetime.now()}")
    logger.info("=" * 50)
    for ticker in tickers:
        try:
            logger.info(f"--- '{ticker}' 종목 처리 시작 ---")

            # 1. 분봉 데이터 수집 (오늘 하루 데이터만 수집/업데이트)
            table_min = 'stock_data_min'
            last_ts_min = db.get_last_timestamp(ticker, table_min)
            logger.info(f"'{ticker}' 분봉 DB 마지막 시간: {last_ts_min}")

            start_to_fetch = None
            if last_ts_min and last_ts_min.date() == date.today():
                start_to_fetch = last_ts_min

            chart_min = api.get_day_chart(ticker, start=start_to_fetch)
            time.sleep(0.5)

            if chart_min and chart_min.bars:
                df_api = pd.DataFrame([{'ticker': ticker, 'timestamp': b.time, 'open': b.open, 'high': b.high,
                                        'low': b.low, 'close': b.close, 'volume': b.volume, 'trading_value': b.amount}
                                       for b in chart_min.bars])
                if not df_api.empty:
                    if pd.api.types.is_datetime64_any_dtype(df_api['timestamp']) and df_api[
                        'timestamp'].dt.tz is not None:
                        df_api['timestamp'] = df_api['timestamp'].dt.tz_localize(None)

                    df_db = db.get_last_n_rows(ticker, table_min, n=40)
                    df_combined = pd.concat([df_db, df_api]).drop_duplicates(subset=['timestamp'],
                                                                             keep='last').sort_values(
                        by='timestamp').reset_index(drop=True)
                    df_with_indicators = calculate_indicators(df_combined)

                    if last_ts_min:
                        df_to_insert = df_with_indicators[df_with_indicators['timestamp'] > last_ts_min].copy()
                    else:
                        df_to_insert = df_with_indicators.copy()

                    logger.info(f"'{ticker}' 저장할 신규 분봉 데이터 개수: {len(df_to_insert)}")

                    if not df_to_insert.empty:
                        db.insert_data(df_to_insert.dropna(), table_min)
                        logger.info(f"'{ticker}': 분봉 {len(df_to_insert)}개 신규 데이터 저장 완료")

            # 2. 일봉 데이터 수집
            table_day = 'stock_data_day'
            last_ts_day = db.get_last_timestamp(ticker, table_day)
            logger.info(f"'{ticker}' 일봉 DB 마지막 시간: {last_ts_day}")
            start_date_day = (last_ts_day + timedelta(days=1)).date() if last_ts_day else date(1980, 1, 1)

            if start_date_day <= date.today():
                chart_day = api.get_daily_chart(ticker, start_date=start_date_day)
                time.sleep(0.5)
                if chart_day and chart_day.bars:
                    df_api_day = pd.DataFrame([{'ticker': ticker, 'timestamp': b.time, 'open': b.open, 'high': b.high,
                                                'low': b.low, 'close': b.close, 'volume': b.volume,
                                                'trading_value': b.amount} for b in chart_day.bars])
                    if not df_api_day.empty:
                        df_with_day_indicators = calculate_indicators(df_api_day)
                        db.insert_data(df_with_day_indicators.dropna(), table_day)
                        logger.info(f"'{ticker}': 일봉 {len(df_with_day_indicators)}개 신규 데이터 저장 완료")

            logger.info(f"--- '{ticker}' 종목 처리 완료 ---\n")

        except Exception as e:
            logger.error(f"'{ticker}' 데이터 수집 중 오류 발생: {e}", exc_info=True)


def main():
    global logger
    logger = setup_logger()
    try:
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')

        db_handler = DBHandler(config['DATABASE'])
        kis_api = KISApi(config['API'])

        domestic_tickers = config['TRADING'].get('domestic_tickers', '').split(',')
        overseas_tickers = config['TRADING'].get('overseas_tickers', '').split(',')
        tickers = [t.strip() for t in domestic_tickers + overseas_tickers if t.strip()]

        if not tickers:
            logger.warning("설정 파일에 분석할 종목(tickers)이 지정되지 않았습니다.")
            return

        run_mode = config['TRADING'].get('run_mode', 'collect')

        # [수정] db -> db_handler 로 변수명 수정
        schedule.every(1).minutes.do(collect_data_job, db_handler, kis_api, tickers)

        logger.info("초기 데이터 수집을 시작합니다.")
        # [수정] db -> db_handler 로 변수명 수정
        collect_data_job(db_handler, kis_api, tickers)

        if run_mode == 'train':
            logger.info("모델 훈련 모드로 실행합니다. 훈련 완료 후 데이터 수집만 계속됩니다.")
            trainer = ModelTrainer(db_handler, config['TRADING'])
            trainer.train()
        elif run_mode == 'trade':
            logger.info("자동 매매/알림 모드로 실행합니다.")
            trader = Trader(kis_api, db_handler, config['TRADING'])
            schedule.every(5).minutes.do(trader.run)
        else:
            logger.info("데이터 수집 모드로 실행합니다.")

        logger.info(f"'{run_mode}' 모드 설정 완료. 데이터 수집은 1분마다 주기적으로 실행됩니다.")

        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as e:
        logger.critical("프로그램 실행 중 심각한 오류가 발생하여 종료합니다.", exc_info=True)


if __name__ == "__main__":
    main()