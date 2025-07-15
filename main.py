import configparser
import schedule
import time
from datetime import datetime, timedelta
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
    logger.info("데이터 수집 작업 시작...")
    for ticker in tickers:
        try:
            # 1. 분봉 데이터 수집
            table_min = 'stock_data_min'
            last_ts_min = db.get_last_timestamp(ticker, table_min)

            # 마지막 데이터가 없으면(최초 수집), 5년 전부터 데이터 수집 시작 (API 한도 고려)
            start_time = None
            if _is_domestic(ticker):
                start_time = last_ts_min + timedelta(minutes=1) if last_ts_min else datetime.now() - timedelta(
                    days=365 * 5)

            chart_min = api.get_day_chart(ticker, start_time=start_time)
            if chart_min and chart_min.bars:
                data_min = [{'ticker': ticker, 'timestamp': b.time, 'open': b.open, 'high': b.high, 'low': b.low,
                             'close': b.close, 'volume': b.volume, 'trading_value': b.amount} for b in chart_min.bars]
                df_min = pd.DataFrame(data_min)

                if last_ts_min: df_min = df_min[df_min['timestamp'] > last_ts_min]

                if not df_min.empty:
                    df_min = calculate_indicators(df_min)
                    # [개선] 보조지표 계산 시 발생하는 초기 NaN 값 제거
                    df_min.dropna(inplace=True)
                    db.insert_data(df_min, table_min)
                    logger.info(f"'{ticker}': 분봉 {len(df_min)}개 신규 데이터 저장")

            # 2. 일봉 데이터 수집
            table_day = 'stock_data_day'
            last_ts_day = db.get_last_timestamp(ticker, table_day)

            # [개선] 마지막 데이터가 없으면(최초 수집), 1980년부터 조회하여 상장일 부터 모든 데이터 확보
            start_date = (last_ts_day + timedelta(days=1)).date() if last_ts_day else datetime(1980, 1, 1).date()

            chart_day = api.get_daily_chart(ticker, start_date=start_date)
            if chart_day and chart_day.bars:
                data_day = [{'ticker': ticker, 'timestamp': b.time, 'open': b.open, 'high': b.high, 'low': b.low,
                             'close': b.close, 'volume': b.volume, 'trading_value': b.amount} for b in chart_day.bars]
                df_day = pd.DataFrame(data_day)

                if last_ts_day: df_day = df_day[df_day['timestamp'] > last_ts_day]

                if not df_day.empty:
                    df_day = calculate_indicators(df_day)
                    # [개선] 보조지표 계산 시 발생하는 초기 NaN 값 제거
                    df_day.dropna(inplace=True)
                    db.insert_data(df_day, table_day)
                    logger.info(f"'{ticker}': 일봉 {len(df_day)}개 신규 데이터 저장")

        except Exception as e:
            if "Duplicate entry" not in str(e):
                logger.error(f"'{ticker}' 데이터 수집 중 오류 발생: {e}", exc_info=True)


def main():
    """메인 실행 함수"""
    global logger
    logger = setup_logger()

    try:
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')

        db_handler = DBHandler(config['DATABASE'])
        kis_api = KISApi(config['API'])

        # [개선] 설정 파일에서 국내/해외 종목 코드를 별도로 읽어와서 통합
        domestic_tickers = config['TRADING'].get('domestic_tickers', '').split(',')
        overseas_tickers = config['TRADING'].get('overseas_tickers', '').split(',')
        tickers = [t.strip() for t in domestic_tickers + overseas_tickers if t.strip()]

        if not tickers:
            logger.warning("설정 파일에 분석할 종목(tickers)이 지정되지 않았습니다.")
            return

        run_mode = config['TRADING'].get('run_mode', 'collect')

        # 데이터 수집 스케줄 등록 (항상 실행)
        schedule.every(1).minutes.do(collect_data_job, db_handler, kis_api, tickers)

        logger.info("초기 데이터 수집을 시작합니다.")
        collect_data_job(db_handler, kis_api, tickers)

        if run_mode == 'train':
            logger.info("모델 훈련 모드로 실행합니다. 훈련 완료 후 데이터 수집만 계속됩니다.")
            trainer = ModelTrainer(db_handler, config['TRADING'])
            trainer.train()
        elif run_mode == 'trade':
            logger.info("자동 매매/알림 모드로 실행합니다.")
            trader = Trader(kis_api, db_handler, config['TRADING'])
            schedule.every(5).minutes.do(trader.run)
        else:  # collect 모드
            logger.info("데이터 수집 모드로 실행합니다.")

        logger.info(f"'{run_mode}' 모드 설정 완료. 데이터 수집은 1분마다 주기적으로 실행됩니다.")

        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as e:
        logger.critical("프로그램 실행 중 심각한 오류가 발생하여 종료합니다.", exc_info=True)


if __name__ == "__main__":
    main()