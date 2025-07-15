import configparser
import logging
import sys
import json
import pandas as pd
from datetime import datetime, timedelta

# --- Step 1: 라이브러리 임포트 및 로거 설정 ---
try:
    from pykis import PyKis, KisAuth

    print("성공: 'pykis' 라이브러리를 성공적으로 불러왔습니다.")
except ImportError:
    # 구버전 호환성 유지
    from pykis import pykis as PyKis, KisAuth

    print("성공: 'pykis' (구버전) 라이브러리를 성공적으로 불러왔습니다.")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("APITest")


def test_api_calls():
    """국내 주식의 과거 분봉 데이터 조회를 테스트합니다."""
    try:
        # --- Step 2: 설정 파일 읽기 및 API 초기화 ---
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')
        api_config = config['API']
        trading_config = config['TRADING']
        logger.info("config.ini 파일 읽기 성공.")

        auth = KisAuth(
            appkey=api_config['appkey'],
            secretkey=api_config['secretkey'],
            id=api_config.get('id'),
            account=api_config['account_number'],
            virtual=config.getboolean('API', 'virtual_trade')
        )

        kis = PyKis(auth)
        logging.info("KIS API 인증 및 초기화 성공.")

        # --- 국내 주식 테스트 ---
        # domestic_ticker = trading_config.get('domestic_tickers', '').split(',')[0].strip()
        domestic_ticker = "005930"
        if domestic_ticker:
            # [수정] 조회 날짜를 2024년 7월 15일로 고정
            test_domestic_chart(kis, domestic_ticker, "20240715")
        else:
            logger.warning("설정 파일에 국내 주식 종목이 없어 테스트를 건너뜁니다.")

    except Exception as e:
        logger.critical("테스트 실행 중 예외가 발생했습니다.", exc_info=True)
        sys.exit(1)


def test_domestic_chart(kis: PyKis, ticker: str, target_date_str: str):
    """국내 주식 과거 분봉 조회 테스트 (API 직접 호출)"""
    logger.info(f"--- 국내 주식 테스트 시작 (종목: {ticker}, 날짜: {target_date_str}) ---")

    try:
        response = kis.fetch(
            path="/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            api="FHKST03010200",
            params={
                "FID_ETC_CLS_CODE": "", "FID_COND_MRKT_DIV_CODE": "UN", "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": target_date_str, "FID_INPUT_DATE_2": target_date_str,
                "FID_PERIOD_DIV_CODE": "M", "FID_ORG_ADJ_PRC": "0"
            }
        )

        print(response)

        if response and response.output2:
            logger.info(f"성공! >>> 국내 주식 분봉 데이터 {len(response.output2)}건을 가져왔습니다.")
            df = pd.DataFrame(response.output2)
            print(df[['stck_cntg_hour', 'stck_prpr', 'cntg_vol']].head())
        else:
            logger.error("실패! >>> 국내 주식 분봉 데이터를 가져오지 못했습니다.")
            if response:
                print(f"API 응답 메시지: {response.msg1}")
                print(f"전체 응답: {response.__data__}")  # 전체 응답 내용 확인
            else:
                print("API 응답이 없습니다 (None).")

    except Exception as e:
        logger.error(f"'{ticker}' 데이터 조회 중 예외 발생", exc_info=True)


if __name__ == "__main__":
    test_api_calls()