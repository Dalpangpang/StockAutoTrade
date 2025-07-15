import pymysql
import pandas as pd
from sqlalchemy import create_engine
import logging

class DBHandler:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.conn = None
        self.engine = None
        self.connect()

    def connect(self):
        """데이터베이스에 연결합니다."""
        try:
            self.conn = pymysql.connect(
                host=self.config['host'],
                user=self.config['user'],
                password=self.config['password'],
                db=self.config['database'],
                port=int(self.config['port']),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor # 결과를 딕셔너리로 받기 위해 추가
            )
            self.engine = create_engine(
                f"mysql+pymysql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
            self.logger.info("데이터베이스 연결 성공")
        except pymysql.Error as e:
            self.logger.critical(f"데이터베이스 연결 실패: {e}")
            self.logger.critical("DB 연결 정보를 확인하고, 데이터베이스와 테이블이 생성되었는지 확인해주세요.")
            raise

    def insert_data(self, df, table_name):
        """데이터를 지정된 테이블에 저장합니다."""
        if df.empty:
            return
        try:
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
        except Exception as e:
            self.logger.warning(f"{table_name} 데이터 저장 중 오류 발생 (중복 가능성): {e}")

    def get_last_timestamp(self, ticker, table_name):
        """특정 테이블에서 종목의 마지막 데이터 시간을 조회합니다."""
        query = f"SELECT MAX(timestamp) FROM {table_name} WHERE ticker = %s"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (ticker,))
                result = cursor.fetchone()
                # 딕셔너리에서 값 추출
                return result['MAX(timestamp)'] if result and result['MAX(timestamp)'] else None
        except pymysql.Error as e:
            self.logger.error(f"{table_name}에서 마지막 타임스탬프 조회 실패: {e}")
            return None

    def get_last_n_rows(self, ticker, table_name, n=40):
        """특정 테이블에서 종목의 마지막 N개 데이터를 조회하여 시간순으로 정렬된 DataFrame을 반환합니다."""
        query = f"(SELECT * FROM {table_name} WHERE ticker = %s ORDER BY timestamp DESC LIMIT %s)"
        try:
            # SQLAlchemy engine을 사용하여 데이터를 읽고, 시간순으로 다시 정렬
            df = pd.read_sql(query, self.engine, params=(ticker, n))
            if not df.empty:
                return df.sort_values(by='timestamp', ascending=True)
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"{table_name}에서 마지막 {n}개 데이터 조회 실패: {e}")
            return pd.DataFrame()