from pykis import PyKis, KisAuth
import logging

class KISApi:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        try:
            auth = KisAuth(
                id=config.get('id', None),
                appkey=config['appkey'],
                secretkey=config['secretkey'],
                account=config['account_number'],
                virtual=config.getboolean('virtual_trade')
            )
            self.kis = PyKis(auth, keep_token=True)
            self.logger.info("KIS API 인증 성공")
        except Exception as e:
            self.logger.critical(f"KIS API 인증 실패: {e}")
            raise

    def get_daily_chart(self, ticker, start_date=None, end_date=None):
        """일봉 데이터를 가져옵니다."""
        try:
            stock = self.kis.stock(ticker)
            return stock.daily_chart(start=start_date, end=end_date)
        except Exception as e:
            self.logger.error(f"'{ticker}' 일봉 데이터 조회 실패: {e}")
            return None

    def get_day_chart(self, ticker, start_time=None, end_time=None):
        """분봉 데이터를 가져옵니다."""
        try:
            stock = self.kis.stock(ticker)
            return stock.day_chart(start=start_time, end=end_time)
        except Exception as e:
            self.logger.error(f"'{ticker}' 분봉 데이터 조회 실패: {e}")
            return None

    def get_balance(self):
        """계좌 잔고를 조회합니다."""
        try:
            return self.kis.account().balance()
        except Exception as e:
            self.logger.error(f"잔고 조회 실패: {e}")
            return None

    def place_order(self, ticker, order_type, quantity, price=None, condition=None):
        """매수/매도 주문을 실행합니다."""
        try:
            stock = self.kis.stock(ticker)
            if order_type.lower() == 'buy':
                return stock.buy(qty=quantity, price=price, condition=condition)
            elif order_type.lower() == 'sell':
                return stock.sell(qty=quantity, price=price, condition=condition)
            else:
                self.logger.warning(f"잘못된 주문 타입: {order_type}")
                return None
        except Exception as e:
            self.logger.error(f"'{ticker}' {order_type} 주문 실패: {e}")
            return None