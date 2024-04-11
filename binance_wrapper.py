from binance.um_futures import UMFutures
from dataclasses import dataclass


@dataclass
class Position:
    symbol: str
    entry_price: float
    market_price: float
    size: float

    def unrealized_profit(self):
        if self.entry_price is None or self.market_price is None:
            return "Entry price or market price is missing."
        return self.market_price - self.entry_price

    def position_side(self):
        if self.size > 0:
            return 'LONG'
        if self.size < 0:
            return 'SHORT'
        return None

    def __getitem__(self, item):
        return getattr(self, item)


@dataclass
class Order:
    symbol: str
    order_id: int
    client_order_id: str
    type: str
    side: str
    stop_price: float

    def __getitem__(self, item):
        return getattr(self, item)


class BinanceAPI:
    def __init__(self, api_key='', api_secret=''):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = UMFutures(key=self.api_key, secret=self.api_secret)

    def get_symbols(self):
        symbols = self.client.exchange_info()['symbols']
        return [symbol['symbol'] for symbol in symbols if symbol['status'] == 'TRADING' and symbol['quoteAsset'] == 'USDT' and symbol['contractType'] == 'PERPETUAL' and symbol['symbol'] != 'USDCUSDT']

    def get_klines(self, symbol: str, interval='1m', limit=1440):
        return [{
            'timestamp': kline[0],
            'open': float(kline[1]),
            'high': float(kline[2]),
            'low': float(kline[3]),
            'close': float(kline[4]),
            'volume': float(kline[5]),
            'trades': float(kline[8])
        } for kline in self.client.klines(symbol=symbol, interval=interval, **{'limit': limit})]

    def get_open_interest_hist(self, symbol: str, period='5m', limit=288):
        return [{'timestamp': item['timestamp'], 'oiv': float(item['sumOpenInterestValue'])} for item in self.client.open_interest_hist(symbol=symbol, period=period, **{'limit': limit})]

    def get_long_short_ratio(self, symbol: str, period='5m', limit=288):
        return [{'timestamp': item['timestamp'], 'lsrv': float(item['longShortRatio'])} for item in self.client.long_short_account_ratio(symbol=symbol, period=period, **{'limit': limit})]

    def get_positions(self):
        response = self.client.get_position_risk()
        positions = []
        for item in response:
            if float(item['positionAmt']) != 0:
                positions.append(Position(symbol=item['symbol'], entry_price=float(item['entryPrice']), market_price=float(item['markPrice']), size=float(item['positionAmt'])))
        return positions

    def get_orders(self):
        response = self.client.get_orders()
        orders = []
        for item in response:
            orders.append(Order(symbol=item['symbol'], order_id=item['orderId'], client_order_id=item['clientOrderId'], type=item['type'], side=item['side'], stop_price=float(item['stopPrice'])))
        return orders
