import concurrent
import itertools
import json
import time
import pandas as pd
import pandas_ta as pta
from apscheduler.schedulers.background import BackgroundScheduler
from rich.console import Console
from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import asyncio
import websockets
from src.binance_wrapper import BinanceAPI

console = Console()
client = BinanceAPI()
symbols = client.get_symbols()
timeframes = ['1m', '5m', '15m', '1h', '4h']
candle_dict: [str, pd.DataFrame] = {}
clients = set()
data = {}


def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


def fetch_kline(symbol, period='1m', limit=720):
    response = client.get_klines(symbol, period, limit)
    df = pd.DataFrame(response)
    # df['datetime'] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).map(lambda x: x.tz_convert("Europe/Lisbon"))
    df.set_index('timestamp', inplace=True)
    return df


def add_new_kline_candle(key, data):
    candle_dict[key].loc[data[0][0]] = [
        float(data[0][1]),
        float(data[0][2]),
        float(data[0][3]),
        float(data[0][4]),
        float(data[0][5]),
        float(data[0][6])
    ]


def apply_klines_indicators(key):
    df = candle_dict[key].copy()
    df['CHANGE'] = df['close'].pct_change(periods=1) * 100
    df['RSI'] = pta.rsi(df['close'], 14)

    last_row = df.iloc[-1]
    data[key] = {
        'close': last_row['close'],
        'change': last_row['CHANGE'],
        'rsi': last_row['RSI'],
    }
    return df


def start_socket():
    channels = [f'kline_{timeframe} ' for timeframe in timeframes]
    chunks = list(split_seq(symbols, 20))
    ubwa = BinanceWebSocketApiManager(exchange="binance.com-futures", output_default="UnicornFy", high_performance=True, warn_on_update=True, ping_timeout_default=3)
    for chunk in chunks:
        ubwa.create_stream(['kline_1m', 'kline_5m', 'kline_15m', 'kline_1h', 'kline_4h'], chunk, process_stream_data=process_socket_event)


def initialize():
    def load_candles(symbol: str):
        for timeframe in timeframes:
            candle_dict[f'{symbol}_{timeframe}'] = fetch_kline(symbol, period=timeframe)
        console.print(f"✅ Loaded {symbol}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=9) as schedule_executor:
        schedule_executor.map(load_candles, symbols)


def process_socket_event(stream):
    if stream and len(stream) > 3:
        if stream['stream_type'] and stream['event_type'] == 'kline':
            open_time_int = int(stream['kline']['kline_start_time'])
            symbol = stream['symbol']
            interval = stream['kline']['interval']
            if f'{symbol}_{interval}' in candle_dict:
                new_kline = [[
                    open_time_int,
                    stream['kline']['open_price'],
                    stream['kline']['high_price'],
                    stream['kline']['low_price'],
                    stream['kline']['close_price'],
                    stream['kline']['base_volume'],
                    stream['kline']['number_of_trades']
                ]]
                add_new_kline_candle(f'{symbol}_{interval}', new_kline)


def schedule():
    start_time = time.time()
    for key in candle_dict.keys():
        apply_klines_indicators(key)

    async def send():
        await broadcast(json.dumps(data))
        console.log(f'Broadcasted, time elapsed: {time.time() - start_time}s')

    asyncio.run(send())


async def start(websocket):
    clients.add(websocket)
    console.log('[SYS] New client has been connected')
    try:
        while True:
            await asyncio.sleep(1)
    finally:
        clients.remove(websocket)
        console.log('[SYS] Client has been removed')


async def broadcast(message):
    for websocket in clients.copy():
        try:
            await websocket.send(message)
        except websockets.ConnectionClosed:
            pass


async def main():
    async with websockets.serve(start, "localhost", 8765):
        await asyncio.Future()  # run forever


start_socket()
initialize()
scheduler = BackgroundScheduler()
scheduler.add_job(schedule, 'cron', second='*/10', timezone='Europe/Lisbon')
scheduler.start()
asyncio.run(main())
while True:
    time.sleep(1)
