import asyncio
import itertools
import json
import websockets
from unicorn_binance_websocket_api import BinanceWebSocketApiManager
from src.binance_wrapper import BinanceAPI

connected_clients = set()
client = BinanceAPI()
symbols = client.get_symbols()


def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


async def forward_messages(websocket, path):
    connected_clients.add(websocket)
    try:
        while True:
            await asyncio.sleep(1)
    finally:
        connected_clients.remove(websocket)


manager = BinanceWebSocketApiManager(exchange="binance.com-futures", output_default="UnicornFy", high_performance=True, warn_on_update=True, ping_timeout_default=3)


def handle_message(message):
    for _client in connected_clients:
        if len(message) > 3:
            if message['stream_type'] and message['event_type'] == 'kline':
                asyncio.ensure_future(_client.send(json.dumps(message['kline'])))


chunks = list(split_seq(symbols, 20))
for chunk in chunks:
    manager.create_stream(['kline_1m', 'kline_5m', 'kline_15m', 'kline_1h', 'kline_4h'], chunk, process_stream_data=handle_message)


async def start_server():
    async with websockets.serve(forward_messages, 'localhost', 8765):
        await asyncio.Future()


# Explicitly create and run the event loop
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(start_server())
    except KeyboardInterrupt:
        pass
    finally:
        # Stop the WebSocket API manager and cleanup
        manager.stop_manager_with_all_streams()
        loop.close()
