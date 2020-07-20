import argparse
import os
import time

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from exchange_sockets.binance_websocket import BinanceWebSocket
from exchange_sockets.bitmex_websocket import BitMexWebSocket
from exchange_sockets.bitfinex_websocket import BitfinexWebSocket
from exchange_sockets.bitstamp_websocket import BitstampWebsocket
from exchange_sockets.coinbase_websocket import CoinbaseWebsocket
from exchange_sockets.kraken_websocket import KrakenWebsocket
from exchange_sockets.okex_websocket import OkexWebsocket
from helpers.helper import *

logger = MyLogger()
node = platform.node()


def start_exchange_socket(exchange_mgr):
    exchange = exchange_mgr.exchange
    exchange_mgr.start_multiple_websocket()

    logger.info('Max delay: %s', exchange_mgr.max_delay)

    while not exchange_mgr.exited:
        diff = int(time.time()) - exchange_mgr.last_msg_time

        if exchange_mgr.on_error:
            exchange_mgr.start_multiple_websocket(init_streams=False)
        elif diff >= exchange_mgr.max_delay:
            # There was no response from web socket since $max_delay seconds
            # Rerun the script if this happens
            exchange_mgr.close_socket()
            logger.warning('%s %s Connection is not established.',
                           node,
                           exchange)
            break
        else:
            logger.info('%s Connection is well established. Diff: %s',
                        exchange,
                        diff)

        time.sleep(10)


def init(exchange):
    try:
        pairs_n_streams = get_streams(exchange)

        if pairs_n_streams:
            exchange_mgr = None

            if exchange == 'binance':
                exchange_mgr = BinanceWebSocket(pairs_n_streams)
            elif exchange == 'bitmex':
                exchange_mgr = BitMexWebSocket(pairs_n_streams)
            elif exchange == 'bitfinex':
                exchange_mgr = BitfinexWebSocket(pairs_n_streams)
            elif exchange == 'coinbase':
                exchange_mgr = CoinbaseWebsocket(pairs_n_streams)
            elif exchange == 'bitstamp':
                exchange_mgr = BitstampWebsocket(pairs_n_streams)
            elif exchange == 'kraken':
                exchange_mgr = KrakenWebsocket(pairs_n_streams)
            elif exchange == 'okex':
                exchange_mgr = OkexWebsocket(pairs_n_streams)

            if exchange_mgr:
                start_exchange_socket(exchange_mgr)
            else:
                logger.info('No exchange mgr found.\n%s\n%s', node, exchange)
        else:
            logger.info('No streams are found. %s. %s', node, exchange)
    except Exception as e:
        logger.error(str(e))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='App description')

    parser.add_argument('-e',
                        '--exchange',
                        type=str,
                        help='Exchange',
                        required=True)
    args = vars(parser.parse_args())
    logger.info(str(args))

    init(args['exchange'].lower())
