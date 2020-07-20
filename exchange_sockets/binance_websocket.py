from binance.websockets import BinanceSocketManager
from binance.client import Client
import time
from singletones.custom_logger import MyLogger
from exchange_sockets.exchange_websocket import ExchangeWebSocket

logger = MyLogger()


class BinanceWebSocket(ExchangeWebSocket):
    def __init__(self, pairs_n_streams):
        super().__init__('Binance', pairs_n_streams)

        self.client = Client('', '')
        self.bm = BinanceSocketManager(self.client)
        self.streams = []
        self.possible_streams = ['trade', 'depth20', 'arr']

    def init_streams(self):
        for pair, stream in self.pairs_n_streams.items():
            if self.has_stream(stream):

                for sub_stream in stream.split(','):
                    self.streams.append('{0}@{1}'.format(pair,
                                                         sub_stream))
            else:
                logger.warning('%s. There is no %s stream',
                               pair, stream)

        logger.info('%s streams are initialized: %s',
                    len(self.streams),
                    self.streams)

    def close_socket(self):
        logger.debug('Closing socket')
        self.exited = True
        self.bm.close()

    def start_multiple_websocket(self, init_streams=True):
        super().start_multiple_websocket(init_streams=init_streams)

        if len(self.streams) > 0:
            self.bm.start_multiplex_socket(self.streams,
                                           self.multiple_data_handle)

            self.bm.start()

            logger.info('{} socket is started:\n{}\n{}'
                             .format(self.exchange,
                                     self.node,
                                     '\n'.join(self.streams)))

    def save_depth_to_file(self, msg):
        stream = msg.get('stream', '')
        stream_parts = stream.split('@')
        pair = stream_parts[0]
        data = msg.get('data')

        last_update_id = data.get('lastUpdateId', 0)

        if last_update_id >= self.depth_last_update.get(pair, 0):
            self.depth_last_update[pair] = last_update_id
            bids = data.get('bids', [])
            asks = data.get('asks', [])

            if len(bids) == 0 and len(asks) == 0:
                bids = data.get('b', [])
                asks = data.get('a', [])

            if len(bids) > 0 or len(asks) > 0:
                time_now = int(time.time())
                data_to_append = ["{},{},{},{}\n".format(time_now, 'B', bid[0], bid[1]) for bid in bids] + \
                                 ["{},{},{},{}\n".format(time_now, 'A', ask[0], ask[1]) for ask in asks]

                data_to_append = "".join(data_to_append)

                self.file_manager.save_data_to_file(self.exchange,
                                                    stream_parts[1],
                                                    pair,
                                                    data_to_append,
                                                    columns='time,side,price,qty')
            else:
                if len(bids) == 0:
                    logger.debug('%s. Bids are empty', pair)

                if len(asks) == 0:
                    logger.debug('%s. Asks are empty', pair)
        else:
            logger.debug('%s. last_update_id is whether none or not new', pair)

    def save_trade_to_file(self, msg):
        stream = msg.get('stream', '')
        stream_parts = stream.split('@')
        pair = stream_parts[0]
        data = msg.get('data')

        del data['e']
        del data['s']

        data_to_insert = "{},{},{},{},{},{},{},{},{}\n".format(data['m'],
                                                               data['E'],
                                                               data['M'],
                                                               data['q'],
                                                               data['T'],
                                                               data['t'],
                                                               data['b'],
                                                               data['p'],
                                                               data['a'])

        self.file_manager.save_trades_to_file(self.exchange,
                                              stream_parts[1],
                                              pair,
                                              data_to_insert, columns="m,E,M,q,T,t,b,p,a")

    def save_min_ticker_to_file(self, msg):
        stream = msg.get('stream', '')
        stream_parts = stream.split('@')
        pair = stream_parts[0]
        data = msg.get('data')

        res = ''
        for cur in data:
            res += "{0},{1},{2},{3},{4}," \
                   "{5},{6},{7},{8}\n".format(cur['e'],
                                              cur['E'],
                                              cur['s'],
                                              cur['c'],
                                              cur['o'],
                                              cur['h'],
                                              cur['l'],
                                              cur['v'],
                                              cur['q'])

        self.file_manager.save_trades_to_file(self.exchange,
                                              stream_parts[1],
                                              pair,
                                              res, columns='e,E,s,c,o,h,l,v,q')

    def multiple_data_handle(self, msg):
        stream = msg.get('stream', '')

        try:
            if 'depth' in stream:
                self.save_depth_to_file(msg)
            elif 'trade' in stream:
                self.save_trade_to_file(msg)
            elif '!miniTicker' in stream:
                self.save_min_ticker_to_file(msg)
            else:
                logger.debug('%s data handler not implemented', stream)

            self.last_msg_time = int(time.time())
        except Exception as e:
            logger.error(str(e))
