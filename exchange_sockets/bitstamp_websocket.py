from exchange_sockets.exchange_websocket import ExchangeWebSocket
from singletones.custom_logger import MyLogger
import websocket
import threading
from time import sleep
from time import time
import json
import ssl

logger = MyLogger()


class BitstampWebsocket(ExchangeWebSocket):

    def __init__(self, pairs_n_streams):
        super().__init__('Bitstamp', pairs_n_streams)
        self.possible_streams = ['live_trades', 'diff_order_book']
        self.streams = []

    def init_streams(self):
        for pair, streams in self.pairs_n_streams.items():
            for sub_stream in streams.split(','):
                if self.has_stream(sub_stream):
                    cur = dict()
                    cur['event'] = 'bts:subscribe'
                    cur['data'] = {'channel': "{}_{}".format(sub_stream, pair)}

                    self.streams.append(cur)

    def start_multiple_websocket(self, init_streams=True):
        super().start_multiple_websocket(init_streams=init_streams)
        websocket.enableTrace(True)

        self.ws = websocket.WebSocketApp("wss://ws.bitstamp.net",
                                         on_open=self.__on_open,
                                         on_message=self.__on_message,
                                         on_error=self.__on_error,
                                         on_close=self.__on_close)

        self.wst = threading.Thread(target=lambda: self.ws.run_forever(sslopt={'cert_reqs': ssl.CERT_NONE}))
        self.wst.daemon = True
        self.wst.start()
        logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 15
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            sleep(1)
            conn_timeout -= 1

        if not conn_timeout:
            logger.error("%s Couldn't connect to %s! Exiting.",
                         self.node,
                         self.exchange)
            self.close_socket()
        else:
            logger.info('{} socket is started:\n{}\n{}'.format(self.exchange,
                                                                    self.node,
                                                                    str(self.streams)))

    def save_trades(self, message):
        data = message['data']
        channel = message['channel']
        symbol = channel.split('_')[-1]
        stream = channel[:-(len(symbol) + 1)]

        append_data = "{},{},{},{}\n".format(data['timestamp'],
                                             data['price'],
                                             data['amount'],
                                             data['type'])

        self.file_manager.save_data_to_file(self.exchange,
                                            stream,
                                            symbol,
                                            append_data)

    def save_level2_orderbook(self, message):
        data = message['data']
        channel = message['channel']
        symbol = channel.split('_')[-1]
        stream = channel[:-(len(symbol) + 1)]

        all_data = {}
        data_time = data['timestamp']

        for side in ['bids', 'asks']:
            for cur in data[side]:
                if not all_data.get(symbol, None):
                    all_data[symbol] = []

                price = cur[0]
                size = cur[1]

                all_data[symbol].append("{},{},{}\n".format(
                    data_time,
                    price,
                    size if side == "bids" else "-{}".format(size)))

        for symbol, l2_ob_data in all_data.items():
            for l2_ob in l2_ob_data:
                self.file_manager.save_data_to_file(self.exchange,
                                                    stream,
                                                    symbol,
                                                    l2_ob)

    def __on_message(self, ws, message):
        if message is None:
            return

        try:
            self.last_msg_time = int(time())
            message = json.loads(message)

            channel = message['channel']

            if channel.startswith('diff_order_book'):
                self.save_level2_orderbook(message)
            elif channel.startswith('live_trades'):
                self.save_trades(message)

        except Exception as e:
            logger.debug(str(e))

    def __on_error(self, ws, error):
        self.on_error = True
        logger.error("On error\n{}\n{} {}".format(self.node,
                                                  self.exchange,
                                                  error))

    def __on_close(self, ws):
        logger.info("On close\n{}".format(self.exchange))

    def __on_open(self, ws):
        logger.info("On Open\n{}".format(self.exchange))

        if self.streams:
            for stream in self.streams:
                logger.info('Subscribing to %s', json.dumps(stream))
                self.ws.send(json.dumps(stream))
                sleep(2)
        else:
            logger.error('%s. Stream is not initialized', self.exchange)

    def close_socket(self):
        self.exited = True
        if self.ws:
            self.ws.close()
