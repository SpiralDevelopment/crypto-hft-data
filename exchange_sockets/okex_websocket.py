from exchange_sockets.exchange_websocket import ExchangeWebSocket
from singletones.custom_logger import MyLogger
import websocket
import threading
from time import sleep
from time import time
import json
import zlib

logger = MyLogger()


class OkexWebsocket(ExchangeWebSocket):

    def __init__(self, stream_n_pairs):
        super().__init__('Okex', stream_n_pairs)
        self.possible_streams = ['depth', 'trade']
        self.stream = {}

    def init_streams(self):
        streams_list = []

        for pair, streams in self.pairs_n_streams.items():
            for sub_stream in streams.split(','):
                if self.has_stream(sub_stream):
                    streams_list.append("spot/{}:{}".format(sub_stream, pair))

        self.stream['op'] = 'subscribe'
        self.stream['args'] = streams_list

    def start_multiple_websocket(self, init_streams=True):
        super().start_multiple_websocket(init_streams=init_streams)
        websocket.enableTrace(True)

        self.ws = websocket.WebSocketApp("wss://real.okex.com:8443/ws/v3",
                                         on_open=self.__on_open,
                                         on_message=self.__on_message,
                                         on_error=self.__on_error,
                                         on_close=self.__on_close)

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
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
                                                                    self.stream))

    def inflate(self, data):
        decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
        )
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated

    def save_trade(self, message):
        data_to_append = {}
        stream = message['table'].split('/')[1]

        for data in message['data']:
            symbol = data['instrument_id']
            time_stamp = data['timestamp']
            append_msg = "{},{},{},{}\n".format(time_stamp,
                                                data['price'],
                                                data['size'],
                                                data['side'][0])
            if not data_to_append.get(symbol, None):
                data_to_append[symbol] = []

            data_to_append[symbol].append(append_msg)

        for symbol, append_msgs in data_to_append.items():
            append_msg = "".join(append_msgs)

            self.file_manager.save_data_to_file(self.exchange,
                                                stream,
                                                symbol,
                                                append_msg)

    def save_level2_orderbook(self, message):
        stream = message['table'].split('/')[1]
        data_to_append = {}

        for data in message['data']:
            symbol = data['instrument_id']
            time_stamp = data['timestamp']
            append_msg = ""

            for ask in data['asks']:
                append_msg += "{},{},-{},{}\n".format(time_stamp, ask[0], ask[1], ask[2])

            for ask in data['bids']:
                append_msg += "{},{},{},{}\n".format(time_stamp, ask[0], ask[1], ask[2])

            if not data_to_append.get(symbol, None):
                data_to_append[symbol] = []

            data_to_append[symbol].append(append_msg)

        for symbol, append_msgs in data_to_append.items():
            append_msg = "".join(append_msgs)

            self.file_manager.save_data_to_file(self.exchange,
                                                stream,
                                                symbol,
                                                append_msg)

    def __on_message(self, ws, message):
        if message is None:
            return

        try:
            self.last_msg_time = int(time())
            message = self.inflate(message)
            message = json.loads(message.decode("utf-8"))

            if message['table'] == 'spot/depth':
                if message['action'] == 'update':
                    self.save_level2_orderbook(message)
            elif message['table'] == 'spot/trade':
                self.save_trade(message)

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

        if self.stream:
            self.ws.send(json.dumps(self.stream))
        else:
            logger.error('%s. Stream is not initialized', self.exchange)

    def close_socket(self):
        self.exited = True
        if self.ws:
            self.ws.close()
