from exchange_sockets.exchange_websocket import ExchangeWebSocket
from singletones.custom_logger import MyLogger
import websocket
import threading
from time import sleep
from time import time
import json

logger = MyLogger()


class CoinbaseWebsocket(ExchangeWebSocket):
    def __init__(self, stream_n_pairs):
        super().__init__('Coinbase', stream_n_pairs)
        self.possible_streams = ['level2', 'matches']
        self.stream = {}
        self.message_bfr = {}
        self.channel_by_type = {'l2update': 'level2',
                                'match': 'matches'}

    def init_streams(self):
        channels = {}

        for pair, stream in self.pairs_n_streams.items():
            self.init_buffer(pair)

            for sub_stream in stream.split(','):
                if self.has_stream(sub_stream):
                    if not channels.get(sub_stream, None):
                        channels[sub_stream] = []

                    channels[sub_stream].append(pair)
                else:
                    logger.warning('%s. There is no "%s" stream',
                                   pair, stream)

        if len(channels) > 0:
            chls = []

            for key, item in channels.items():
                chls.append({
                    'name': key,
                    'product_ids': item
                })

            self.stream = json.dumps({'type': 'subscribe',
                                      'channels': chls})

    def init_buffer(self, pair):
        self.message_bfr[pair] = {}

        for strm in self.possible_streams:
            self.message_bfr[pair][strm] = ''
            self.message_bfr[pair][strm + '_count'] = 0

    def start_multiple_websocket(self, init_streams=True):
        super().start_multiple_websocket(init_streams=init_streams)
        websocket.enableTrace(True)

        self.ws = websocket.WebSocketApp("wss://ws-feed.pro.coinbase.com/",
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

    def append_msg_to_buffer(self, pair, channel, msg, msg_max_count=30):
        self.message_bfr[pair][channel] += msg
        self.message_bfr[pair][channel + '_count'] += 1

        if self.message_bfr[pair][channel + '_count'] > msg_max_count:
            self.file_manager.save_data_to_file(self.exchange,
                                                channel,
                                                pair,
                                                self.message_bfr[pair][channel])

            self.message_bfr[pair][channel] = ''
            self.message_bfr[pair][channel + '_count'] = 0

    def __on_message(self, ws, message):
        if message is None:
            return

        try:
            self.last_msg_time = int(time())
            message = eval(message)
            pair = message['product_id']
            channel = self.channel_by_type[message['type']]
            msg_time = message['time']

            if channel == 'level2':
                changes = message['changes'][0]
                price = changes[1]
                qty = changes[2]

                if changes[0] == 'sell' and qty != '0':
                    qty = '-' + qty

                self.append_msg_to_buffer(pair,
                                          channel,
                                          "{},{},{}\n".format(msg_time,
                                                              price,
                                                              qty),
                                          msg_max_count=50)

            elif channel == 'matches':
                self.append_msg_to_buffer(pair,
                                          channel,
                                          "{},{},{},{}\n".format(message['time'],
                                                              message['side'],
                                                              message['size'],
                                                              message['price']),
                                          msg_max_count=3)

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
            self.ws.send(self.stream)
        else:
            logger.error('%s. Stream is not initialized', self.exchange)

    def close_socket(self):
        self.exited = True
        if self.ws:
            self.ws.close()
