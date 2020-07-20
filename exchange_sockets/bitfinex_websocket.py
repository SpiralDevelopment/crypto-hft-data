import websocket
import json
from singletones.custom_logger import MyLogger
import threading
from time import sleep
import time
from exchange_sockets.exchange_websocket import ExchangeWebSocket

logger = MyLogger()


class BitfinexWebSocket(ExchangeWebSocket):
    def __init__(self, stream_n_pairs):
        super().__init__('Bitfinex', stream_n_pairs)
        self.ws = None
        self.possible_streams = ['book', 'trades']
        self.streams = []
        self.message_bfr = {}
        self.msg_cnt_idx = {'book': 1, 'trades': 2}

    def init_streams(self):
        for pair, stream in self.pairs_n_streams.items():
            if self.has_stream(stream):
                for sub_stream in stream.split(','):
                    self.streams.append(json.dumps({"event": "subscribe",
                                                    "channel": sub_stream,
                                                    "pair": pair}))
            else:
                logger.warning('%s. There is no "%s" stream',
                               pair, stream)

    def save_data_to_file(self, channel, pair, data):
        self.file_manager.save_data_to_file(self.exchange,
                                            channel,
                                            pair,
                                            data)

    def __on_message(self, ws, message):
        # print(message)
        if message is None:
            return

        try:
            self.last_msg_time = int(time.time())
            message = eval(message)

            if isinstance(message, list):
                chan_id = message[0]

                cur_channel = self.message_bfr[chan_id]['channel']
                can_insert = True

                if cur_channel == 'trades' and message[1] != 'te':
                    can_insert = False

                if can_insert:
                    append_msg = str(message[self.msg_cnt_idx[cur_channel]])
                    append_msg = append_msg.replace(' ', '').replace('[', '').replace(']', '')

                    if append_msg != "hb":
                        if cur_channel == 'book':
                            append_msg = "{0},{1}".format(time.time(), append_msg)
                        elif cur_channel == 'trades':
                            first_comma_idx = append_msg.find(' ')
                            append_msg = append_msg[first_comma_idx + 1:]

                        self.message_bfr[chan_id][cur_channel] += "{0}\n".format(append_msg)
                        self.message_bfr[chan_id][cur_channel + '_count'] += 1

                        if self.message_bfr[chan_id][cur_channel + '_count'] > 30:
                            self.save_data_to_file(cur_channel,
                                                   self.message_bfr[chan_id]['pair'],
                                                   self.message_bfr[chan_id][cur_channel])

                            self.message_bfr[chan_id][cur_channel] = ''
                            self.message_bfr[chan_id][cur_channel + '_count'] = 0
            elif isinstance(message, dict):
                logger.info(message)

                if 'chanId' in message:
                    chan_id = message['chanId']

                    del message['chanId']
                    del message['event']

                    self.message_bfr[chan_id] = message

                    for strm in self.possible_streams:
                        self.message_bfr[chan_id][strm] = ''
                        self.message_bfr[chan_id][strm + '_count'] = 0
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
                self.ws.send(stream)
        else:
            logger.error('Bitmex. Streams are not initialized')

    def close_socket(self):
        self.exited = True
        if self.ws:
            self.ws.close()

    def start_multiple_websocket(self, init_streams=True):
        super().start_multiple_websocket(init_streams=init_streams)

        websocket.enableTrace(True)

        self.ws = websocket.WebSocketApp("wss://api.bitfinex.com/ws/2",
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
                                                               '\n'.join(self.streams)))
