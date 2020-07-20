from exchange_sockets.exchange_websocket import ExchangeWebSocket
from singletones.custom_logger import MyLogger
import websocket
import threading
from time import sleep
from time import time
import json

logger = MyLogger()


class KrakenWebsocket(ExchangeWebSocket):

    def __init__(self, stream_n_pairs):
        super().__init__('Kraken', stream_n_pairs)
        self.possible_streams = ['book', 'trade']
        self.streams = []
        self.message_bfr = {}

    def init_streams(self):
        channels = {}

        for pair, stream in self.pairs_n_streams.items():
            for sub_stream in stream.split(','):
                if self.has_stream(sub_stream):
                    if not channels.get(sub_stream, None):
                        channels[sub_stream] = []

                    channels[sub_stream].append(pair)
                else:
                    logger.warning('%s. There is no "%s" stream',
                                   pair, stream)

        if len(channels) > 0:
            for stream, pairs in channels.items():
                if stream == "book":
                    self.streams.append({
                        'event': 'subscribe',
                        'subscription': {'name': stream, "depth": 25},
                        'pair': pairs
                    })
                else:
                    self.streams.append({
                        'event': 'subscribe',
                        'subscription': {'name': stream},
                        'pair': pairs
                    })

    def start_multiple_websocket(self, init_streams=True):
        super().start_multiple_websocket(init_streams=init_streams)
        websocket.enableTrace(True)

        self.ws = websocket.WebSocketApp("wss://ws.kraken.com",
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
                                                                    self.streams))

    def __on_message(self, ws, message):
        if message is None:
            return

        try:
            try:
                self.last_msg_time = int(time())
                message = eval(message)

                if isinstance(message, list):
                    chan_id = message[0]
                    data = message[1]

                    stream = self.message_bfr[chan_id]['subscription']['name']
                    can_insert = True

                    if stream == 'book' and 'as' in data:
                        can_insert = False

                    if can_insert:
                        append_msg = ""

                        if stream == 'book':
                            if 'b' in data:
                                for bid in data['b']:
                                    append_msg += "{},{},{}\n".format(bid[2], bid[0], bid[1])

                            if 'a' in data:
                                for bid in data['a']:
                                    append_msg += "{},{},-{}\n".format(bid[2], bid[0], bid[1])
                        elif stream == 'trade':
                            for trade in data:
                                append_msg += ",".join(trade[:5])
                                append_msg += "\n"

                        if append_msg:
                            self.message_bfr[chan_id][stream] += append_msg
                            self.message_bfr[chan_id][stream + '_count'] += 1

                            if self.message_bfr[chan_id][stream + '_count'] > 0:
                                self.file_manager.save_data_to_file(self.exchange,
                                                                    stream,
                                                                    self.message_bfr[chan_id]['pair'].replace('/', ''),
                                                                    self.message_bfr[chan_id][stream])

                                self.message_bfr[chan_id][stream] = ''
                                self.message_bfr[chan_id][stream + '_count'] = 0
                elif isinstance(message, dict):
                    logger.info(message)

                    if 'channelID' in message:
                        chan_id = message['channelID']

                        self.message_bfr[chan_id] = message

                        for strm in self.possible_streams:
                            self.message_bfr[chan_id][strm] = ''
                            self.message_bfr[chan_id][strm + '_count'] = 0
            except Exception as e:
                logger.debug(str(e))
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
