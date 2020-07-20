from singletones.custom_logger import MyLogger
from managers.file_manager import FileManager
import platform
from time import time

logger = MyLogger()


class ExchangeWebSocket(object):
    def __init__(self, exchange, pairs_n_streams):
        if pairs_n_streams:
            self.pairs_n_streams = pairs_n_streams
        else:
            raise Exception('pairs and streams/channels are empty')

        self.exited = False
        self.on_error = False
        self.ws = None
        self.last_msg_time = 0
        self.exchange = exchange.lower()
        self.depth_last_update = {}
        self.file_manager = FileManager()
        self.file_manager.create_dirs(self.exchange, self.pairs_n_streams)
        self.possible_streams = []
        self.node = platform.node()
        self.max_delay = 90

    def start_multiple_websocket(self, init_streams=True):
        logger.info('Starting multiple websocket for %s', self.exchange)
        if init_streams:
            self.init_streams()

        self.on_error = False
        self.last_msg_time = int(time())

    def get_possible_streams(self):
        if len(self.possible_streams) > 0:
            return self.possible_streams
        else:
            raise NotImplementedError('Possible streams are not implemented')

    def has_stream(self, streams):
        for stream in streams.split(','):
            if stream not in self.get_possible_streams():
                return False

        return True

    def init_streams(self):
        raise NotImplementedError('init_streams')

    def close_socket(self):
        raise NotImplementedError('close_socket')

