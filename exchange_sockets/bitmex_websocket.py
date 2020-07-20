import websocket
import json
import urllib, hmac, hashlib
from helpers.date_time_helper import *
from exchange_sockets.exchange_websocket import ExchangeWebSocket
from singletones.custom_logger import MyLogger
import time
import threading

logger = MyLogger()


class BitMexWebSocket(ExchangeWebSocket):
    def __init__(self, pairs_n_streams, api_key=None, api_secret=None):
        super().__init__('Bitmex', pairs_n_streams)

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self.api_key = api_key
        self.api_secret = api_secret
        self.streams = []

        self.ws = None
        self.possible_streams = ['trade', 'orderBook10', 'liquidation', 'orderBookL2']

    def init_streams(self):
        streams_dict = {}

        for pair, stream in self.pairs_n_streams.items():
            if self.has_stream(stream):
                for sub_stream in stream.split(','):
                    if not streams_dict.get(sub_stream, None):
                        streams_dict[sub_stream] = []

                    streams_dict[sub_stream].append(pair.upper())
            else:
                logger.warning('%s. %s stream is not in possible streams list',
                               pair, stream)

        for stream, pairs in streams_dict.items():
            for pair in pairs:
                self.streams.append("{}:{}".format(stream, pair))

        logger.info('%s Streams are initialized: %s',
                    len(self.streams),
                    self.streams)

        if len(self.streams) == 1 and self.streams[0].startswith('liquidation'):
            self.max_delay = 600

    def start_multiple_websocket(self, init_streams=True):
        super().start_multiple_websocket(init_streams=init_streams)

        endpoint = "wss://www.bitmex.com/realtime?subscribe=" + ','.join(self.streams)

        websocket.enableTrace(True)

        self.ws = websocket.WebSocketApp(endpoint,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            time.sleep(1)
            conn_timeout -= 1

        if not conn_timeout:
            logger.error("%s Couldn't connect to %s! Exiting.", self.node, self.exchange)
            self.close_socket()
        else:
            logger.info('{} socket is started:'
                             '\n{}\n{}'.format(self.exchange,
                                               self.node,
                                               '\n'.join(self.streams)))

    def __on_error(self, ws, error):
        self.on_error = True
        logger.error("On error\n{}\n{} {}".format(self.node,
                                                  self.exchange,
                                                  error))

    def __on_close(self, ws):
        logger.info("On close\n{}".format(self.exchange))

    def __on_open(self, ws):
        logger.info("On Open\n{}".format(self.exchange))

    def close_socket(self):
        self.exited = True
        if self.ws:
            self.ws.close()

    def save_trade_to_file(self, data):

        if data.get('action', '') == 'insert':
            if isinstance(data.get('data', None), list):
                all_csv_data = {}

                for cur in data['data']:
                    utc_dtm = str_to_date_time(cur['timestamp'],
                                               from_time_format="%Y-%m-%dT%H:%M:%S.%fZ")
                    timestamp = from_utc_dtm_to_utc_timestamp(utc_dtm)

                    if not all_csv_data.get(cur['symbol'], None):
                        all_csv_data[cur['symbol']] = []

                    all_csv_data[cur['symbol']].append(",".join([str(cur) for cur in [timestamp,
                                                                                      cur['side'],
                                                                                      cur['size'],
                                                                                      cur['price'],
                                                                                      cur['tickDirection'],
                                                                                      cur['grossValue'],
                                                                                      cur['homeNotional'],
                                                                                      cur['foreignNotional']]]) + "\n")

                for symbol, csv_data in all_csv_data.items():
                    write_data = ''.join(csv_data)
                    # print(write_data)
                    self.file_manager.save_trades_to_file(self.exchange,
                                                          data['table'],
                                                          symbol,
                                                          write_data,
                                                          columns='timestamp,side,size,price,'
                                                                  'tick_direction,gross_value,'
                                                                  'home_notional,foreign_notional\n')

    def save_l2_ob_to_file(self, data):
        if data.get('action', '') == 'update':
            if isinstance(data.get('data', None), list):
                all_data = {}

                for cur in data['data']:
                    if not all_data.get(cur['symbol'], None):
                        all_data[cur['symbol']] = []

                    all_data[cur['symbol']].append("{},{},{}\n".format(
                        time.time(),
                        cur['size'],
                        cur['side']))

                for symbol, l2_ob_data in all_data.items():
                    for l2_ob in l2_ob_data:
                        self.file_manager.save_data_to_file(self.exchange,
                                                            data['table'],
                                                            symbol,
                                                            l2_ob)

    def save_ob_to_file(self, data):
        if data.get('action', '') == 'update':
            if isinstance(data.get('data', None), list):

                all_data = {}

                for cur in data['data']:
                    utc_dtm = str_to_date_time(cur['timestamp'],
                                               from_time_format="%Y-%m-%dT%H:%M:%S.%fZ")
                    timestamp = from_utc_dtm_to_utc_timestamp(utc_dtm)

                    if not all_data.get(cur['symbol'], None):
                        all_data[cur['symbol']] = []

                    bids = 'B{0}\n'.format(timestamp)
                    asks = 'A{0}\n'.format(timestamp)

                    bids += '\n'.join(["{0},{1}".format(bid[0], bid[1]) for bid in cur['bids']])
                    asks += '\n'.join(["{0},{1}".format(ask[0], ask[1]) for ask in cur['asks']])

                    all_data[cur['symbol']].append("{0}\n{1}\n".format(bids, asks))

                for symbol, bids_asks_str in all_data.items():
                    for ba_str in bids_asks_str:
                        self.file_manager.save_data_to_file(self.exchange,
                                                            data['table'],
                                                            symbol,
                                                            ba_str)

    def save_liquidation(self, data):
        if data.get('action', '') == 'insert':
            if isinstance(data.get('data', None), list):
                # print(data)
                side = 'short' if data['data'][0]['side'] == 'Buy' else 'long'
                qty = str(data['data'][0]['leavesQty'])
                price = str(data['data'][0]['price'])
                symbol = data['data'][0]['symbol']

                # liq_str = 'Liquidating ' + symbol + ' ' + side + ': ' + data['data'][0][
                #     'side'] + ' ' + qty + ' at ' + price

                # print(liq_str)

                save_data = "{}\n".format(','.join([str(int(time.time())),
                                                    symbol,
                                                    side,
                                                    qty,
                                                    price]))

                self.file_manager.save_data_to_file(self.exchange,
                                                    'liquidation',
                                                    symbol,
                                                    save_data,
                                                    columns='time,symbol,side,quantity,price')

    def __on_message(self, ws, message):
        data = json.loads(message)
        self.last_msg_time = int(time.time())

        if data.get('table', '') == 'trade':
            self.save_trade_to_file(data)
        elif data.get('table', '') == 'orderBook10':
            self.save_ob_to_file(data)
        elif data.get('table', '') == 'orderBookL2':
            self.save_l2_ob_to_file(data)
        elif data.get('table', '') == 'liquidation':
            self.save_liquidation(data)

    def __get_auth(self):
        if self.api_key:
            nonce = self.__generate_nonce()
            return [
                "api-nonce: " + str(nonce),
                "api-signature: " + self.__generate_signature(self.api_secret, 'GET', '/realtime', nonce, ''),
                "api-key:" + self.api_key
            ]
        else:
            return []

    def __generate_nonce(self):
        return int(round(time.time() * 1000))

    def __generate_signature(self, secret, verb, url, nonce, data):
        """Generate a request signature compatible with BitMEX."""
        # Parse the url so we can remove the base and extract just the path.
        parsedURL = urllib.parse.urlparse(url)
        path = parsedURL.path
        if parsedURL.query:
            path = path + '?' + parsedURL.query

        # print "Computing HMAC: %s" % verb + path + str(nonce) + data
        message = (verb + path + str(nonce) + data).encode('utf-8')

        signature = hmac.new(secret.encode('utf-8'), message, digestmod=hashlib.sha256).hexdigest()
        return signature
