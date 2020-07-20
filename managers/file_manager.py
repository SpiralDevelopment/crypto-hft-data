from datetime import datetime
import os
import csv

MAIN_FILES_DIR = os.path.join(os.getcwd(), 'files')


class FileManager(object):
    def __init__(self):
        pass

    @staticmethod
    def create_dirs(exchange, pairs_n_streams):
        if not os.path.exists(MAIN_FILES_DIR):
            os.mkdir(MAIN_FILES_DIR)

        exchange_files_dir = os.path.join(MAIN_FILES_DIR, exchange)

        if not os.path.exists(exchange_files_dir):
            os.mkdir(exchange_files_dir)

        for pair, stream in pairs_n_streams.items():
            for substream in stream.split(','):
                streams_files_dir = os.path.join(exchange_files_dir, substream)

                if not os.path.exists(streams_files_dir):
                    os.mkdir(streams_files_dir)

    @staticmethod
    def get_all_files(exchange, pair, stream):
        path = os.path.join(os.getcwd(), 'files', exchange, stream)
        a = [s for s in os.listdir(path)
             if pair in s and os.path.isfile(os.path.join(path, s))]
        # a.sort(key=lambda s: os.path.getmtime(os.path.join(path, s)))
        return a

    @staticmethod
    def get_last_file(exchange, pair, stream):
        file_name = '{}_{}_{}'.format(exchange,
                                      pair.upper(),
                                      str(datetime.now().date()))
        return os.path.join(os.getcwd(), 'files', exchange, stream, file_name)

    @staticmethod
    def does_file_exist(exchange, stream, pair):
        file_path = FileManager.get_last_file(exchange, pair, stream)
        return os.path.exists(file_path)

    @staticmethod
    def save_data_frame_to_file(exchange, stream, pair, df):
        file_path = FileManager.get_last_file(exchange, pair, stream)

        with open(file_path, 'a') as file:
            df.to_csv(file, sep=',', header=file.tell() == 0, index=False)

    @staticmethod
    def save_data_to_file(exchange, stream, pair, data, columns=None):
        file_path = FileManager.get_last_file(exchange, pair, stream)

        with open(file_path, 'a') as file:
            if file.tell() == 0 and columns:
                file.write(columns+"\n")

            file.write(data)

    @staticmethod
    def save_trades_to_file(exchange, stream, pair, trade, columns=None):
        file_path = FileManager.get_last_file(exchange, pair, stream)

        if isinstance(trade, dict):
            with open(file_path, 'a') as file:
                w = csv.DictWriter(file, trade.keys())

                if file.tell() == 0:
                    w.writeheader()

                w.writerow(trade)
        elif isinstance(trade, str):
            with open(file_path, 'a') as file:

                if file.tell() == 0 and columns:
                    file.write(columns + "\n")

                file.write(trade)
