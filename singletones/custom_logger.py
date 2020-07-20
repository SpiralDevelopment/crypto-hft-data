import logging
import os
import inspect

ALL_LOGS_FILE = 'logs.log'


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class MyLogger(metaclass=Singleton):
    console_logger = None
    file_logger = None

    def __init__(self, all_logs_file_name=ALL_LOGS_FILE):

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(threadName)s - %(filename)s - %(message)s",
            handlers=[
                logging.StreamHandler()
            ])

        all_logs_file_name = os.path.join(os.getcwd(), all_logs_file_name)

        fh = logging.FileHandler(all_logs_file_name)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(threadName)s - %(message)s"))
        logging.getLogger(__name__ + '.file_logger').addHandler(fh)
        self.file_logger = logging.getLogger(__name__ + '.file_logger')

        # console logger
        self.console_logger = logging.getLogger(__name__ + '.console_logger')

    @staticmethod
    def __get_call_info():
        stack = inspect.stack()

        fn = stack[2][1]
        ln = stack[2][2]
        func = stack[2][3]

        return fn, func, ln

    def info(self, message, *args, file=False):
        if file:
            self.file_logger.info(message, *args)
        else:
            self.console_logger.info(message, *args)

    def debug(self, message, *args, file=True):
        if file:
            self.file_logger.debug(message, *args)
        else:
            self.console_logger.info(message, *args)

    def warning(self, message, *args, file=True):
        if file:
            self.file_logger.warning(message, *args)
        else:
            self.console_logger.warning(message, *args)

    def error(self, message, *args, exc_info=1, file=True):
        if file:
            self.file_logger.error(message, *args, exc_info=exc_info)
        else:
            self.console_logger.error(message, *args, exc_info=exc_info)

    def critical(self, message, *args, file=True):
        if file:
            self.file_logger.critical(message, *args)
        else:
            self.console_logger.critical(message, *args)