import os
from singletones.custom_logger import MyLogger
import json
logger = MyLogger()

CONFIGS_PATH = os.path.join(os.getcwd(), 'configs.json')


def get_streams(exchange):
    exchange = exchange.lower()
    file_path = os.path.join(CONFIGS_PATH)

    if os.path.exists(file_path):
        with open(file_path) as f:
            configs = f.read()
            configs = json.loads(configs)

            if exchange in configs:
                return configs[exchange]
            else:
                logger.info('No config for %s exchange', exchange)
    else:
        logger.info('%s does not exist', file_path)
