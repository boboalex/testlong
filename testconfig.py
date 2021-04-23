import logging
import requests
from configparser import ConfigParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
cfg = ConfigParser()
cfg.read('config.ini')

class TestConfig:
    def __init__(self):
        self.host = cfg.get('server', 'ki_host')
        self.port = cfg.get('server', 'ki_port')

        if self.host is None or self.port is None:
            raise ValueError('initializing http client failed')
        if not self.host:
            raise ValueError('initializing http client failed')

if __name__ == '__main__':
    tc = TestConfig()