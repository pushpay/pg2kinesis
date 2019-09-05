import logging

from pg2kinesis.log import logger


class LogWriter(object):
    """
    Basic writer that uses a logger to print messages.
    """

    def __init__(self, logger_name):
        self.logger = logging.Logger(logger_name)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)s: %(asctime)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def put_message(self, fmt_msg):

        if fmt_msg:
            self.logger.info(fmt_msg.fmt_msg)

        return True
