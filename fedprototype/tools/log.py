import logging
from fedprototype.base.base_logger_factory import BaseLoggerFactory
from fedprototype.typing import Logger


class LocalLoggerFactory(BaseLoggerFactory):
    LOG_FORMAT = "%(name)s >> [%(levelname)s] [%(asctime)s] [%(process)s:%(thread)s] - " \
                 "%(filename)s[func:%(funcName)s, line:%(lineno)d]: %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    LEVEL = logging.DEBUG

    @staticmethod
    def get_handler(level=None):
        handler = logging.StreamHandler()
        formatter = logging.Formatter(fmt=LocalLoggerFactory.LOG_FORMAT,
                                      datefmt=LocalLoggerFactory.DATE_FORMAT)
        if level:
            handler.level = level
        handler.setFormatter(formatter)
        return handler

    @staticmethod
    def get_logger(name: str) -> Logger:
        logger = logging.getLogger(name)
        if not logger.hasHandlers():
            logger.setLevel(LocalLoggerFactory.LEVEL)
            stream_handler = LocalLoggerFactory.get_handler()
            logger.addHandler(stream_handler)
        return logger
