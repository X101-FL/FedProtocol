import logging


class LoggerFactory:

    LOG_FORMAT = "%(name)s >> [%(levelname)s] [%(asctime)s] [%(process)s:%(thread)s] - " \
                 "%(filename)s[func:%(funcName)s, line:%(lineno)d]: %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    LEVEL = logging.DEBUG

    @staticmethod
    def get_handler(level=None):
        handler = logging.StreamHandler()
        formatter = logging.Formatter(fmt=LoggerFactory.LOG_FORMAT,
                                      datefmt=LoggerFactory.DATE_FORMAT)
        if level:
            handler.level = level
        handler.setFormatter(formatter)
        return handler

    @staticmethod
    def get_logger(role_name):
        logger = logging.getLogger(role_name)
        logger.setLevel(LoggerFactory.LEVEL)
        stream_handler = LoggerFactory.get_handler()
        logger.addHandler(stream_handler)
        return logger
