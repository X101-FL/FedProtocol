import os
import logging
from tools import file_utils


class LoggerFactory:

    LOG_FORMAT = "=%(name)s= [%(levelname)s] [%(asctime)s] [%(process)s:%(thread)s] - " \
                 "%(filename)s[func:%(funcName)s, line:%(lineno)d]: %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    LEVEL = logging.DEBUG
    LOG_DIR = None

    @staticmethod
    def set_directory(log_dir="logs"):
        LoggerFactory.LOG_DIR = os.path.join(file_utils.get_project_dir(), log_dir)
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)

    @staticmethod
    def get_handler(level=None, log_file=None):
        if log_file:
            if not LoggerFactory.LOG_DIR:
                LoggerFactory.set_directory()
            log_path = os.path.join(LoggerFactory.LOG_DIR, log_file)
            handler = logging.FileHandler(log_path)
        else:
            handler = logging.StreamHandler()
        formatter = logging.Formatter(fmt=LoggerFactory.LOG_FORMAT,
                                      datefmt=LoggerFactory.DATE_FORMAT)
        if level:
            handler.level = level
        handler.setFormatter(formatter)
        return handler

    @staticmethod
    def get_logger(role_name, log_file="log.txt"):
        logger = logging.getLogger(role_name)
        logger.setLevel(LoggerFactory.LEVEL)
        stream_handler = LoggerFactory.get_handler()
        logger.addHandler(stream_handler)

        file_handler = LoggerFactory.get_handler(log_file=log_file)
        logger.addHandler(file_handler)
        return logger
