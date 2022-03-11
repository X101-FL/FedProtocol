import logging
from logging import config

from fedprototype.constants import LOGGING_CONFIG_FILE

config.fileConfig(LOGGING_CONFIG_FILE)
getLogger = logging.getLogger
