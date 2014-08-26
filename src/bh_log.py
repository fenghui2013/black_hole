import logging
import logging.handlers
import sys
from bh_parse_conf import *

BLACKHOLE_LOG_CONFIG = get_config_log_blackhole()
TRANSLATE_LOG_CONFIG = get_config_log_translate()

FORMAT = "[%(asctime)s - %(name)s]%(levelname)s:%(message)s - %(filename)s:%(lineno)d"

BLACKHOLE_LOGGER = None
TRANSLATE_LOGGER = None

def get_logger_black_hole(m):
    global BLACKHOLE_LOGGER
    if None == BLACKHOLE_LOGGER:
        BLACKHOLE_LOGGER = logging.getLogger(m)

        handler = logging.handlers.RotatingFileHandler(BLACKHOLE_LOG_CONFIG["path"])
        formatter = logging.Formatter(FORMAT)
        handler.setFormatter(formatter)

        BLACKHOLE_LOGGER.addHandler(handler)
        _set_level(BLACKHOLE_LOGGER, BLACKHOLE_LOG_CONFIG["level"])
    return BLACKHOLE_LOGGER 

def get_logger_translate(m):
    global TRANSLATE_LOGGER
    if None == TRANSLATE_LOGGER:
        TRANSLATE_LOGGER = logging.getLogger(m)

        handler = logging.handlers.RotatingFileHandler(TRANSLATE_LOG_CONFIG["path"])
        formatter = logging.Formatter(FORMAT)
        handler.setFormatter(formatter)

        TRANSLATE_LOGGER.addHandler(handler)
        _set_level(TRANSLATE_LOGGER, TRANSLATE_LOG_CONFIG["level"])
    return TRANSLATE_LOGGER 

def _console_handler():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    return handler

def _set_level(logger, level):
    if "debug" == level:
        handler = _console_handler()
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    elif "info" == level:
        logger.setLevel(logging.INFO)
    elif "warning" == level:
        logger.setLevel(logging.WARNING)
    elif "error" == level:
        logger.setLevel(logging.ERROR)
    elif "critical" == level:
        logger.setLevel(logging.CRITICAL)
