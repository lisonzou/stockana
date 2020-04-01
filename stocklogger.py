import logging
import logging.handlers
import os
import sys
from readconfig import get_config

def init_mylogger(action, logger, default_loglevel):
    filename = get_config('LOG', 'logfile')
    selfdirname, selffilename = os.path.split(os.path.abspath(sys.argv[0]))
    log_filename = selfdirname + '\\log\\' + filename
    formatter = logging.Formatter('%(asctime)s - %(levelname)s  - %(message)s')
    handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=1024 * 1024, backupCount=5)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if default_loglevel == 'INFO' or default_loglevel is None:
        logger.setLevel(logging.INFO)
    elif default_loglevel == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    elif default_loglevel == 'ERROR':
        logger.setLevel(logging.ERROR)
    elif default_loglevel == 'WARNING':
        logger.setLevel(logging.WARNING)
    elif default_loglevel == 'CRITICAL':
        logger.setLevel(logging.CRITICAL)

    if action == 'empty':
        logger.removeHandler(handler)
    return logger, handler


def getmylogger():
    default_loglevel = 'INFO'
    logger1 = logging.getLogger('stockana')
    if len(logger1.handlers) > 0:
        mylogger, myhandler = init_mylogger('empty', logger1, default_loglevel)
    else:
        mylogger, myhandler = init_mylogger('init', logger1, default_loglevel)
    return mylogger
