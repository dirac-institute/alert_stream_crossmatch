import os
import inspect
import time
import logging
import logging.config

BASE_DIR = os.path.dirname(os.path.abspath(inspect.getfile(
                inspect.currentframe()))) + '/'

class UTCFormatter(logging.Formatter):
    """Output logs in UTC"""
    converter = time.gmtime


LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'utc': {
            '()': UTCFormatter,
            'format': '%(asctime)s %(levelname)s %(module)s %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
    'handlers': {
        'console':{
            'level':'INFO',
            'class':'logging.StreamHandler',
            'formatter': 'simple',
            'stream'  : 'ext://sys.stdout'
        },
        'logfile': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': f'{BASE_DIR}/../logs/ztf.log',
            'formatter': 'utc',
            'when': 'midnight',
            'utc': 'True'
        }
    },
    'loggers': {
        '': { # this is the root logger; doesn't work if we call it root
            'handlers':['console','logfile'],
            'level':'INFO',
        },
        'aiohttp': {
            'handlers':['logfile'],
            'level':'INFO',
        }
    }
}

