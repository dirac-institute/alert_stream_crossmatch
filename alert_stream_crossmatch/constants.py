import os
import inspect
import time
import logging
import logging.config

BASE_DIR = os.path.dirname(os.path.abspath(inspect.getfile(
                inspect.currentframe()))) + '/'
DB_DIR = BASE_DIR

SIMBAD_EXCLUDES = ['G?', 'SC?', 'C?G', 'Gr?', 'As?', 'Y*?', 'pr?', 'TT?', 'Mi?', 'SCG', 'ClG',
'GrG', 'CGG', 'PaG', 'IG', 'Y*O', 'pr*', 'TT*', 'Or*', 'FU*', 'BY*', 'RS*',
'Pu*', 'RR*', 'Ce*', 'dS*', 'RV*', 'WV*', 'bC*', 'cC*', 'gD*', 'LP*', 'Mi*',
'SN*', 'su*', 'G', 'PoG', 'GiC', 'BiC', 'GiG', 'GiP', 'HzG', 'ALS', 'LyA',
'DLA', 'mAL', 'LLS', 'BAL', 'rG', 'H2G', 'LSB', 'AG?', 'Q?', 'Bz?', 'BL?',
'EmG', 'SBG', 'bCG', 'LeI', 'LeG', 'LeQ', 'AGN', 'LIN', 'SyG', 'Sy1', 'Sy2',
'Bla', 'BLL', 'QSO']

class UTCFormatter(logging.Formatter):
    """Output logs in UTC"""
    converter = time.gmtime

logging_level = 'INFO'

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
            'level': f'{logging_level}',
            'class':'logging.StreamHandler',
            'formatter': 'simple',
            'stream'  : 'ext://sys.stdout'
        },
        'logfile': {
            'level': f'DEBUG',
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
            'level': f'{logging_level}',
        },
        'aiohttp': {
            'handlers':['logfile'],
            'level': f'{logging_level}',
        }
    }
}
