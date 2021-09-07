import os
import inspect
import time
import logging
import logging.config

BASE_DIR = os.path.dirname(os.path.abspath(inspect.getfile(
                inspect.currentframe()))) + '/'
DB_DIR = f"{BASE_DIR}../local/db/"
FITS_DIR = f"{BASE_DIR}../local/cutouts/"

# Catalog requires xray_name, RA_DEG, DEC_DEG, and err_pos_arcsec columns
CATALOG_DIR = '/epyc/users/ykwang/data/xray_catalog.csv'  # Change me 

# group id for kafka consumer
GROUP_ID_PREFIX = 'uw_xray'

# time to continue consuming for after getting to end of queue
KAFKA_TIMEOUT = 20000 #7000000 # ms

SIMBAD_EXCLUDES = ['G?', 'SC?', 'C?G', 'Gr?', 'As?', 'Y*?', 'pr?', 'TT?', 'Mi?', 'SCG', 'ClG',
'GrG', 'CGG', 'PaG', 'IG', 'Y*O', 'pr*', 'TT*', 'Or*', 'FU*', 'BY*', 'RS*',
'Pu*', 'RR*', 'Ce*', 'dS*', 'RV*', 'WV*', 'bC*', 'cC*', 'gD*', 'LP*', 'Mi*',
'SN*', 'su*', 'G', 'PoG', 'GiC', 'BiC', 'GiG', 'GiP', 'HzG', 'ALS', 'LyA',
'DLA', 'mAL', 'LLS', 'BAL', 'rG', 'H2G', 'LSB', 'AG?', 'Q?', 'Bz?', 'BL?',
'EmG', 'SBG', 'bCG', 'LeI', 'LeG', 'LeQ', 'AGN', 'LIN', 'SyG', 'Sy1', 'Sy2',
'Bla', 'BLL', 'QSO'] + ['HII',  'No*', 'MoC', 'Cld', 'HH', 'Ae*'] # Additional otypes from 210630 meeting

class UTCFormatter(logging.Formatter):
    """Output logs in UTC"""
    converter = time.gmtime

logging_level = 'INFO' # Change to DEBUG for more verbose logs

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
