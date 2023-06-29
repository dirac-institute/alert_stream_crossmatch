# Combines db tables of date from cmd line with objs and lcs csvs
import pandas as pd
import numpy as np
import argparse
import sqlite3
from os import listdir
from os.path import isfile, join
DATA_PATH = '/epyc/users/ykwang/Github/dev/alert_stream_crossmatch/local/db/archival/'
SAVE_PATH = '/epyc/projects/xrb_search/data/'
# data_files = [f for f in listdir(DATA_PATH) if isfile(join(DATA_PATH, f))]
# data_files = pd.Series(data_files).sort_values().values

parser = argparse.ArgumentParser()
parser.add_argument("date", type=str, help="date of db")
args = parser.parse_args()

df = pd.DataFrame([])

try:
    conn = sqlite3.connect(DATA_PATH + f'sqlite_{args.date}.db')
    obj = pd.read_sql_query("SELECT * from ZTF_objects", conn)
    lc = pd.read_sql_query("SELECT * from lightcurves", conn)
    # temp = pd.read_csv(DATA_PATH + f)
except:
    import pdb; pdb.set_trace()

    
    
objs = pd.read_csv(SAVE_PATH + 'objs.csv')
lcs = pd.read_csv(SAVE_PATH + 'lcs.csv')

lcs = pd.concat([lcs, lc])
lcs.drop_duplicates(inplace=True)
objs = pd.concat([objs, obj])
objs.drop_duplicates(inplace=True)
conn.close()


objs.to_csv(f'{SAVE_PATH}objs.csv', index=False)
lcs.to_csv(f'{SAVE_PATH}lcs.csv', index=False)
 