import tarfile
import fastavro
import io
import os
import json
import sqlite3
import pandas as pd
import numpy as np
import pdb
import matplotlib.pyplot as plt
import multiprocessing as mp
from itertools import repeat


RAW_TAR_LIST='/epyc/users/ykwang/scripts/raw_alert_list.txt'

# List of alert tar files to look in
TAR_LIST='/epyc/users/ykwang/Github/dev/alert_stream_crossmatch/alert_stream_crossmatch/alert_list.txt'
LC_FIELDS = ["jd", "fid", "magpsf", "sigmapsf", "diffmaglim", "isdiffpos", "magnr", "sigmagnr", "field", "rcid"]

SAVE_LC_PATH = '../local/lcs/archival/'
CV_PATH = '/epyc/projects/xrb_search/data/CVs.csv'

def get_CV_candids():
    '''Returns a numpy array of all CVs saved in Fritz'''
    df = pd.read_csv(CV_PATH)
    return df['id'].values

def read_avro_bytes(buf):
    """Reads a single packet from an avro file stored with schema on disk."""
    with io.BytesIO(buf) as f:
        freader = fastavro.reader(f)
        for packet in freader:
            return packet

def test_read_tar(tar_path):
    '''Tests whether the tarfile is corrupt and returns the path and the number of alerts'''
    # open tarfile
    try:
        tar = tarfile.open(tar_path, 'r:gz')
    except tarfile.ReadError:
        print(f'Read error reading {tar_path}')
        return tar_path, 0
    except Exception as e:
        print(f"can't open {tar_path}", e) 
        return tar_path, 0
    
    # try to get number of elements in tarfile
    try:
        n_alerts = len(tar.getmembers())
        print(f'{tar_path}: {n_alerts}')
        return tar_path, n_alerts
    except tarfile.ReadError:
        print(f'Read error reading {tar_path}')
        return tar_path, 0
    except Exception as e:
        print(f"can't open {tar_path}", e) 
        return tar_path, 0
    

def make_dataframe(packet, save_oid_fields=False, repeat_obs=False):
    '''Extract relevant lightcurve data from packet into pandas DataFrame.'''
    
    # Get observation specific data (i.e. LC_FIELDS)
    df = pd.DataFrame(packet["candidate"], index=[0])
    if repeat_obs:
        df["ZTF_object_id"] = packet["objectId"]
        return df[["ZTF_object_id"] + LC_FIELDS]

    # Get nondetection data if not previously seen
    df_prv = pd.DataFrame(packet["prv_candidates"])
    df_merged = pd.concat([df, df_prv], ignore_index=True)
    df_merged["ZTF_object_id"] = packet["objectId"]
    
    # Save object data (OID_FIELDS)
#     if save_oid_fields:
#         df["ZTF_object_id"] = packet["objectId"]
#         df[["ZTF_object_id"] + OID_FIELDS].to_csv(OID_CSV_PATH, mode='a', index=False, header=False)

    return df_merged[["ZTF_object_id"] + LC_FIELDS]

def get_dflc(tar_path, candids, save=True):
    # Get all lightcurve data for 
    try:
        tar = tarfile.open(tar_path, 'r:gz')
    except tarfile.ReadError:
        print(f'Read error reading {tar_path}')
        return pd.DataFrame([])
    except Exception as e:
        print(f'Other error reading {tar_path}, {e}')
        return pd.DataFrame([])
    
    processed = []
    try:
        print(f'Total alerts: {len(tar.getmembers())} beginning...')
    except Exception as e:
        print(f"can't open {tar_path}", e) 
        return pd.DataFrame([])
        
    for ii, tarpacket in enumerate(tar.getmembers()):
#         if ii%5000 == 0:
#             print(f"{ii} messaged consumed")
        try:
            packet = read_avro_bytes(tar.extractfile(tarpacket).read())
            if packet['objectId'] in candids: 
                # print(packet['objectId'])
                processed.append(make_dataframe(packet))
        except Exception as e:
            # print(packet['objectId'])
            print(f'error reading an oject in {tar_path}', e)
            continue
    try:
        data = pd.DataFrame([])
        if len(processed) > 0:
            data = pd.concat(processed)
            # data.to_csv(f'{LC_SAVE_DIR}{ts}_{program}.csv', index=False)
        return data
    
    except Exception as e:
        print(f'error saving {tar_path}', e)
        return pd.DataFrame([])
    return pd.DataFrame([])

def get_full_lightcurves(n_cores=24, batches=25, save_name='lcs.csv'):
    # Set up multiprocessing
    niceness = 5
    os.nice(niceness)
    p = mp.Pool(n_cores)
    
    # Get list of tarfiles to look in
    with open(TAR_LIST) as f:  
        tar_list = np.array([x.strip() for x in f.readlines()])
    
    # Split list of tar_files into chunks
    tar_chunks = np.array_split(tar_list, batches)
    
    # Get all cvs
    candids = get_CV_candids()
    
    for ii, chunk in enumerate(tar_chunks):
        print(f'chunk #{ii}, total tarfiles {len(chunk)}')
        raw_lcs = p.starmap(get_dflc, zip(chunk, repeat(candids)))
        df = pd.concat(raw_lcs)
        df['jd'] = df['jd'].round(6)
        df.drop_duplicates(subset=['ZTF_object_id', 'jd', 'fid'], inplace=True)
        df.to_csv(SAVE_LC_PATH + f'lcs{ii}.csv', index=False)
        
    # concat all chunks together, dropping duplicates
    lcs = []
    for ii in range(len(tar_chunks)):
        lcs.append(pd.read_csv(SAVE_LC_PATH + f'lcs{ii}.csv'))
        df = pd.concat(lcs)
        df['jd'] = df['jd'].round(6)
        df.drop_duplicates(subset=['ZTF_object_id', 'jd', 'fid'], inplace=True)
        lcs = [df]
        
    df.to_csv(SAVE_LC_PATH + save_name)
        


def find_noncorrupt_tarfiles(n_cores=40, save=True):
    niceness = 3
    os.nice(niceness)
    p = mp.Pool(n_cores)

    with open(RAW_TAR_LIST) as f:  
        tar_list = np.array([x.strip() for x in f.readlines()])    
    n_alerts = p.map(test_read_tar, tar_list)
    df = pd.DataFrame(n_alerts, columns=['path', 'n_alerts'])
    df.sort_values('path')
    df.to_csv('n_alerts.csv', index=False)
    np.savetxt('alert_list.txt', df.loc[df['n_arlerts'] > 0, 'path'].values, delimiter='\n')


if __name__ == '__main__':
    get_full_lightcurves(save_name='CVs')
