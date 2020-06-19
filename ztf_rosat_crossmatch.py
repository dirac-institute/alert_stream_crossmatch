#!/usr/bin/env python
# By Myles McKay
# June 7, 2020
# ZTF crossmatch with X-Ray Binaries (ROSAT catalog)

import glob
import argparse
import logging
import numpy as np
import pandas as pd
import fastavro
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.io import fits
import adc.streaming as adcs
import requests
from astropy.io import ascii

# Example command line execution:

# $ python ztf_rosat_crossmatch.py --kafka_path="kafka://partnership.alerts.ztf.uw.edu/ztf_20200514_programid1" > kafka_output_20200514.txt

SIGMA_TO_95pctCL = 1.95996

def read_avro_file(fname):
    """Reads a single packet from an avro file stored with schema on disk."""
    with open(fname,'rb') as f:
        freader = fastavro.reader(f)
        for packet in freader:
            return packet

def get_candidate_info(packet):

    return {'ra': packet['candidate']['ra'], 'dec': packet['candidate']['dec'],
            'object_id': packet['objectId'], 'candid': packet['candid']}
                    
    

def load_rosat():    
    # Open ROSAT catalog
    rosat_fits = fits.open('/epyc/data/rosat_2rxs/cat2rxs.fits')
    
    # Make ROSAT data into a pandas dataframe
    rosat_data = rosat_fits[1].data
    dfx = pd.DataFrame(rosat_data)
    
    # exclude sources that are not observable
    dfx = dfx[dfx.DEC_DEG >= -30] 
    
    # List ROSAT error position [arcsec] 
    dfx['err_pos_arcsec'] = np.sqrt(((dfx.XERR*45)**2.+ (dfx.YERR*45)**2.) + 0.6**2.) * SIGMA_TO_95pctCL 
    
    # Put ROSAT ra and dec list in SkyCoord [degrees]
    rosat_skycoord = SkyCoord(ra=dfx.RA_DEG, dec=dfx.DEC_DEG, 
            frame='icrs', unit=(u.deg))

    return dfx[['IAU_NAME','RA_DEG','DEC_DEG','err_pos_arcsec']], rosat_skycoord


def not_moving_object(packet):
    """Check if there are > 2 detections separated by at least 30 minutes.
    
    Parameters:
       avro_packet: dictionary
          Extracted data from the avro file
                
    Return:
        real: boolean
            True if there is another detection > 30 minutes from the triggering detection
    """    
    
    date0 = float(packet['candidate']['jd'])
    if len(packet['prv_candidates']) == 0:
        logging.debug("No previous detections of {packet['objectId']}")
        return False

    for prv_candidate in packet['prv_candidates']:
        if prv_candidate['candid'] is not None:
            date1 = float(prv_candidate['jd'])
            diff_date = date0 - date1
            if diff_date < (0.5/24):
                continue
            else:
                logging.debug("Previous detection of {packet['objectId']} {diff_date} days earlier")
                return True

    return False




def ztf_rosat_crossmatch(ztf_source, rosat_skycoord, dfx):
    """
    Cross match ZTF and ROSAT data using astropy.coordinates.SkyCoord
    
    Parameters:
                ztf_source: dict
                    {'ra': float (degrees), 'dec': float (degrees),
                    'object_id': string, 'candid': int}
                    
                rosat_skycoord: astropy.coordinates.SkyCoord
                    ROSAT catalog in astropy.coordinates.SkyCoord
                    
                dfx: pandas.DataFrame
                    slim ROSAT catalog
                
    Return:
            matched_source: dict or None
                if a source matches, return
                    {'ra': float (degrees), 'dec': float (degrees),
                    'source_name': string, 'sep2d': float (arcsec)}
                else None
    """
    

    # Input avro data ra and dec in SkyCoords
    avro_skycoord = SkyCoord(ra=ztf_source['ra'], dec=ztf_source['dec'], 
            frame='icrs', unit=(u.deg))
    
    # Finds the nearest ROSAT source's coordinates to the avro files ra[deg] and dec[deg]
    match_idx, match_sep2d, _ = avro_skycoord.match_to_catalog_sky(rosat_skycoord) 
    
    match_row = dfx.iloc[match_idx]

    matched = match_sep2d[0] <= match_row['err_pos_arcsec'] * u.arcsecond

    match_result = {'match_name': match_row['IAU_NAME'], 
                    'match_ra': match_row['RA_DEG'],
                    'match_dec': match_row['DEC_DEG'],
                    'match_err_pos': match_row['err_pos_arcsec'],
                    'match_sep': match_sep2d[0].to(u.arcsecond).value}

    if matched:
        #logging.info
        print(f"{ztf_source['object_id']} ({avro_skycoord.to_string('hmsdms')}) matched {match_result['match_name']} ({match_result['match_sep']:.2f} arcsec away)")
        return match_result

    else:
        #logging.debug
        print(f"{ztf_source['object_id']} ({avro_skycoord.to_string('hmsdms')}) did not match (nearest source {match_result['match_name']}, {match_result['match_sep']:.2f} arcsec away")
        return None

    
    
def get_programidx(program_name, username, password):
    '''Given a marshal science program name, it returns its programidx'''

    r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/list_programs.cgi',
                      auth=(username, password))
    programs = r.json()
    program_dict = {p['name']: p['programidx'] for p in programs}

    try:
        return program_dict[program_name]
    except KeyError:
        print(f'The user {username} does not have access to the program \
              {program_name}')
        return None
    
    
def ingest_growth_marshal(programidx, candid):
    # Using ingest_avro_id.cgi to save data to X-ray counterpart programid 14
    r = requests.post("http://skipper.caltech.edu:8080/cgi-bin/growth/ingest_avro_id.cgi",
                              auth=(username_marshal,
                                    password_marshal),
                              data={'programidx': programid14,
                                    'avroid': candid})
    try:
        r.raise_for_status()
        logging.info(f'Successfully ingested {candid}')
    except Exception as e:
        logging.exception(e)    
    
    
#if __name__ == '__main__':
def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument('date', type = str, help = 'UTC date as YYMMDD')
    parser.add_argument('program_id', type = int, help = 'Program ID (1 or 2)')

    args = parser.parse_args()

    if len(args.date) != 6:
        raise ValueError(f'Date must be specified as YYMMDD.  Provided {args.date}')

    if args.program_id not in [1,2]:
        raise ValueError(f'Program id must be 1 or 2.  Provided {args.program_id}')
    
    # load X-ray catalogs
    dfx, rosat_skycoord = load_rosat()
        
    
    # Read the secrets - Make a .csv and type username and password for GROWTH Marshall 
    secrets = ascii.read('secrets.csv', format='csv')
    
    # GROWTH marshal credentials
    username_marshal = secrets['marshal_user'][0]
    password_marshal = secrets['marshal_pwd'][0]
    
    # Get programidx
    #programid14 = get_programidx("X-ray Counterparts", username_marshal, password_marshal)
    programid14 = 14

    kafka_topic = f"ztf_20{args.date}_programid{args.program_id}"
    kafka_path = f"kafka://partnership.alerts.ztf.uw.edu/{kafka_topic}"
    logging.info("Connecting to Kafka topic {kafka_topic}"

    with adcs.open(kafka_path, "r", format="avro", start_at="earliest") as stream:
        for nread, packet in enumerate(stream(progress=True, timeout=False)):#, start=1):
            ztf_source = get_candidate_info(packet)

            matched_source = ztf_rosat_crossmatch(ztf_source, rosat_skycoord, dfx)

            if matched_source is not None:
                if not_moving_object(avro_packet):
                    ingest_growth_marshal(programid14, ztf_source['candid'])
        
            stream.commit()

    stream.commit(defer=False)
