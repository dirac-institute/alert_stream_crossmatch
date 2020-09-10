#!/usr/bin/env python
# By Myles McKay
# June 7, 2020
# ZTF crossmatch with X-Ray Binaries (ROSAT catalog)


import io
import glob
import time
import argparse
import logging
from threading import Lock
from copy import deepcopy
import numpy as np
import pandas as pd
import fastavro
from astropy.coordinates import SkyCoord, match_coordinates_sky
import astropy.units as u
from astropy.io import fits
from aiokafka import AIOKafkaConsumer
import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools

import requests
from astropy.io import ascii
from astroquery.simbad import Simbad
from .constants import UTCFormatter, LOGGING, BASE_DIR, SIMBAD_EXCLUDES

# Example command line execution:

# $ python ztf_rosat_crossmatch.py --kafka_path="kafka://partnership.alerts.ztf.uw.edu/ztf_20200514_programid1" > kafka_output_20200514.txt

SIGMA_TO_95pctCL = 1.95996

# GROWTH marshal credentials
# secrets = ascii.read(BASE_DIR+'/secrets.csv', format='csv')
username_marshal = None # secrets['marshal_user'][0]
password_marshal = None # secrets['marshal_pwd'][0]


def read_avro_file(fname):
    """Reads a single packet from an avro file stored with schema on disk."""
    with open(fname,'rb') as f:
        freader = fastavro.reader(f)
        for packet in freader:
            return packet

def read_avro_bytes(buf):
    """Reads a single packet from an avro file stored with schema on disk."""
    with io.BytesIO(buf) as f:
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
    rosat_fits.close()
    
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


customSimbad=Simbad()
customSimbad.add_votable_fields("otype(3)")
customSimbad.add_votable_fields("otypes(3)") # returns a '|' separated list of all the otypes

def query_simbad(ra,dec):
    sc = SkyCoord(ra,dec,unit=u.degree)
    result_table = customSimbad.query_region(sc, radius=2*u.arcsecond) 
    return result_table

def is_excluded_simbad_class(ztf_source):
    """Is the object in Simbad, with object types we reject (e.g., AGN)?
    """
    try:
        result_table = query_simbad(ztf_source['ra'],ztf_source['dec'])
        if result_table is None:
            logging.info(f"{ztf_source['object_id']} not found in Simbad") 
            return False # not in Simbad
        else:
            # for now let's just take the closest match
            otype = result_table['OTYPE_3'][0].decode("utf-8")
            simbad_id = result_table['MAIN_ID'][0].decode("utf-8")
            if otype in SIMBAD_EXCLUDES:
                logging.info(f"{ztf_source['object_id']} found in Simbad as {simbad_id} ({otype}); omitting")
                return True
            else:
                logging.info(f"{ztf_source['object_id']} found in Simbad as {simbad_id} ({otype}); saving")
                return False

    except Exception as e:
        # if this doesn't work, record the exception and continue
        logging.exception(f"Error querying Simbad for {ztf_source['object_id']}",e)
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
        logging.info(f"{ztf_source['object_id']} ({avro_skycoord.to_string('hmsdms')}; {ztf_source['candid']}) matched {match_result['match_name']} ({match_result['match_sep']:.2f} arcsec away)")
        return match_result

    else:
        logging.debug(f"{ztf_source['object_id']} ({avro_skycoord.to_string('hmsdms')}) did not match (nearest source {match_result['match_name']}, {match_result['match_sep']:.2f} arcsec away")
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
    
    
def ingest_growth_marshal(candid):
    """Using ingest_avro_id.cgi to save data to X-ray counterpart programid 14
    """
    
    # Get programidx
    #programid14 = get_programidx("X-ray Counterparts", username_marshal, password_marshal)
    programid14 = 14

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
    

def process_packet(packet, rosat_skycoord, dfx, saved_packets, lock):
                                    
    ztf_source = get_candidate_info(packet)

    matched_source = ztf_rosat_crossmatch(ztf_source, rosat_skycoord, dfx)
    if matched_source is not None:
        if not_moving_object(packet):
            with lock:
                saved_packets.append(packet)
            #if not is_excluded_simbad_class(ztf_source):
            #    ingest_growth_marshal(ztf_source['candid'])


def check_simbad_and_save(packets_to_simbad, lock_packets_to_simbad):
    with lock_packets_to_simbad:
        logging.info(f'{len(packets_to_simbad)} being sent to Simbad for matching')
        ras = []
        decs = []
        for packet in packets_to_simbad:
            ras.append(packet['candidate']['ra'])
            decs.append(packet['candidate']['dec'])

        sc = SkyCoord(ras, decs, unit=u.degree)

        MATCH_RADIUS = 2*u.arcsecond 
        try:
            result_table = customSimbad.query_region(sc, radius=MATCH_RADIUS)
        except Exception as e:
            logging.exception("Error querying Simbad",e)

        # annoyingly, the result table is unsorted and unlabeled, so we have
        # to re-match it to our packets
        
        sc_simbad = SkyCoord(result_table['RA'], result_table['DEC'], 
                unit=('hourangle','degree'))

        idx, sep2d, _ = match_coordinates_sky(sc, sc_simbad)
        assert( len(sc) == len(idx))
        
        for i, packet in enumerate(packets_to_simbad):
            # check if we have a simbad match for each packet we sent
            if sep2d[i] <= MATCH_RADIUS:
                matched_row = result_table[idx[i]]
                otype = matched_row['OTYPE_3'].decode("utf-8")
                simbad_id = matched_row['MAIN_ID'].decode("utf-8")
                if otype in SIMBAD_EXCLUDES:
                    logging.info(f"{packet['objectId']} found in Simbad as {simbad_id} ({otype}); omitting")
                else:
                    logging.info(f"{packet['objectId']} found in Simbad as {simbad_id} ({otype}); saving")
                    ingest_growth_marshal(packet['candid'])
            else:
                # no match in simbad
                logging.info(f"{packet['objectId']} not found in Simbad") 
                ingest_growth_marshal(packet['candid'])







def main():


    
    parser = argparse.ArgumentParser()
    parser.add_argument('date', type = str, help = 'UTC date as YYMMDD')
    parser.add_argument('program_id', type = int, help = 'Program ID (1 or 2)')

    args = parser.parse_args()

    if len(args.date) != 6:
        raise ValueError(f'Date must be specified as YYMMDD.  Provided {args.date}')

    if args.program_id not in [1,2]:
        raise ValueError(f'Program id must be 1 or 2.  Provided {args.program_id}')
    

    kafka_topic = f"ztf_20{args.date}_programid{args.program_id}"
    kafka_server = "partnership.alerts.ztf.uw.edu:9092"

    LOGGING['handlers']['logfile']['filename'] = f'{BASE_DIR}/../logs/{kafka_topic}.log'
    logging.config.dictConfig(LOGGING)

    # load X-ray catalogs
    dfx, rosat_skycoord = load_rosat()
    

    logging.info(f"Connecting to Kafka topic {kafka_topic}")


    loop = asyncio.get_event_loop()

    async def consume():
        consumer = AIOKafkaConsumer(
            kafka_topic, #other_topic,
            loop=loop, bootstrap_servers=kafka_server,
            auto_offset_reset="earliest",
            value_deserializer=read_avro_bytes,
            group_id="uw_xray")
        # Get cluster layout and join group `my-group`
        await consumer.start()
        tstart = time.perf_counter()
        tbatch = tstart
        i=0
        nmod = 1000
        pool = ThreadPoolExecutor(max_workers=64)
        packets_from_kafka = []
        packets_to_simbad = []
        lock_packets_from_kafka = Lock()
        lock_packets_to_simbad = Lock()
        try:
            # Consume messages
            async for msg in consumer:
                i+=1
                if i % nmod  == 0:
                    elapsed = time.perf_counter() - tstart
                    logging.info(f'Consumed {i} messages in {elapsed:.1f} sec ({i/elapsed:.1f} messages/s)')


                # query simbad in batches
                if time.perf_counter() - tbatch >= 40:
                    with lock_packets_from_kafka:
                        with lock_packets_to_simbad:
                            packets_to_simbad = deepcopy(packets_from_kafka)
                            packets_from_kafka=[]
                    if len(packets_to_simbad) > 0:
                        loop.run_in_executor(pool, 
                            functools.partial(check_simbad_and_save,
                                packets_to_simbad, 
                                lock_packets_to_simbad))
                    tbatch = time.perf_counter()

                #print("consumed: ", msg.topic, msg.partition, msg.offset,
                #      msg.key, msg.timestamp)
                
                packet = msg.value

                loop.run_in_executor(pool, 
                        functools.partial(process_packet, 
                            packet, rosat_skycoord, dfx, packets_from_kafka, 
                            lock_packets_from_kafka))

        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()
            pool.shutdown(wait=True)
            # TODO: flush out saved packets

    loop.run_until_complete(consume())
