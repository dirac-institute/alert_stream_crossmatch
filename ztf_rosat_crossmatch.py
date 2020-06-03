#!/usr/bin/env python
import pandas as pd
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import fastavro
import aplpy
from astropy.coordinates import SkyCoord
import astropy.units as u
import glob
import argparse
from tqdm import tqdm
from tqdm.notebook import tqdm

# Example command line execution:

# $ python ztf_rosat_crossmatch.py --kafka_path="kafka://partnership.alerts.ztf.uw.edu/ztf_20200514_programid1" > kafka_output_20200514.txt

# Extract .avro RA and Dec 
#def extract_avro_data(fname):
#    """
#    Parameters:
#                fname: str
#                
#    
#    """
#    with open(fname,'rb') as f:
#        freader = fastavro.reader(f)
#        schema = freader.writer_schema # freader.schema - depercated 
#    
#        for packet in freader:
#            avro_ra = packet['candidate']['ra']
#            avro_dec = packet['candidate']['dec']
#
#    return avro_ra, avro_dec

def astroid_rejection(avro_packet):
    """Astriod rejection checks if the source has at least 2 previous detections > 30 mins apart"""
    avro_date0 = float(avro_packet['prv_candidates'][0]['jd'])
    num = 0
    for i in range(1,10):
        try:
            avro_date1 = float(avro_packet['prv_candidates'][i]['jd'])
        except IndexError as err:
            print(err)    
        else:
            diff_date = avro_date1 - avro_date0
            print(avro_date0, avro_date1, diff_date)
            if num == 2:
                print('real')
                break
            elif num < 2:
                if diff_date < 0.0208:
                    print('Within 30mins')
                    i += 1
                elif diff_date > 0.0208:
                    print('Longer 30mins')
                    num += 1
                    i += 1
        finally:
            if num == 2:
                real = True
            else:
                real = False
    return real

def ztf_rosat_crossmatch(avro_ra, avro_dec, rosat_skycoord, rosat_pos_err, avro_fname, avro_candid, avro_packet, ingest=False):
    """
    
    Cross match ZTF and ROSAT data using astropy.coordinates.SkyCoord
    
    Parameters:
                avro_ra: float or list of float
                
                avro_dec: float or list of float
                
                rosat_skycoord: float or list 
                
                rosat_ra_list: pandas.series
                
                rosat_dec_list: pandas/series
                
    Return:
        
                ztf_rosat_ra_match: float
                
                ztf_rosat_dec_match: float
                
                match: tuple
                    Output from Skycoord.match_to_catalog_sky
    
    """
    

    # Input avro data ra and dec in SkyCoords
    avro_skycoord = SkyCoord(ra=avro_ra, dec=avro_dec, frame='icrs', unit=(u.deg))
    
    
    # Finds the nearest ROSAT source's coordinates to the avro files ra[deg] and dec[deg]
    match = avro_skycoord.match_to_catalog_sky(catalogcoord=rosat_skycoord, nthneighbor=1, )
    #print(match)
    match_idx = match[0]
    match_sep2d = match[1].value #units [degree]
    
    # Collect filename of avro file
    fname = avro_fname.split('/')[-1]
    
    # Crossmatch ROSAT and ZTF sources within ROSAT position error
    if match_sep2d <= rosat_pos_err[match_idx] * 0.000277778:
        match_result = 'Good! sep2d={} deg'.format(match_sep2d)
        
        #Run astriod rejection
        result = astroid_rejection(avro_packet)
        if result is True:
            print(fname, avro_candid, avro_skycoord.ra, avro_skycoord.dec, 
              rosat_skycoord.ra[match_idx], rosat_skycoord.dec[match_idx],
              match_result)
            
            # Ingest matched data to GROWTH Marshal
            if ingest == True:
                candid = avro_candid # matched candid
                growth_marshal_ingestion(programid14, candid)
        else:
            pass
    
    else:
        pass
        #match_result = 'Not so good sep2d={} deg'.format(match_sep2d)
        #print(fname, avro_skycoord.ra, avro_skycoord.dec, rosat_skycoord.ra[match_idx], rosat_skycoord.dec[match_idx],
        #      match_result)
        
            

    return (avro_skycoord.ra, 
            avro_skycoord.dec, 
            rosat_skycoord.ra[match_idx], 
            rosat_skycoord.dec[match_idx], 
            match_sep2d, 
            avro_candid)
    
    
# Pulls the programidx from list_programs.cgi
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
    
    
def growth_marshal_ingestion(programidx, candid):
    # Using ingest_avro_id.cgi to save data to X-ray counterpart programid 14
    r = requests.post("http://skipper.caltech.edu:8080/cgi-bin/growth/ingest_avro_id.cgi",
                              auth=(username_marshal,
                                    password_marshal),
                              data={'programidx': programid14,
                                    'avroid': candid})
    try:
        r.raise_for_status()
        print('Ingestion successful')
        #print(f"Successfully ingested {s['name']} in \{args.ingest_program}")
        #out_string = f"{s['name']}, {s['candid']}, {args.ingest_program}\n"
        #ingested_file.write(out_string)
    except requests.exceptions.HTTPError as e:
        print("Error ingesting")
        print(e)    
    
    
def parse_args():
    """Parses command line arguments.

    Parameters:
        nothing

    Returns:
        args : argparse.Namespace object
            An argparse object containing all of the added arguments.

    Outputs:
        nothing
    """

    #Create help string:
    ztf_path_help = 'Path to the .avro files directory'
    kafka_path_help = 'Path to the .avro files directory'
    #rosat_skycoords_help = 'ROSAT catalog in astropy.coordinates.sky_coordinate.SkyCoord'
    # Add arguments:
    parser = argparse.ArgumentParser()
    #parser.add_argument('--ztf_path', '-ztf_path', dest = 'ztf_path', action = 'store',
    #                    type = str, required = False, help = ztf_path_help)
    parser.add_argument('--kafka_path', '-kafka_path', dest = 'kafka_path', action = 'store',
                        type = str, required = True, help = kafka_path_help)   
    #parser.add_argument('--rosat_skycoords', '-rosat_skycoords', dest = 'rosat_skycoords', action = 'store', 
    #                    type = SkyCoord,required = True, help = rosat_skycoords_help)


    # Parse args:
    args = parser.parse_args()

    return args
# -------------------------------------------------------------------
if __name__ == '__main__':
    args = parse_args()
    
    ######################################### ROSAT data collection  #############################################
    
    
    # Open ROSAT catalog
    from astropy.io import fits
    import numpy as np
    rosat_fits = fits.open('/epyc/users/mmckay/cat2rxs.fits')
    
    # Make ROSAT data into a pandas dataframe
    rosat_data = rosat_fits[1].data
    dfx = pd.DataFrame(rosat_data)
    
    # exclude sources that are not observable
    dfx = dfx[dfx.DEC_DEG >= -30] 
    
    # List of ROSAT RA and DEC
    rosat_ra_list = dfx.RA_DEG
    rosat_dec_list = dfx.DEC_DEG
    
    # List ROSAT error position [arcsec] 
    dfx['err_pos_arcsec'] = np.sqrt(((dfx.XERR*45)**2.+ (dfx.YERR*45)**2.) + 0.6**2.)
    err_pos_arcsec = dfx['err_pos_arcsec'].values
    
    # Put ROSAT ra and dec list in SkyCoord [degrees]
    rosat_skycoord = SkyCoord(ra=rosat_ra_list, dec=rosat_dec_list, frame='icrs', unit=(u.deg))
    
    
    ######################################### ROSAT data collection  #############################################

        
    # Running data stream
    import adc.streaming as adcs
    import requests
    from astropy.io import ascii
    
    # Read the secrets - Make a .csv and type username and password for GROTH Marshall 
    secrets = ascii.read('secrets.csv', format='csv')
    
    # GROWTH marshal credentials
    username_marshal = secrets['marshal_user'][0]
    password_marshal = secrets['marshal_pwd'][0]
    
    # Get programidx
    programid14 = get_programidx("X-ray Counterparts", username_marshal, password_marshal)
    
    print('Filename', 'candid', 'ZTF_RA_[deg]', 'ZTF_Dec_[deg]', 'ROSAT_RA_[deg], ROSAT_Dec_[deg], Sep2d')
    # "kafka://partnership.alerts.ztf.uw.edu/ztf_20200501_programid1"
    kafka_path = args.kafka_path
    with adcs.open(kafka_path, "r", format="avro", start_at="earliest") as stream:
        for nread, obj in enumerate(stream(progress=True, timeout=30), start=1):
        #for nread, obj in enumerate(stream(progress=True, timeout=False)):#, start=1):
            #print(nread,obj['objectId'])
            avro_fname = obj['objectId']
            avro_ra = obj['candidate']['ra']
            avro_dec = obj['candidate']['dec']
            avro_candid = obj['candidate']['candid']
            # any other processing
            stream.commit()
            
            avro_ra_match, avro_dec_match, rosat_ra_match, rosat_dec_match, match_2dsep, match_avro_candid = ztf_rosat_crossmatch(avro_ra, avro_dec, rosat_skycoord, err_pos_arcsec, avro_fname, avro_candid, obj, ingest=False)
            
    stream.commit(defer=False)
            # Astriod rejection checks if the source has a least 2 detections > 30 mins apart
            #avro_date0 = float(obj['prv_candidates'][0]['jd'])
            #num = 0
            #for i in range(1,10):
            #    try:
            #        avro_date1 = float(obj['prv_candidates'][i]['jd'])
            #    except IndexError as err:
            #        continue
            #        #print(err)    
            #    else:
            #        diff_date = avro_date1 - avro_date0
            #        print(avro_date0, avro_date1, diff_date)
            #        if num == 2:
            #            break
            #        elif num < 2:
            #            if diff_date < 0.0208:
            #                print('Within 30mins')
            #                i += 1
            #            elif diff_date > 0.0208:
            #                print('Longer than 30mins')
            #                num += 1
            #                i += 1
            #    finally:
            #        if num == 2:
            #            avro_ra_match, avro_dec_match, rosat_ra_match, rosat_dec_match, match_2dsep, match_avro_candid = ztf_rosat_crossmatch(avro_ra, avro_dec, rosat_skycoord, err_pos_arcsec, avro_fname, avro_candid, ingest=False)
            #        else:
            #            print('possible astriod: {}'.format(avro_candid))
            #            pass
    
        #stream.commit(defer=False)
    
            #Run Crossmatch function
            #avro_ra_match, avro_dec_match, rosat_ra_match, rosat_dec_match, match_2dsep, match_avro_candid = ztf_rosat_crossmatch(avro_ra, avro_dec, rosat_skycoord, err_pos_arcsec, avro_fname, avro_candid, ingest=False)
            
    #stream.commit(defer=False)








