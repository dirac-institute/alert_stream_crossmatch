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



# Extract .avro RA and Dec 
def extract_avro_data(fname):
    """
    Parameters:
                fname: str
                
    
    """
    with open(fname,'rb') as f:
        freader = fastavro.reader(f)
        schema = freader.writer_schema # freader.schema - depercated 
    
        for packet in freader:
            avro_ra = packet['candidate']['ra']
            avro_dec = packet['candidate']['dec']

    return avro_ra, avro_dec



def ztf_rosat_crossmatch(avro_ra, avro_dec, rosat_skycoord, rosat_pos_err, avro_fname):
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
    
    # Check that the match index is a numpy.ndarray
    #if type(match_idx) == np.ndarray:
    fname = avro_fname.split('/')[-1]
    if match_sep2d <= rosat_pos_err[match_idx] * 0.000277778:
        match_result = 'Good! sep2d={} deg'.format(match_sep2d)
        
        print(fname, avro_skycoord.ra, avro_skycoord.dec, rosat_skycoord.ra[match_idx], rosat_skycoord.dec[match_idx],
              match_result)
    
    else:
        #match_result = 'Not so good sep2d={} deg'.format(match_sep2d)
        #print(fname, avro_skycoord.ra, avro_skycoord.dec, rosat_skycoord.ra[match_idx], rosat_skycoord.dec[match_idx],
        #      match_result)
        pass
            
        
        
    return avro_skycoord.ra, avro_skycoord.dec, rosat_skycoord.ra[match_idx], rosat_skycoord.dec[match_idx], match_sep2d

    #else:
    #    print("Not able to crossmatch match_idx = {}".format(match_idx))
    #    pass
    
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
        
    #parser.add_argument('--rosat_skycoords', '-rosat_skycoords', dest = 'rosat_skycoords', action = 'store', type = SkyCoord, required = True, help = rosat_skycoords_help)



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
    
    # Put ROSAT ra and dec list in SkyCoord [degrees]
    rosat_skycoord = SkyCoord(ra=rosat_ra_list, dec=rosat_dec_list, frame='icrs', unit=(u.deg))
    
    
    ######################################### ROSAT data collection  #############################################

        
    #Running data stream
    import adc.streaming as adcs
    
    print('Filename', 'ZTF_RA_[deg]', 'ZTF_Dec_[deg]', 'ROSAT_RA_[deg], ROSAT_Dec_[deg], Sep2d')
    # "kafka://partnership.alerts.ztf.uw.edu/ztf_20200501_programid1"
    kafka_path = args.kafka_path
    with adcs.open(kafka_path, "r", format="avro", start_at="earliest") as stream:
        for nread, obj in enumerate(stream(progress=True, timeout=30), start=1):
            #print(nread,obj['objectId'])
            avro = obj['objectId']
            avro_ra = obj['candidate']['ra']
            avro_dec = obj['candidate']['dec']
            # any other processing
            stream.commit()
    
            #Run Crossmatch function
            avro_ra_match, avro_dec_match, rosat_ra_match, rosat_dec_match, match_2dsep = ztf_rosat_crossmatch(avro_ra, avro_dec, rosat_skycoord, dfx.err_pos_arcsec, avro)

    
    stream.commit(defer=False)
    
    
    
    
    
    
    
    # Store .avro files into a list to interate 
    #ztf_data_dir = args.ztf_path
    #avro_fname_list = glob.glob('{}*.avro'.format(ztf_data_dir))
    #print(avro_fname_list[:5])

    # Make a list to store 
    #avro_ra_list = []
    #avro_dec_list = []
    #
    #rosat_ra_list = []
    #rosat_dec_list = []
    #
    #sep2d_list = []
    
    # Run crossmatch on .avro files
    #print('Filename', 'ZTF_RA_[deg]', 'ZTF_Dec_[deg]', 'ROSAT_RA_[deg], ROSAT_Dec_[deg], Sep2d')
    
    #for avro in tqdm(avro_fname_list):
    #    
    #    # Extract ra and dec values from avro files
    #    avro_ra, avro_dec = extract_avro_data(avro)
    #    
    #    # Run crossmath functions
    #    avro_ra_match, avro_dec_match, rosat_ra_match, rosat_dec_match, match_2dsep = ztf_rosat_crossmatch(avro_ra, avro_dec, rosat_skycoord, dfx.err_pos_arcsec, avro)

        # Append data to a list if needed
        #avro_ra_list.append(avro_ra_match)
        #avro_dec_list.append(avro_dec_match)
        #rosat_ra_list.append(rosat_ra_match)
        #rosat_dec_list.append(rosat_dec_match)
        #sep2d_list.append(match_2dsep)
        














