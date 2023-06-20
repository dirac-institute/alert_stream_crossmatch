#!/usr/bin/env python
# By David Wang
# Find outbursting sources in ZTF >3 mag brighter than ps counterpart

import io
import gzip
import tarfile
import time
import datetime
import argparse
import logging
from copy import deepcopy
import numpy as np
import pandas as pd
import fastavro
from astropy.coordinates import SkyCoord, match_coordinates_sky
import astropy.units as u
from astropy.io import fits
import os
import multiprocessing as mp

from astroquery.simbad import Simbad
from utils.constants import UTCFormatter, LOGGING, BASE_DIR, DB_DIR, FITS_DIR, CATALOG_DIR, SIMBAD_EXCLUDES, GROUP_ID_PREFIX, KAFKA_TIMEOUT, ARCHIVAL_DIR
from utils.db_caching import create_connection, insert_data, update_value, insert_lc_dataframe, \
    get_cached_ids, init_db, add_db2_to_db1


SIGMA_TO_95pctCL = 1.95996


def exception_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.exception(e)

    return wrapper

@exception_handler
def galactic_latitude(self, ra, dec):
    # l_ref = 33.012 # deg
    ra_ref = 282.25 # deg
    g = 62.6 # deg
    b =  np.arcsin(np.sin(np.deg2rad(dec)) * np.cos(np.deg2rad(g)) - \
                   np.cos(np.deg2rad(dec)) * np.sin(np.deg2rad(g)) * np.sin(np.deg2rad(ra) - np.deg2rad(ra_ref)))
    return np.rad2deg(b)

@exception_handler
def read_avro_file(fname):
    """Reads a single packet from an avro file stored with schema on disk."""
    with open(fname, "rb") as f:
        freader = fastavro.reader(f)
        for packet in freader:
            return packet


@exception_handler
def read_avro_bytes(buf):
    """Reads a single packet from an avro file stored with schema on disk."""
    with io.BytesIO(buf) as f:
        freader = fastavro.reader(f)
        for packet in freader:
            return packet


@exception_handler
def get_candidate_info(packet):
    info = {"ra": packet["candidate"]["ra"], "dec": packet["candidate"]["dec"],
            "object_id": packet["objectId"], "candid": packet["candid"],
            "fid":  packet["candidate"]["fid"], "magpsf":  packet["candidate"]["magpsf"]}
    # if packet["candidate"]["distpsnr1"] is None:
    #     info["distpsnr1"] = -999
    for i in range(1, 4):
        if (packet["candidate"][f"distpsnr{i}"] < 2) and (packet["candidate"][f"distpsnr{i}"] > 0):
            info[f"distpsnr{i}"] = packet["candidate"][f"distpsnr{i}"]
            info[f"sgmag{i}"] = packet["candidate"][f"sgmag{i}"]
            info[f"srmag{i}"] = packet["candidate"][f"srmag{i}"]
            info[f"simag{i}"] = packet["candidate"][f"simag{i}"]
            info[f"sgscore{i}"] = packet["candidate"][f"sgscore{i}"]
            info[f"objectidps{i}"] = packet["candidate"][f"objectidps{i}"]
    return info



@exception_handler
def save_cutout_fits(packet, output):
    """Save fits cutouts from packed into output."""
    objectId = packet["objectId"]
    for im_type in ["Science", "Template", "Difference"]:
        with gzip.open(io.BytesIO(packet[f"cutout{im_type}"]["stampData"]), "rb") as f:
            with fits.open(io.BytesIO(f.read())) as hdul:
                hdul.writeto(f"{output}/{objectId}_{im_type}.fits", overwrite=True)


@exception_handler
def make_dataframe(packet, repeat_obs=True):
    """Extract relevant lightcurve data from packet into pandas DataFrame."""
    df = pd.DataFrame(packet["candidate"], index=[0])
    columns = ["ZTF_object_id", "jd", "fid", "magpsf", "sigmapsf", "diffmaglim",
               "isdiffpos", "magnr", "sigmagnr", "field", "rcid"]

    if repeat_obs:
        df["ZTF_object_id"] = packet["objectId"]
        return df[columns]

    df_prv = pd.DataFrame(packet["prv_candidates"])
    df_merged = pd.concat([df, df_prv], ignore_index=True)
    df_merged["ZTF_object_id"] = packet["objectId"]
    return df_merged[columns]


@exception_handler
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
    if packet['prv_candidates'] is None:
        logging.info("prv_candidates is None")
        return False
    if len(packet['prv_candidates']) == 0:
        logging.debug("No previous detections of {packet['objectId']}")
        return False

    for prv_candidate in packet['prv_candidates']:
        if prv_candidate['candid'] is not None:
            date1 = float(prv_candidate['jd'])
            diff_date = date0 - date1
            if diff_date < (0.5 / 24):
                continue
            else:
                # logging.debug(f"Previous detection of {packet['objectId']} {diff_date} days earlier")
                return True

    return False


customSimbad = Simbad()
customSimbad.add_votable_fields("otype(3)")
customSimbad.add_votable_fields("otypes(3)")  # returns a '|' separated list of all the otypes


@exception_handler
def query_simbad(ra, dec):
    sc = SkyCoord(ra, dec, unit=u.degree)
    result_table = customSimbad.query_region(sc, radius=2 * u.arcsecond)
    return result_table


def is_excluded_simbad_class(ztf_source):
    """Is the object in Simbad, with object types we reject (e.g., AGN)?
    """
    try:
        result_table = query_simbad(ztf_source['ra'], ztf_source['dec'])
        if result_table is None:
            logging.info(f"{ztf_source['object_id']} not found in Simbad")
            return False  # not in Simbad
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
        logging.exception(f"Error querying Simbad for {ztf_source['object_id']}", e)
        return False

def mag_difference(ztf_source):
    """
    Calculates the magnitude difference between the source and magpsf values for different bands.

    Args:
        ztf_source (dict): Dictionary containing source information, including filter IDs and magnitudes.

    Returns:
        int: The index (1, 2, or 3) corresponding to the first magnitude that has a difference greater than 3.
             Returns 0 if no magnitude difference greater than 3 is found.

    """
    # Dictionary mapping filter IDs to corresponding filter colors
    filter_color = {1: "g", 2: "r", 3: "i"}

    # Determine the filter color based on the fid (filter ID) of the source
    band = filter_color[ztf_source["fid"]]

    # Iterate through possible bands (g, r, i) to find a magnitude difference
    for i in range(1, 4):
        # Check if the source has the corresponding sgmag key
        if f"sgmag{i}" in ztf_source.keys():
            # Calculate the magnitude difference between the source's sgmag and magpsf
            difference = ztf_source[f"s{band}mag{i}"] - ztf_source["magpsf"]

            # If the magnitude difference is greater than 3, return the corresponding index i
            if difference > 3:
                return i

    # Return 0 if no magnitude difference greater than 3 is found
    return 0

@exception_handler
def save_to_db(packet, otype, sources_saved, database, interest):
    """Save matches to database
    """
    ztf_object_id = packet["objectId"]
    data_to_update = {"SIMBAD_otype": f'"{otype}"', "ra": packet["candidate"]["ra"],
                      "dec": packet["candidate"]["dec"], "SIMBAD_include": interest,
                      "last_obs": packet['candidate']['jd'], "seen_flag": 0, "interest_flag": interest}
    logging.info(f"Saving new source {ztf_object_id} to database.")

    conn = create_connection(database)
    if ztf_object_id in sources_saved:
        logging.info(f"{ztf_object_id} already saved in time between simbad check and now")
        dflc = make_dataframe(packet, repeat_obs=True)
    else:
        update_value(conn, data_to_update, f'ZTF_object_id = "{ztf_object_id}"')
        dflc = make_dataframe(packet, repeat_obs=True) # only save the current obs for archival xmatch
        # cache_ZTF_object(conn, (ztf_object_id, simbad_otype, ra, dec, rosat_iau_name))
        logging.debug(f"Successfully saved new source {ztf_object_id} to database.")
        # save_cutout_fits(packet, FITS_DIR)
        logging.debug(f"Successfully saved cutouts of {ztf_object_id}")
    insert_lc_dataframe(conn, dflc)
    logging.debug(f"Successfully ingested lightcurve data from {ztf_object_id} to database.")
    sources_saved.update((ztf_object_id,))
    conn.close()


@exception_handler
def check_for_new_sources(packets_to_simbad, sources_saved, database):
    """Checks the packets_to_simbad for ZTF objects not previously saved to the database.
    """
    new_packets = [packet for packet in packets_to_simbad if packet["objectId"] not in sources_saved]
    old_packets = [packet for packet in packets_to_simbad if packet["objectId"] in sources_saved]

    logging.debug("New sources: {}".format(", ".join([packet["objectId"] for packet in new_packets])))
    if len(new_packets) < len(packets_to_simbad):
        logging.info(f"{len(packets_to_simbad) - len(new_packets)} seen before")

    for packet in old_packets:  # If we've seen this star before
        ztf_object_id = packet["objectId"]
        conn = create_connection(database)
        # Add to lightcurve
        dflc = make_dataframe(packet, repeat_obs=True)
        insert_lc_dataframe(conn, dflc)
        logging.debug(f"Successfully updated lightcurve data from {ztf_object_id} to database.")
        # Save most recent cutout
        # save_cutout_fits(packet, FITS_DIR)
        logging.debug(f"Successfully updated cutouts of {ztf_object_id}")
        # Update some of the table values: last_obs...
        data_to_update = {"last_obs": packet["candidate"]["jd"]}
        update_value(conn, data_to_update, f'ZTF_object_id = "{ztf_object_id}"')
        conn.close()

    return new_packets

# TODO: add criteria for amplitude cut
@exception_handler
def process_ac_packet(packet, saved_packets, sources_seen, database):
    """Examine packet for matches in the ROSAT database. Save object to database if match found"""
    rb_key = "drb" if "drb" in packet["candidate"].keys() else 'rb'
    if packet["candidate"][rb_key] < 0.8:  # if packet real/bogus score is low, ignore
        return
    # # Not a solar system object (or no known obj within 5")
    if not((packet["candidate"]['ssdistnr'] is None) or (packet["candidate"]['ssdistnr'] < 0) or (packet["candidate"]['ssdistnr'] > 5)):
        return

    # Read in avro
    ztf_source = get_candidate_info(packet)

    # If moving, return
    if not not_moving_object(packet):
        return

    conn = create_connection(database)
    if packet["objectId"] in sources_seen:
        logging.debug(f"{packet['objectId']} already known match, adding packet to packets_from_kafka")
        saved_packets.append(packet)
        sources_seen.update((packet["objectId"],))
        conn.close()
        logging.debug(f"Total of {len(saved_packets)} saved for query.")
    else:
        pass_ac = mag_difference(ztf_source)
        if pass_ac:
            logging.debug("adding packet to packets_from_kafka")
            saved_packets.append(packet)
            sources_seen.update((packet["objectId"],))

            i = pass_ac
            data_to_insert = {"ZTF_object_id": packet["objectId"], "distpsnr": ztf_source[f"distpsnr{i}"],
                              "sgmag": ztf_source[f"sgmag{i}"], "srmag": ztf_source[f"srmag{i}"],
                              "simag": ztf_source[f"simag{i}"], "objectidps": ztf_source[f"objectidps{i}"],
                              "sgscore": ztf_source[f"sgscore{i}"]}
            insert_data(conn, "ZTF_objects", data_to_insert)
            logging.debug(f"Successfully saved {packet['objectId']} to database")
            conn.close()
            logging.debug(f"Total of {len(saved_packets)} saved for query.")


@exception_handler
def check_simbad_and_save(packets_to_simbad, sources_saved, database):
    logging.info("Checking packets for new sources")
    new_packets_to_simbad = check_for_new_sources(packets_to_simbad, sources_saved, database)
    logging.debug(f"{len(packets_to_simbad) - len(new_packets_to_simbad)} sources already cached.")

    # Return if sources were previously seen and recorded
    if len(new_packets_to_simbad) == 0:
        logging.debug(f"All {len(packets_to_simbad)} were previously seen.")
        return None

    logging.info(f"{len(new_packets_to_simbad)} being sent to Simbad for matching")
    ras = []
    decs = []
    for packet in new_packets_to_simbad:
        ras.append(packet["candidate"]["ra"])
        decs.append(packet["candidate"]["dec"])

    sc = SkyCoord(ras, decs, unit=u.degree)

    MATCH_RADIUS = 2 * u.arcsecond

    sep2d = None
    result_table = None

    try:
        result_table = customSimbad.query_region(sc, radius=MATCH_RADIUS)  # TODO: examine result_table for errors
        # annoyingly, the result table is unsorted and unlabeled, so we have
        # to re-match it to our packets
        if result_table is not None:
            sc_simbad = SkyCoord(result_table["RA"], result_table["DEC"],
                                 unit=("hourangle", "degree"))
            idx, sep2d, _ = match_coordinates_sky(sc, sc_simbad)
            # assert(len(sc) == len(idx))
        else:
            logging.info("No matches found in SIMBAD")
            # return

    except Exception as e:
        logging.exception("Error querying Simbad", e)

    for i, packet in enumerate(new_packets_to_simbad):
        # check if we have a simbad match for each packet we sent
        if sep2d[i] <= MATCH_RADIUS:
            matched_row = result_table[idx[i]]
            otype = matched_row["OTYPE_3"].decode("utf-8")
            simbad_id = matched_row["MAIN_ID"].decode("utf-8")
            if otype in SIMBAD_EXCLUDES:
                logging.info(f"{packet['objectId']} found in Simbad as {simbad_id} ({otype}); omitting")
                save_to_db(packet, otype, sources_saved, database, interest=0)
            else:
                logging.info(f"{packet['objectId']} found in Simbad as {simbad_id} ({otype}); saving")
                save_to_db(packet, otype, sources_saved, database, interest=1)

        else:
            # no match in simbad,
            logging.info(f"{packet['objectId']} not found in Simbad")
            save_to_db(packet, None, sources_saved, database, interest=1)


def main():
#     parser = argparse.ArgumentParser()
    #parser.add_argument("date", type=str, help="UTC date as YYMMDD")
    #parser.add_argument("program_id", type=int, help="Program ID (1 or 2)")
#     parser.add_argument("tarball", type=str, help="name of tarball file (.tar.gz)")
    # parser.add_argument("suffix", type=str, help="suffix of db")

#     args = parser.parse_args()
#     suffix = 'archival'
    # if len(args.date) != 6:
    #     raise ValueError(f"Date must be specified as YYMMDD.  Provided {args.date}")

    # if args.program_id not in [1, 2]:
    #     raise ValueError(f"Program id must be 1 or 2.  Provided {args.program_id}")

    # TIMESTAMP = '20' + args.date
    # program = 'public' if args.program_id == 1 else 'partnership'
#     tarball_path = args.tarball # .split("/")[-1] # f'ztf_{program}_{TIMESTAMP}.tar.gz'
    # tarball_dir = ARCHIVAL_DIR + program + '/' + tarball_name
    test = open("alert_list.txt")
    alerts = test.read().splitlines()# test.readlines()
    print(alerts)
    ncpus=20
    os.nice(1)
    p = mp.Pool(ncpus)
    p.map(consume_tarball, alerts)
    # consume_tarball(tarball_path)

def consume_tarball(tarball_path):
    tar = tarfile.open(tarball_path)

    # database = DB_DIR + f"sqlite_{suffix}.db"
    # logging.info(f"Database at {database}")


    tarball_day = tarball_path.split("/")[-1].split(".")[0].split("_")[-1]
    init_db(f'_{tarball_day}', subfolder='archival/')
    database = DB_DIR + f'archival/sqlite_{tarball_day}.db'

    now = datetime.datetime.now().strftime("%d%m%y_%H%M%S")
    LOGGING["handlers"]["logfile"]["filename"] = f"{BASE_DIR}../local/logs/archival/archival_ztf{tarball_day}_{now}.log"
    logging.config.dictConfig(LOGGING)

    logging.info(f"Database at {database}")

    # Get cluster layout and join group `my-group`
    tstart = time.perf_counter()
    tbatch = tstart
    i = 0
    nmod = 1000
    packets_from_kafka = []
    packets_to_simbad = []
    # conn = create_connection(database)
    all_db = DB_DIR + 'sqlite_all3.db'
    total_conn = create_connection(database)
    sources_saved = set(get_cached_ids(total_conn).values)    # These are sources in both tables (ZTF_objects and lightcurves)
    sources_seen = set(get_cached_ids(total_conn).values)     # These are sources only in ZTF objects
    logging.info(f"{len(sources_saved)} sources previously seen")
    n0_sources = len(sources_saved)
    total_conn.close()
    logging.info('begin ingesting messages, {} total'.format(len(tar.getmembers())))
    try:
        for tarpacket in tar.getmembers():
            i += 1
            if i % nmod == 0:
                elapsed = time.perf_counter() - tstart
                logging.info(f'Consumed {i} messages in {elapsed:.1f} sec ({i / elapsed:.1f} messages/s)')
                logging.info(f'Matched {len(sources_saved) - n0_sources} sources seen in {elapsed:.1f} sec')

            # query simbad in batches
            if time.perf_counter() - tbatch >= 40:
                logging.debug("start simbad query process")
                logging.debug('copying packets from kafka to simbad') # .format(len(packets_from_kafka)))
                packets_to_simbad = deepcopy(packets_from_kafka)
                packets_from_kafka = []
                logging.debug("determine if there are packets to simbad")
                if len(packets_to_simbad) > 0:
                    logging.info(f'{len(packets_to_simbad)} packets to query')
                    check_simbad_and_save(
                        packets_to_simbad,
                        sources_saved, database)
                tbatch = time.perf_counter()

            packet = read_avro_bytes(tar.extractfile(tarpacket).read())
            process_ac_packet(packet, packets_from_kafka, sources_seen, database)

    except Exception as e:
        logging.exception(e)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        logging.debug("start last simbad query process")
        logging.debug('copying last packets from kafka to simbad')  # .format(len(packets_from_kafka)))
        packets_to_simbad = deepcopy(packets_from_kafka)
        logging.debug("determine if there are packets to simbad")
        if len(packets_to_simbad) > 0:
            logging.info(f'{len(packets_to_simbad)} packets to query')
            check_simbad_and_save(
                packets_to_simbad,
                sources_saved, database)
        tbatch = time.perf_counter()
        logging.info(f"finished consuming all packets in {tarball_path.split('/')[-1]}")
        add_db2_to_db1(all_db, database)
        logging.info(f"added rows from {database} to {all_db}")
        # TODO: flush out saved packets

if __name__ == "__main__":
    main()
