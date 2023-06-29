#!/usr/bin/env python
# By Myles McKay
# June 7, 2020
# ZTF crossmatch with X-Ray Binaries (ROSAT catalog)


import io
import time
import datetime
import argparse
import logging
import gzip
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

from astroquery.simbad import Simbad
from .constants import LOGGING, BASE_DIR, DB_DIR, SIMBAD_EXCLUDES # FITS_DIR
from .db_caching import create_connection, insert_data, update_value, insert_lc_dataframe, get_cached_ids

# Example command line execution:

# for date 210119, program_id 1, 64 cores, suffix 64t
# $ python ztf_rosat_crossmatch.py 210119 1 64 64t

SIGMA_TO_95pctCL = 1.95996
FITS_DIR = "../local/cutouts_debug"

def exception_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.exception(e)

    return wrapper


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
    return {"ra": packet["candidate"]["ra"], "dec": packet["candidate"]["dec"],
            "object_id": packet["objectId"], "candid": packet["candid"]}


@exception_handler
def save_cutout_fits(packet, output):
    """Save fits cutouts from packed into output."""
    objectId = packet["objectId"]
    pid = packet["candidate"]["pid"]
    for im_type in ["Science", "Template", "Difference"]:
        with gzip.open(io.BytesIO(packet[f"cutout{im_type}"]["stampData"]), "rb") as f:
            with fits.open(io.BytesIO(f.read())) as hdul:
                hdul.writeto(f"{output}/{objectId}_{pid}_{im_type}.fits", overwrite=True)


@exception_handler
def make_dataframe(packet, repeat_obs=True):
    """Extract relevant lightcurve data from packet into pandas DataFrame."""
    df = pd.DataFrame(packet["candidate"], index=[0])
    if repeat_obs:
        df["ZTF_object_id"] = packet["objectId"]
        return df[["ZTF_object_id", "jd", "fid", "magpsf", "sigmapsf", "diffmaglim"]]

    df_prv = pd.DataFrame(packet["prv_candidates"])
    df_merged = pd.concat([df, df_prv], ignore_index=True)
    df_merged["ZTF_object_id"] = packet["objectId"]
    return df_merged[["ZTF_object_id", "jd", "fid", "magpsf", "sigmapsf", "diffmaglim"]]


@exception_handler
def load_rosat():
    # Open ROSAT catalog
    rosat_fits = fits.open("/epyc/data/rosat_2rxs/cat2rxs.fits")

    # Make ROSAT data into a pandas dataframe
    rosat_data = rosat_fits[1].data
    dfx = pd.DataFrame(rosat_data)
    rosat_fits.close()

    # exclude sources that are not observable
    dfx = dfx[dfx.DEC_DEG >= -30]

    # List ROSAT error position [arcsec]
    dfx["err_pos_arcsec"] = np.sqrt(((dfx.XERR * 45) ** 2. + (dfx.YERR * 45) ** 2.) + 0.6 ** 2.) * SIGMA_TO_95pctCL

    # Put ROSAT ra and dec list in SkyCoord [degrees]
    rosat_skycoord = SkyCoord(ra=dfx.RA_DEG, dec=dfx.DEC_DEG,
                              frame="icrs", unit=(u.deg))

    return dfx[["IAU_NAME", "RA_DEG", "DEC_DEG", "err_pos_arcsec"]], rosat_skycoord


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
    try:
        # Input avro data ra and dec in SkyCoords
        avro_skycoord = SkyCoord(ra=ztf_source["ra"], dec=ztf_source["dec"],
                                 frame="icrs", unit=(u.deg))

        # Finds the nearest ROSAT source's coordinates to the avro files ra[deg] and dec[deg]
        match_idx, match_sep2d, _ = avro_skycoord.match_to_catalog_sky(rosat_skycoord)

        match_row = dfx.iloc[match_idx]

        matched = match_sep2d[0] <= match_row["err_pos_arcsec"] * u.arcsecond

        match_result = {"match_name": match_row["IAU_NAME"],
                        "match_ra": match_row["RA_DEG"],
                        "match_dec": match_row["DEC_DEG"],
                        "match_err_pos": match_row["err_pos_arcsec"],
                        "match_sep": match_sep2d[0].to(u.arcsecond).value}

        if matched:
            logging.info(
                f"{ztf_source['object_id']} ({avro_skycoord.to_string('hmsdms')}; {ztf_source['candid']}) matched {match_result['match_name']} ({match_result['match_sep']:.2f} arcsec away)")
            return match_result

        else:
            # logging.debug(f"{ztf_source['object_id']} ({avro_skycoord.to_string('hmsdms')}) did not match (nearest source {match_result['match_name']}, {match_result['match_sep']:.2f} arcsec away")
            return None
    except Exception as e:
        logging.exception(f"Unable to crossmatch {ztf_source['object_id']} with ROSAT", e)


@exception_handler
def save_to_db(packet, otype, sources_saved, lock_sources_saved, database, interest):
    """Save matches to database
    """
    ztf_object_id = packet["objectId"]
    data_to_update = {"SIMBAD_otype": f'"{otype}"', "ra": packet["candidate"]["ra"],
                      "dec": packet["candidate"]["dec"], "SIMBAD_include": interest}
    logging.info(f"Saving new source {ztf_object_id} to database.")

    with lock_sources_saved:
        conn = create_connection(database)
        if ztf_object_id in sources_saved:
            logging.info(f"{ztf_object_id} already saved in time between simbad check and now")
            dflc = make_dataframe(packet, repeat_obs=True)
        else:
            update_value(conn, data_to_update, f'ZTF_object_id = "{ztf_object_id}"')
            dflc = make_dataframe(packet, repeat_obs=False)
            # cache_ZTF_object(conn, (ztf_object_id, simbad_otype, ra, dec, rosat_iau_name))
            logging.debug(f"Successfully saved new source {ztf_object_id} to database.")
        insert_lc_dataframe(conn, dflc)
        logging.debug(f"Successfully ingested lightcurve data from {ztf_object_id} to database.")
        save_cutout_fits(packet, FITS_DIR)
        logging.debug(f"Successfully saved cutouts of {ztf_object_id}")
        sources_saved.update((ztf_object_id,))
        conn.close()


@exception_handler
def check_for_new_sources(packets_to_simbad, sources_saved, lock_sources_saved, database):
    """Checks the packets_to_simbad for ZTF objects not previously saved to the database.
    """
    logging.debug(f"Checking for new sources...")
    with lock_sources_saved:
        new_packets = [packet for packet in packets_to_simbad if packet["objectId"] not in sources_saved]
        old_packets = [packet for packet in packets_to_simbad if packet["objectId"] in sources_saved]
        logging.debug("New sources: {}".format(", ".join([packet["objectId"] for packet in new_packets])))
        if len(new_packets) < len(packets_to_simbad):
            logging.info(f"{len(packets_to_simbad) - len(new_packets)} seen before")

    logging.debug("Updating info for previously seen sources...")
    for packet in old_packets:
        ztf_object_id = packet["objectId"]
        conn = create_connection(database)
        dflc = make_dataframe(packet, repeat_obs=True)
        insert_lc_dataframe(conn, dflc)
        logging.debug(f"Successfully updated lightcurve data from {ztf_object_id} to database.")
        save_cutout_fits(packet, FITS_DIR)
        logging.debug(f"Successfully updated cutouts of {ztf_object_id}")
        conn.close()

    return new_packets


@exception_handler
def process_packet(packet, rosat_skycoord, dfx, saved_packets, lock, sources_seen, lock_sources_seen, database):
    """Examine packet for matches in the ROSAT database. Save object to database if match found"""
    if packet["candidate"]["drb"] < 0.8:  # if packet real/bogus score is low, ignore
        return
    ztf_source = get_candidate_info(packet)
    # conn = create_connection(database)
    with lock:
        with lock_sources_seen:
            if packet["objectId"] in sources_seen:
                # logging.debug(f"{packet['objectId']} already known match, adding packet to packets_from_kafka")
                saved_packets.append(packet)
                sources_seen.update((packet["objectId"],))
                # conn.close()
                logging.debug(f"Total of {len(saved_packets)} saved for query.")
            else:
                matched_source = ztf_rosat_crossmatch(ztf_source, rosat_skycoord, dfx)
                if (matched_source is not None) and not_moving_object(packet):
                    # logging.debug("adding packet to packets_from_kafka")
                    saved_packets.append(packet)
                    sources_seen.update((packet["objectId"],))
                    conn = create_connection(database)
                    data_to_insert = {"ZTF_object_id": packet["objectId"],
                                      "ROSAT_IAU_NAME": matched_source["match_name"]}
                    insert_data(conn, "ZTF_objects", data_to_insert)
                    logging.debug(f"Successfully saved {packet['objectId']} to database")
                    conn.close()
                    logging.debug(f"Total of {len(saved_packets)} saved for query.")


@exception_handler
def check_simbad_and_save(packets_to_simbad, lock_packets_to_simbad, sources_saved, lock_sources_saved, database):
    with lock_packets_to_simbad:
        logging.info("Checking packets for new sources")
        new_packets_to_simbad = check_for_new_sources(packets_to_simbad, sources_saved,
                                                      lock_sources_saved, database)
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
                return

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
                    save_to_db(packet, otype, sources_saved, lock_sources_saved, database, interest=0)
                else:
                    logging.info(f"{packet['objectId']} found in Simbad as {simbad_id} ({otype}); saving")
                    save_to_db(packet, otype, sources_saved, lock_sources_saved, database, interest=1)

            else:
                # no match in simbad,
                logging.info(f"{packet['objectId']} not found in Simbad")
                save_to_db(packet, None, sources_saved, lock_sources_saved, database, interest=0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", type=str, help="UTC date as YYMMDD")
    parser.add_argument("program_id", type=int, help="Program ID (1 or 2)")
    parser.add_argument("threads", type=int, help="Number of threads (1 to 64)")
    parser.add_argument("suffix", type=str, help="suffix of db")

    args = parser.parse_args()

    if len(args.date) != 6:
        raise ValueError(f"Date must be specified as YYMMDD.  Provided {args.date}")

    if args.program_id not in [1, 2]:
        raise ValueError(f"Program id must be 1 or 2.  Provided {args.program_id}")

    if (args.threads < 1) or (args.threads > 64):
        raise ValueError(f"threads must be between 1 and 64.  Provided {args.threads}")

    database = DB_DIR + f"sqlite{args.suffix}.db"
    logging.debug(f"Database at {database}")

    kafka_topic = f"ztf_20{args.date}_programid{args.program_id}"
    kafka_server = "partnership.alerts.ztf.uw.edu:9092"

    now = datetime.datetime.now().strftime("%d%m%y_%H%M%S")
    LOGGING["handlers"]["logfile"]["filename"] = f"{BASE_DIR}/../logs/{kafka_topic}_{now}.log"
    logging.config.dictConfig(LOGGING)

    logging.debug(f"Args parsed and validated: {args.date}, {args.program_id}")

    # load X-ray catalogs
    dfx, rosat_skycoord = load_rosat()

    logging.info(f"Connecting to Kafka topic {kafka_topic}")

    loop = asyncio.get_event_loop()

    async def consume():
        consumer = AIOKafkaConsumer(
            kafka_topic,  # other_topic,
            loop=loop, bootstrap_servers=kafka_server,
            auto_offset_reset="earliest",
            value_deserializer=read_avro_bytes,
            group_id=f"uw_xray_{args.suffix}")
        # Get cluster layout and join group `my-group`
        await consumer.start()
        tstart = time.perf_counter()
        tbatch = tstart
        i = 0
        nmod = 1000
        pool = ThreadPoolExecutor(max_workers=args.threads)
        packets_from_kafka = []
        packets_to_simbad = []
        try:
            conn = create_connection(database)
        except Exception as e:
            logging.exception(f"Could not connect to {database}", e)
            return
        sources_saved = set(get_cached_ids(conn).values)
        sources_seen = set(get_cached_ids(conn).values)
        logging.info(f"{len(sources_saved)} sources previously seen")
        n0_sources = len(sources_saved)
        conn.close()
        lock_packets_from_kafka = Lock()
        lock_packets_to_simbad = Lock()
        lock_sources_saved = Lock()
        lock_sources_seen = Lock()
        logging.debug("begin ingesting messages")
        try:
            # Consume messages
            async for msg in consumer:
                i += 1
                if i % nmod == 0:
                    elapsed = time.perf_counter() - tstart
                    logging.info(f"Consumed {i} messages in {elapsed:.1f} sec ({i / elapsed:.1f} messages/s)")
                    with lock_sources_saved:
                        logging.info(f"Matched {len(sources_saved) - n0_sources} sources seen in {elapsed:.1f} sec")

                # query simbad in batches
                if time.perf_counter() - tbatch >= 40:
                    with lock_packets_from_kafka:
                        with lock_packets_to_simbad:
                            logging.debug("copying {} packets from kafka to simbad".format(len(packets_from_kafka)))
                            packets_to_simbad = deepcopy(packets_from_kafka)
                            packets_from_kafka = []
                    if len(packets_to_simbad) > 0:
                        logging.debug(f"{len(packets_to_simbad)} packets to query")
                        loop.run_in_executor(pool,
                                             functools.partial(check_simbad_and_save,
                                                               packets_to_simbad,
                                                               lock_packets_to_simbad,
                                                               sources_saved,
                                                               lock_sources_saved,
                                                               database))
                    tbatch = time.perf_counter()

                # print("consumed: ", msg.topic, msg.partition, msg.offset,
                #      msg.key, msg.timestamp)

                packet = msg.value

                loop.run_in_executor(pool,
                                     functools.partial(process_packet,
                                                       packet, rosat_skycoord, dfx, packets_from_kafka,
                                                       lock_packets_from_kafka,
                                                       sources_seen, lock_sources_seen, database))

        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()
            pool.shutdown(wait=True)
            # TODO: flush out saved packets

    loop.run_until_complete(consume())