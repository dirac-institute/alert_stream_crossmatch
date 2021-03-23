#!/usr/bin/env python
# By David Wang
# Sept 10, 2020
# Store queried ZTF objects in database

import sqlite3
import pandas as pd
from sqlite3 import Error
import os
import inspect
import pdb
import sys

# from .constants import DB_DIR

DB_DIR = '../local/db/'


def create_connection(db_file):
    """ Create a database connection to the SQLite database
        specified by db_file
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        raise Exception(f"Error creating connection {e}")

    return conn


def create_table(conn, create_table_sql):
    """ Create a table from the create_table_sql statement
    """
    try:
        cur = conn.cursor()
        cur.execute(create_table_sql)
        cur.close()
    except Error as e:
        raise Exception(f"Error creating table {e}")


def cache_ZTF_object(conn, ztf_object):
    """
    Add ZTF object to database.

    Parameters
    ----------
    conn: sqlite3.Connection object
        The connection to the database
    ztf_object: list or str
        Data to insert into the database in the follwoing form: (ZTF_object_id,SIMBAD_otype,ra,dec,ROSAT_IAU_NAME)

    Returns
    -------
    cur.lastrowid: int
        Id of the last row inserted into the database
    """

    sql = ''' INSERT INTO ZTF_objects(ZTF_object_id,SIMBAD_otype,ra,dec,ROSAT_IAU_NAME)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, ztf_object)
    conn.commit()
    return cur.lastrowid


def insert_data(conn, table, val_dict):
    cur = conn.cursor()
    try:
        cols = tuple(val_dict.keys())
        vals = tuple('{}'.format(val_dict[col]) for col in cols)
        if len(cols) == 1:
            cols = f"({cols[0]})"
            vals = f"('{vals[0]}')"
        cur.execute(f"INSERT INTO {table}{str(cols)} VALUES {str(vals)}")
    except Error as e:
        raise Exception(f"Error inserting data into {table}: {e}")
    conn.commit()


def select_ZTF_objects(conn, ztf_object_ids):
    """
    Select rows from database with id(s) in ztf_object_ids.

    Parameters
    ----------
    conn: sqlite3.Connection object
        The connection to the database
    ztf_object_ids: str or tuple of strs
        The ztf_object_ids to select from the database

    Returns
    df: pandas DataFrame
        Rows in the database corresponding to ztf_object_ids
    """
    cur = conn.cursor()
    if isinstance(ztf_object_ids, str):
        try:
            cur.execute("SELECT * FROM ZTF_objects WHERE ZTF_object_id=?", (ztf_object_ids,))
        except Error as e:
            raise Exception(f"Error selection objecs from ZTF_objects {e}")

    else:
        try:
            cur.execute("SELECT * FROM ZTF_objects WHERE ZTF_object_id IN {}".format(str(ztf_object_ids)))
        except Error as e:
            raise Exception(f"Error selecting all objects from ZTF_objects {e}")
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["ZTF_object_id","SIMBAD_otype","ra","dec","ROSAT_IAU_NAME", "SIMBAD_include"])
    cur.close()
    return df


def get_cached_ids(conn, condition=None):
    """Return ids of all objects previously seen
    """
    cur = conn.cursor()
    try:
        if condition is None:
            cur.execute("SELECT ZTF_object_id FROM ZTF_objects")
        else:
            cur.execute(f"SELECT ZTF_object_id FROM ZTF_objects WHERE {condition}")
    except Error as e:
        raise Exception(f"Error getting cached ids {e}")
    rows = [x[0] for x in cur.fetchall()]
    ids = pd.Series(rows)
    cur.close()
    return ids


def clear_ZTF_table(conn):
    """Delete all rows in ZTF_objects
    """
    cur = conn.cursor()
    cur.execute("DELETE FROM ZTF_objects")         
    cur.close()


def select_all_objects(conn):
    return select_ZTF_objects(conn, tuple(get_cached_ids(conn).unique()))


def update_value(conn, val_dict, condition, table='ZTF_objects'):
    """Update the value of col with val, for the given conditions
    Ex. val_dict = {'SIMBAD_otype': '\"Sy1\"'}, condition = 'ZTF_object_id = \"ZTF19abkfpqk\"' """
    cur = conn.cursor()
    try:
        cur.execute(f"UPDATE {table} SET " + ", ".join([f"{col} = {val_dict[col]}" for col in val_dict.keys()]) +
                    f" WHERE {condition}")
    except Error as e:
        raise Exception(f"Error updating values {e}")
    conn.commit()


def insert_lc_dataframe(conn, df):
    df.to_sql('lightcurves', conn, if_exists='append', index=False)


def main():
    # print("arg 1: delete from database, arg 2: suffix for database")
    database = DB_DIR + 'sqlite{}.db'.format(sys.argv[2])

    sql_create_ZTF_objects_table = """CREATE TABLE IF NOT EXISTS ZTF_objects (
                                    ZTF_object_id text,
                                    SIMBAD_otype text,
                                    ra float,
                                    dec float,
                                    ROSAT_IAU_NAME text,
                                    SIMBAD_include int,
                                    last_obs float,
                                    seen_flag int,
                                    interest_flag int,
                                    notes text
                                );"""

    sql_create_lightcurves_table = """CREATE TABLE IF NOT EXISTS lightcurves (
                                    ZTF_object_id text,
                                    jd text,
                                    fid text,
                                    magpsf float,
                                    sigmapsf float,
                                    diffmaglim float,
                                    isdiffpos text,
                                    magnr float,
                                    sigmagnr float,
                                    field int,
                                    rcid int 
                                );"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create tasks table
        create_table(conn, sql_create_ZTF_objects_table)
        create_table(conn, sql_create_lightcurves_table)
    else:
        print("Error! cannot create the database connection.")

    if (len(sys.argv) > 1) and (sys.argv[1].lower() in ['true', 'y', 'yes']):
        cur = conn.cursor()
        cur.execute("DELETE FROM ZTF_objects")
        cur.close()

    print("Connected to database")
    conn.close()

if __name__ == '__main__':
    main()
