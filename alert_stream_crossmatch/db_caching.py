#!/usr/bin/env python
# By David Wang
# Sept 10, 2020
# Store queried ZTF objects in database

import sqlite3
import pandas as pd
from sqlite3 import Error
from constants import DB_DIR

def create_connection(db_file):
    """ Create a database connection to the SQLite database
        specified by db_file
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    """ Create a table from the create_table_sql statement
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def cache_ZTF_object(conn, ztf_object):
    """
    Add ZTF object to database.
    """

    sql = ''' INSERT INTO ZTF_objects(ZTF_object_id,SIMBAD_otype,ra,dec,ROSAT_IAU_NAME)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, ztf_object)
    conn.commit()
    return cur.lastrowid

def select_ZTF_objects(conn, ztf_object_ids):
    cur = conn.cursor()
    if isinstance(ztf_object_ids, str):
        try:
            cur.execute("SELECT * FROM ZTF_objects WHERE ZTF_object_id=?", (ztf_object_ids,))
        except Error as e:
            print(e)

    else:
        try:
            cur.execute("SELECT * FROM ZTF_objects WHERE ZTF_object_id IN {}".format(str(ztf_object_ids)))
        except Error as e:
            print(e)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["ZTF_object_id","SIMBAD_otype","ra","dec","ROSAT_IAU_NAME"])
    return df

def main():
    database = DB_DIR + 'test_sqlite.db'

    sql_create_ZTF_objects_table = """CREATE TABLE IF NOT EXISTS ZTF_objects (
                                    ZTF_object_id text PRIMARY KEY,
                                    SIMBAD_otype text,
                                    ra float NOT NULL,
                                    dec float NOT NULL,
                                    ROSAT_IAU_NAME text
                                );"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create tasks table
        create_table(conn, sql_create_ZTF_objects_table)
    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    main()
