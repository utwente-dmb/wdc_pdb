import sys
import json
import gzip
from configparser import ConfigParser
import psycopg2

INSERTS_PER_COMMIT = 1024
VERBOSE = True
MAX_INSERT = 50000  # sys.maxsize

SQL = """
    DROP TABLE IF EXISTS pg_wdc_small CASCADE;
    CREATE TABLE pg_wdc_small (
        id              BIGINT PRIMARY KEY,
        cluster_id      BIGINT             ,
        category        TEXT   DEFAULT NULL,
        title           TEXT   DEFAULT NULL,
        description     TEXT   DEFAULT NULL,
        brand           TEXT   DEFAULT NULL,
        price           TEXT   DEFAULT NULL,
        specTableContent TEXT  DEFAULT NULL,
        keyValuePairs   TEXT   DEFAULT NULL
    );

    DROP TABLE IF EXISTS pg_wdc_key_small CASCADE;
    CREATE TABLE pg_wdc_key_small (
        prod_id         BIGINT NOT NULL,
        id_type         TEXT   NOT NULL,
        id_val          TEXT   NOT NULL,
        PRIMARY KEY (prod_id, id_type, id_val),
        FOREIGN KEY (prod_id) REFERENCES pg_wdc_small(id)
    )
"""


def init_db(tablebase=None):
    try:
        execute_pg(SQL)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        exit()  # pretty fatal


def add_offer_to_db(tablebase, tablebase_key, cursor, offer):
    try:
        keyValuePairs = str(offer['keyValuePairs']) if offer['keyValuePairs'] else None

        all_args = [int(offer['id']), int(offer['cluster_id']), offer['category'], offer['title'],
                    offer['description'], offer['brand'], offer['price'], offer['specTableContent'],
                    keyValuePairs]

        cursor.execute('INSERT INTO {} VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);'.format(tablebase),
                       all_args)

        for id in offer['identifiers']:
            for key, val in id.items():
                val = list(val[1:-1].split(", "))
                if len(val) > 1:
                    for item in val:
                        key_args = [int(offer['id']), key, item]
                        cursor.execute('INSERT INTO {} VALUES(%s, %s, %s);'.format(tablebase_key), key_args)
                else:
                    key_args = [int(offer['id']), key, val[0]]
                    cursor.execute('INSERT INTO {} VALUES(%s, %s, %s);'.format(tablebase_key), key_args)


    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: {}'.format(error))
        exit()  # pretty fatal


def convert_json(jsonzip=None, tablebase=None, tablebase_key=None):
    """ Convert the .gz JSON input to a table in de PostgreSQL database """
    init_db(tablebase)
    with gzip.open(jsonzip) as fp:
        cursor = None
        inserted = 0
        total = 0
        for line in fp:
            offer = json.loads(line)
            # print(json.dumps(offer, indent=4))
            if cursor is None:
                cursor = conn_pg.cursor()
            inserted += 1
            total += 1
            if total <= MAX_INSERT:
                add_offer_to_db(tablebase, tablebase_key, cursor, offer)
            if inserted >= INSERTS_PER_COMMIT:
                if VERBOSE:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                inserted = 0
                conn_pg.commit()
                cursor.close()
                cursor = None
        if cursor is not None:
            conn_pg.commit()
            cursor.close()
            cursor = None
        if VERBOSE:
            sys.stdout.write('\n')
    if VERBOSE:
        print("# Succesfully inserted {} tuples.".format(total))


def config(configname='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(configname)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, configname))

    return db


conn_pg = None


def connect_pg(configname='database.ini'):
    """ Connect to the PostgreSQL database server """
    try:
        # read connection parameters
        params = config(configname=configname)

        # connect to the PostgreSQL server
        if VERBOSE:
            print('Connecting to the PostgreSQL database...')
        global conn_pg
        conn_pg = psycopg2.connect(**params)

        # create a cursor
        cur = conn_pg.cursor()

        # execute a statement
        if VERBOSE:
            print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        if VERBOSE:
            print(db_version)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        exit()  # pretty fatal


def close_pg():
    """ Close connection to the PostgreSQL database server """
    try:
        if conn_pg is not None:
            conn_pg.close()
            if VERBOSE:
                print('Database connection closed.')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def execute_pg(sql_stat=None):
    """ Execute single command on the PostgreSQL database server """
    try:
        cur = conn_pg.cursor()
        cur.execute(sql_stat)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == '__main__':
    connect_pg(configname='database.ini')
    # analyze_json(jsonzip='./sample_offersenglish.json.gz')
    convert_json(jsonzip='./offers_corpus_english_v2.json.gz', tablebase='pg_wdc_small', tablebase_key='pg_wdc_key_small')
    # convert_json(jsonzip='./offers_corpus_english_v2_sample.json.gz', tablebase='pg_wdc_small', tablebase_key='pg_wdc_key_small')
    close_pg()