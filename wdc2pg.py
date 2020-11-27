import sys
import json
import gzip
from configparser import ConfigParser
import psycopg2

INSERTS_PER_COMMIT = 99
MAX_INSERT         = 1000

def read_json(jsonzip=None,tablename=None):
    try:
       execute_pg('DROP TABLE IF EXISTS {};'.format(tablename))
       execute_pg('CREATE TABLE {} (URL TEXT,NODE_ID TEXT,CLUSTER_ID BIGINT);'.format(tablename))
       with gzip.open(jsonzip) as fp:
          cursor = None
          inserted = 0
          total    = 0
          for line in fp:
              d = json.loads(line)
              # print(json.dumps(d, indent=4))
              if cursor is None:
                  cursor = conn_pg.cursor()
              inserted += 1
              total    += 1
              if total <= MAX_INSERT:
                  cursor.execute('INSERT INTO {} (URL, NODE_ID, CLUSTER_ID) VALUES(%s, %s, %s);'.format(tablename), (d['url'], d['nodeID'], int(d['cluster_id'])))
              if inserted >= INSERTS_PER_COMMIT:
                  inserted = 0
                  conn_pg.commit()
                  cursor.close()
                  cursor = None
          if cursor is not None:
              conn_pg.commit() 
              cursor.close()
              cursor = None
       print("# Succesfully inserted {} tuples in {}\n".format(total,tablename))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

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
        print('Connecting to the PostgreSQL database...')
        global conn_pg
        conn_pg = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn_pg.cursor()
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def close_pg():
    """ Close connection to the PostgreSQL database server """
    try:
        if conn_pg is not None:
            conn_pg.close()
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
    # read_json(jsonzip='./sample_offersenglish.json.gz',tablename='WDC_OFF_ENG')
    read_json(jsonzip='./offers_english.json.gz',tablename='WDC_OFF_ENG')
    close_pg()
