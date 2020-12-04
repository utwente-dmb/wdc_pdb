import sys
import json
import gzip
from configparser import ConfigParser
import psycopg2

INSERTS_PER_COMMIT = 1024
VERBOSE            = True
MAX_INSERT         = 1000000 # sys.maxsize

TOP15PROPERTIES = ['name',
                   'price',
                   'offers',
                   'priceCurrency',
                   'image',
                   'description',
                   'availability',
                   'url',
                   'ratingValue',
                   'sku',
                   'reviewCount',
                   'aggregateRating',
                   'brand',
                   'productID',
                   'manufacturer'
]

SQL_START = """
    DROP TABLE IF EXISTS TABLEBASE_key CASCADE;
    CREATE SEQUENCE TABLEBASE_key_seq;
    CREATE TABLE TABLEBASE_key (
        key     BIGINT NOT NULL DEFAULT nextval('TABLEBASE_key_seq'),
        url     TEXT,
        node_id TEXT,
        PRIMARY KEY (url, node_id)
    );
    ALTER SEQUENCE TABLEBASE_key_seq OWNED BY TABLEBASE_key.key;
    CREATE INDEX TABLEBASE_key_index on TABLEBASE_key(key);
    
    DROP TABLE IF EXISTS TABLEBASE_offer CASCADE;
    CREATE TABLE TABLEBASE_offer (
          key        BIGINT NOT NULL
        , cluster_id BIGINT NOT NULL
        , prop       TEXT   DEFAULT NULL
        TOP15PROPERTIES_ATTRIBUTES
    );

    -- fast insert with duplicate check
    DROP FUNCTION IF EXISTS insert_wdc_key;
    CREATE FUNCTION insert_wdc_key(p_url TEXT, p_node_id TEXT) 
        RETURNS BIGINT AS $$
    WITH w AS (
        INSERT INTO TABLEBASE_key(url,node_id,key)
                    VALUES(p_url,p_node_id,DEFAULT)
                    ON CONFLICT DO NOTHING
                    RETURNING key
    )
    SELECT COALESCE(
        (SELECT key FROM w),
        (SELECT key FROM TABLEBASE_key WHERE url=p_url AND node_id=p_node_id)
    )
    $$ LANGUAGE SQL;
"""

def init_db(tablebase=None):
    try:
       top15attributes = ''
       for prop in TOP15PROPERTIES:
           top15attributes += ', p_{} TEXT DEFAULT NULL\n'.format(prop)
       sql = SQL_START.replace('TABLEBASE',tablebase).replace('TOP15PROPERTIES_ATTRIBUTES',top15attributes)
       execute_pg(sql)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        exit() # pretty fatal
    
def add_offer_to_db(tablebase, cursor, offer):
    try:
        all_args = [offer['url'], offer['nodeID'], int(offer['cluster_id'])]
        p_names  = ''
        p_perc   = ''
        #
        prop = get_top_properties(offer)
        for k in prop.keys():
            p_names += ',p_'+k
            p_perc  += ',%s'
            all_args.append(prop[k])
            # print(k,prop[k])
        cursor.execute('INSERT INTO {}_offer(key,cluster_id{}) VALUES(insert_wdc_key(%s,%s),%s{});'.format(tablebase,p_names,p_perc),all_args)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        exit() # pretty fatal

def convert_json(jsonzip=None,tablebase=None):
    """ Convert the .gz JSON input to a table in de PostgreSQL database """
    init_db(tablebase)
    with gzip.open(jsonzip) as fp:
       cursor   = None
       inserted = 0
       total    = 0
       for line in fp:
           offer = json.loads(line)
           # print(json.dumps(offer, indent=4))
           if cursor is None:
               cursor = conn_pg.cursor()
           inserted += 1
           total    += 1
           if total <= MAX_INSERT:
               add_offer_to_db(tablebase,cursor,offer)
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

def add_top_property(pl,label,new_d):
    key = '/'+label
    for single_d in pl:
        if key in single_d:
            val = single_d[key]
            if val.startswith('['):
                # strip [] and leading/trailing spaces
                val = val[1:len(val)-1].strip()
            new_d[label] = val
            print(label,'=',val)
            return True
    return False

def get_top_properties(offer):
    result_d = {}
    d_ident  = offer['identifiers'];
    d_schema = offer['schema.org_properties'];
    for label in TOP15PROPERTIES:
        if not add_top_property(d_ident, label,  result_d):
            # property was not in ident dict, now look in schema
            add_top_property(d_schema, label, result_d)
    return result_d

def analyze_json(jsonzip=None):
    # framework for doing try-out on json 'offer' object
    with gzip.open(jsonzip) as fp:
        for line in fp:
            offer = json.loads(line)

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
        exit() # pretty fatal


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
    # convert_json(jsonzip='./offers_english.json.gz',tablebase='WDC_ENG')
    convert_json(jsonzip='./sample_offersenglish.json.gz',tablebase='WDC_ENG')
    close_pg()
