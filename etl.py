import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time

start = time.time()
print("Starting the ETL...")
print()

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        #print("Loading staging tables... ({})".format(query))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        #print("Inserting table data... ({})".format(query))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    #print()
    insert_tables(cur, conn)
    #print()

    conn.close()


if __name__ == "__main__":
    main()
    end = time.time()
    print("Execution Time: {}".format(end - start))
