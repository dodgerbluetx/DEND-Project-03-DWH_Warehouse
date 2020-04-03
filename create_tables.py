import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import time

start = time.time()
print("Setting up the DB...")
print()

def drop_tables(cur, conn):
    for query in drop_table_queries:
        #print("Dropping tables... ({})".format(query))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        #print("Creating tables... ({})".format(query))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    #print()
    create_tables(cur, conn)
    #print()

    conn.close()


if __name__ == "__main__":
    main()
    end = time.time()
    print("Execution Time: {}".format(end - start))
