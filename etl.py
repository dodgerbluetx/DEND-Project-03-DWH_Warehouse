import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time
import datetime


def load_staging_tables(cur, conn):
    """
    Description:
      Loads data from defined S3 bucket into staging tables. Use the
      sql_queries script to loop through each query defined in the
      copy_table_queries list.

    Parameters:
      cur - the db connection cursor
      conn - the db connection object

    Returns:
      none
    """
    for query in copy_table_queries:
        print("Copying data to staging tables...")
        print()
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description:
      Inserts transformed data from staging tables into the fact and dimension
      tables. Use the sql_queries script to loop through each query defined
      in the insert_table_queries list.

    Parameters:
      cur - the db connection cursor
      conn - the db connection object

    Returns:
      none
    """
    for query in insert_table_queries:
        print("Inserting data into fact/dim tables...")
        print()
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
    start = datetime.datetime.now()
    start_dt = start.strftime("%Y-%m-%d %H:%M:%S")

    print()
    print('Starting the ETL script at {}'.format(start_dt))
    print()

    main()

    end = datetime.datetime.now()
    end_dt = end.strftime("%Y-%m-%d %H:%M:%S")

    print()
    print('ETL script complete at {}'.format(end_dt))
    print()
    print("Total execution time: {}".format(end - start))
    print()
