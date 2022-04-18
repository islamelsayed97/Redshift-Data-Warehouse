import configparser
import psycopg2
from sql_queries import copy_table_queries, staging_tables_order, insert_table_queries, dwh_tables_order


def load_staging_tables(cur, conn):
    """
    Load data from files stored in S3 to the staging tables.
    """
    
    print("Loading data from JSON files stored in S3 buckets into staging tables...")
    
    for i, (table, query) in enumerate(zip(staging_tables_order, copy_table_queries), 1):
        print(f'{i}. Loading data into {table} table...')
        cur.execute(query)
        conn.commit()
        print('Done.')
    
    print("\nAll loaded into staging tables successfully.\n")


def insert_tables(cur, conn):
    """
    Insert data from staging tables into the tables.
    """
    
    print("Inserting data from staging tables into our data warehouse...")
    
    for i, (table, query) in enumerate(zip(dwh_tables_order, insert_table_queries), 1):
        print(f'{i}. Inserting data into {table} table...')
        cur.execute(query)
        conn.commit()
        print('Done.')

    print("\nAll data inserted into data warehouse successfully.\n")
    

if __name__ == "__main__":
    """
    Extract song metadata and user activity data from S3, transform it using a staging table, 
    and load it into fact and dimensional tables for analysis
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()