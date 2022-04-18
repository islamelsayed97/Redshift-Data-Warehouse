import sys
import configparser
import psycopg2
from sql_queries import create_table_queries, create_tables_order, drop_table_queries, drop_tables_order

def drop_tables(cur, conn):
    """
    Drop all the table in the Redshift cluster
    
    Params:
    cur -- cursor object to database connection
    conn -- connection object to database
    """
    
    
    for i, (table, query) in enumerate(zip(drop_tables_order, drop_table_queries), 1):
        try:
            print(f'{i}. Deleting {table} table...')
            cur.execute(query)
            conn.commit()
            print('Done.')
        except psycopg2.Error as e:
            print(e)
            conn.close()


def create_tables(cur, conn):
    """
    Create all the tables in the Redshift cluster
    
    Params:
    cur -- cursor object to database connection
    conn -- connection object to database
    """
    
    for i, (table, query) in enumerate(zip(create_tables_order, create_table_queries), 1):
        try:
            print(f'{i}. Creating {table} table...')
            cur.execute(query)
            conn.commit()
            print('Done.')
        except psycopg2.Error as e:
            print(e)
            conn.close()

if __name__ == "__main__":
 
    # get the credintials from dwh.cfg file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to the Redshift cluster
    try:
        print('Connecting to the Redshift cluster...')
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except Exception as e:
        print(e)

    # reset the tables
    print('\nFirst, drop all the tables if exists')
    drop_tables(cur, conn)
    print('\nThen, create the tables')
    create_tables(cur, conn)

    print('\nAll tables were created successfully.\n\nClosing the connection...')
    # close the connection
    conn.close()