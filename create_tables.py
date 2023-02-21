import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops all tables in the Redshift cluster.

    Args:
        cur (psycopg2.extensions.cursor): Cursor object for the Redshift cluster.
        conn (psycopg2.extensions.connection): Connection object for the Redshift cluster.

    Returns:
        None
    """
    print('[INFO] DROP TABLES')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates all tables in the Redshift cluster.

    Args:
        cur (psycopg2.extensions.cursor): Cursor object for the Redshift cluster.
        conn (psycopg2.extensions.connection): Connection object for the Redshift cluster.

    Returns:
        None
    """
    print('[INFO] CREATE TABLES')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Main function for creating and dropping tables in the Redshift cluster.

    Args:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    HOST = config.get("CLUSTER", "HOST")
    DB_NAME = config.get("CLUSTER", "DB_NAME")
    DB_USER = config.get("CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DB_PORT = config.get("CLUSTER", "DB_PORT")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
