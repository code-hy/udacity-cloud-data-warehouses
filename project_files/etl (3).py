import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import datetime
now = datetime.datetime.now()

## define logstart and logend
def logstart(eventname):
    """
    this function takes one character string and prints out 
    the character string , start of process and the start time
    Parameters:
       eventname: specific event that is running
    """
    print(eventname + " started " + str(datetime.datetime.now()))
def logend(eventname):
    """
    this function takes one character string and prints out 
    the character string , end of process and the end time
    Parameters:
       eventname: specific event that is running
    """
    print(eventname + " ended "   + str(datetime.datetime.now()))


def load_staging_tables(cur, conn):
    """
    this function loads the staging tables with data from s3 buckets
    Parameters:
       cur: cursor to the database
       conn: connection string to the database
    """
    logstart("load staging tables")
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    logend("load staging tables")


def insert_tables(cur, conn):
    """
    this function inserts data into the facts and dimension tables
    Parameters:
       cur: cursor to the database
       conn: connection string to the database
    """
    logstart("insert to facts and dimensions")
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    logend("insert to facts and dimensions")


def main():
    """
    main function that is run when etl.py is executed
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()