## drop all tables, then create the staging tables, and then the fact-dimension tables
## use the sql_queries.py to store all the specific ddl

import configparser
import psycopg2
import datetime
from sql_queries import create_table_queries, drop_table_queries
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
    print(eventname + " ended " + str(datetime.datetime.now()))

def drop_tables(cur, conn):
    """
    this function drop all the staging, fact and dimension tables
    Parameters:
        cur:cursor to the database
        conn: connection string to the database
    """
    for query in drop_table_queries:
        print("Running the drop query: ", query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    this function creates all the staging, fact and dimension tables
    Parameters:
        cur:cursor to the database
        conn: connection string to the database
    """
    for query in create_table_queries:
        print("Running the create query: ", query)
        cur.execute(query)
        conn.commit()


def main():
    """
    this main function gets the configuration from dwh.cfg and then connects to the database
    and drop all tables and create all required tables
    """
    logstart("drop and create tables")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    logend("drop and create tables")

if __name__ == "__main__":
    main()
