import configparser
import psycopg2
from sql_queries import select_table_queries
from sql_queries import count_table_queries
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
    print(eventname + " ended " + str(datetime.datetime.now()))


def run_select_queries(cur, conn):
    """
    this function run the select queries
    Parameters:
       cur: cursor to the database
       conn: connection string to the database
    """
    logstart("run select queries")
    for query in select_table_queries:
        print(query)
        cur.execute(query)
        results = cur.fetchall()
        print(results)
        conn.commit()
    logend("run select queries")

def run_count_queries(cur, conn):
    """
    this function run the select count queries
    Parameters:
       cur: cursor to the database
       conn: connection string to the database
    """
    logstart("run count queries")
    for query in count_table_queries:
        print(query)
        cur.execute(query)
        results=cur.fetchone()
        print(results)
        conn.commit()
    logend("run count queries")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    run_select_queries(cur, conn)
    run_count_queries(cur, conn)


    conn.close()


if __name__ == "__main__":
    main()