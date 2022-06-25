"""This module creates the tables in the PostgreSQL"""

import psycopg2
from sql_queries import drop_table_queries, create_table_queries


def drop_tables(cur, conn):
    """This function drops the tables (if they exist) in PostgresSQL DB.

    Args:
        cur  (object): cursor to the database
        conn (object): connection to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This function creates the tables in the in PostgresSQL DB.

    Args:
        cur  (object): cursor to the database
        conn (object): connection to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    conn_string = "host='localhost' dbname='olist' user='renan' password='postgres'"
    print(conn_string)

    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    print(">> DROPPPING TABLES (IF EXISTS).")
    drop_tables(cur, conn)

    print(">> CREATING TABLES.")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
