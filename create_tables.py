"""This module creates the tables in the PostgreSQL"""

import datetime
import psycopg2
from sql_queries import drop_table_queries, create_table_queries
from db_params import param_dict


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
    # Get params for connecting to DB
    pdict = param_dict

    # Connect to DB
    conn = psycopg2.connect(**pdict)

    # Create cursor
    cur = conn.cursor()

    print(f">> {datetime.datetime.now()} | [ TABLE ] | DROPPPING TABLES (IF EXISTS).")
    drop_tables(cur, conn)

    print(f">> {datetime.datetime.now()} | [ TABLE ] | CREATING TABLES.")
    create_tables(cur, conn)
    print(f">> {datetime.datetime.now()} | [ TABLE ] | TABLES CREATED.")

    conn.close()


if __name__ == "__main__":
    main()
