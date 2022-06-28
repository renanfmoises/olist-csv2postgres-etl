import sys
import datetime
import csv
from io import StringIO
import psycopg2
import pandas as pd

from db_params import param_dict


tables = [
    "customers",
    "geolocation",
    "orders",
    "products",
    "sellers",
    "order_items",
    "order_payments",
    "order_reviews",
]

file_names = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
]


def connect(params: dict):
    conn = None
    try:
        # connect to the PostgreSQL server
        print(f"{datetime.datetime.now()} - Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**param_dict)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print(f" {datetime.datetime.now()} - PostgreSQL database connection established.")


def copy_from_string(conn, df, table):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()

    try:
        cursor.copy_from(buffer, table, sep=",", null='', columns=df.columns)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 1
    print(f"{datetime.datetime.now()} - {table} table populated.")

    cursor.close()


# def copy_data_to_psql(file_name: str, cur, conn, table_name: str):
#     with open(f"raw_data/{file_name}", "r") as row:
#         next(row)
#         cur.copy_from(row, table_name, null = "", sep = ",")
#         conn.commit()
#         row.close()


def main():
    # connect to DB
    conn = connect(param_dict)

    # copy data to DB
    for file, table in zip(file_names, tables):
        copy_from_string(conn, pd.read_csv(f"raw_data/{file}"), table)

    # close connection
    conn.close()

    # conn_string = "host='localhost' dbname='olist' user='renan' password='postgres'"
    # conn = psycopg2.connect(conn_string)
    # cur = conn.cursor()

    # for table, file in zip(tables, file_names):
    #     copy_data_to_psql(file, cur, conn, table)


if __name__ == "__main__":
    main()
