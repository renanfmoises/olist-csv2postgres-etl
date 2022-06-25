import os
import psycopg2

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


def copy_data_to_psql(file_name: str, cur, conn, table_name: str):
    with open(f"raw_data/{file_name}", "r") as row:
        next(row)
        cur.copy_from(row, table_name, sep=",")
        conn.commit()


def main():
    conn_string = "host='localhost' dbname='olist' user='renan' password='postgres'"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    for table, file in zip(tables, file_names):
        copy_data_to_psql(file, cur, conn, table)


if __name__ == "__main__":
    main()
