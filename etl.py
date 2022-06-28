import sys
import datetime
from io import StringIO
import psycopg2
import pandas as pd

from db_params import param_dict

# Table names
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

# File names
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
    """This function connects to the PostgreSQL database.
    Args:
        params (dict): dictionary with the parameters to connect to the DB.

    Returns:
        conn (object): connection to the database.
    """

    conn = None

    try:
        # connect to the PostgreSQL server
        print(f">> {datetime.datetime.now()} - Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**param_dict)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print(f">> {datetime.datetime.now()} - PostgreSQL database connection established.")


def copy_from_string(conn, df, table):
    """This function copies data from a pandas dataframe to a PostgreSQL table.
    Args:
        conn (object): connection to the database.
        df (pandas.DataFrame): dataframe with the data to be copied.
        table (str): name of the table to be populated.

    Returns:
        None
    """
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()

    try:
        cursor.copy_from(buffer, table, sep=",", null="", columns=df.columns)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 1
    print(f">> {datetime.datetime.now()} - {table} table populated.")

    cursor.close()


def main():
    """This function connects to the PostgreSQL database and populates the tables."""
    # Connect to DB
    conn = connect(param_dict)

    # Copy data to DB
    for file, table in zip(file_names, tables):
        copy_from_string(conn, pd.read_csv(f"raw_data/{file}"), table)

    # Close connection
    conn.close()


# Run the main function
if __name__ == "__main__":
    main()
