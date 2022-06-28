"""This module keeps the SQL queries for the ETL pipeline."""

# DROP TABLES

drop_if_exists = "DROP TABLE IF EXISTS {} CASCADE;"

customers_table_drop = drop_if_exists.format("customers")
geolocation_table_drop = drop_if_exists.format("geolocation")
orders_table_drop = drop_if_exists.format("orders")
products_table_drop = drop_if_exists.format("products")
sellers_table_drop = drop_if_exists.format("sellers")
order_items_table_drop = drop_if_exists.format("order_items")
order_payments_table_drop = drop_if_exists.format("order_payments")
order_reviews_table_drop = drop_if_exists.format("order_reviews")


# CREATE_TABLES

customers_table_create = """
    CREATE TABLE customers (
        customer_id               VARCHAR NOT NULL PRIMARY KEY,
        customer_unique_id        VARCHAR NOT NULL,
        customer_zip_code_prefix  VARCHAR,
        customer_city             VARCHAR,
        customer_state            VARCHAR
    );
"""

geolocation_table_create = """
    CREATE TABLE geolocation (
        geolocation_zip_code_prefix  VARCHAR,
        geolocation_lat              FLOAT,
        geolocation_lng              FLOAT,
        geolocation_city             VARCHAR,
        geolocation_state            VARCHAR
    );
"""

orders_table_create = """
    CREATE TABLE orders (
        order_id                       VARCHAR NOT NULL PRIMARY KEY,
        customer_id                    VARCHAR REFERENCES customers (customer_id),
        order_status                   VARCHAR,
        order_purchase_timestamp       TIMESTAMP,
        order_approved_at              TIMESTAMP,
        order_delivered_carrier_date   TIMESTAMP,
        order_delivered_customer_date  TIMESTAMP,
        order_estimated_delivery_date  TIMESTAMP
    );
"""

products_table_create = """
    CREATE TABLE products (
        product_id                  VARCHAR NOT NULL PRIMARY KEY,
        product_category_name       VARCHAR,
        product_name_lenght         FLOAT,
        product_description_lenght  FLOAT,
        product_photos_qty          FLOAT,
        product_weight_g            FLOAT,
        product_length_cm           FLOAT,
        product_height_cm           FLOAT,
        product_width_cm            FLOAT
    );
"""

sellers_table_create = """
    CREATE TABLE sellers (
        seller_id               VARCHAR NOT NULL PRIMARY KEY,
        seller_zip_code_prefix  VARCHAR,
        seller_city             VARCHAR,
        seller_state            VARCHAR
    );
"""

order_items_table_create = """
    CREATE TABLE order_items (
        order_id             VARCHAR REFERENCES orders (order_id),
        order_item_id        INT NOT NULL,
        product_id           VARCHAR REFERENCES products (product_id),
        seller_id            VARCHAR REFERENCES sellers (seller_id),
        shipping_limit_date  TIMESTAMP,
        price                FLOAT,
        freight_value        FLOAT

    );
"""

order_payments_table_create = """
    CREATE TABLE order_payments (
        order_id              VARCHAR REFERENCES orders (order_id),
        payment_sequential    INT,
        payment_type          VARCHAR,
        payment_installments  INT,
        payment_value         FLOAT
    );
"""

# order_reviews_table_create = """
#     CREATE TABLE order_reviews (
#         review_id                VARCHAR NOT NULL PRIMARY KEY,
#         order_id                 VARCHAR REFERENCES orders (order_id),
#         review_score             INT,
#         review_comment_title     VARCHAR,
#         review_comment_message   VARCHAR,
#         review_creation_date     TIMESTAMP,
#         review_answer_timestamp  TIMESTAMP

#     );
# """

order_reviews_table_create = """
    CREATE TABLE order_reviews (
        review_id                VARCHAR NOT NULL PRIMARY KEY,
        order_id                 VARCHAR,
        review_score             INT,
        review_creation_date     TIMESTAMP,
        review_answer_timestamp  TIMESTAMP
    );
"""


# STAGING TABLES
# TODO


# QUERIES LISTS

drop_table_queries = [
    customers_table_drop,
    geolocation_table_drop,
    orders_table_drop,
    products_table_drop,
    sellers_table_drop,
    order_items_table_drop,
    order_payments_table_drop,
    order_reviews_table_drop,
]

create_table_queries = [
    customers_table_create,
    geolocation_table_create,
    orders_table_create,
    products_table_create,
    sellers_table_create,
    order_items_table_create,
    order_payments_table_create,
    order_reviews_table_create,
]
