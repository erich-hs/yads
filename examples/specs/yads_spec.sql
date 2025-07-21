CREATE TABLE warehouse.orders.customer_orders (
    order_id UUID NOT NULL,
    company_id TEXT NOT NULL,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    order_total DECIMAL(10, 2) NOT NULL DEFAULT 0.5,
    shipping_address VARCHAR(255),
    line_items ARRAY<STRUCT<product_id INTEGER NOT NULL, quantity INTEGER NOT NULL, price DECIMAL(8, 2) NOT NULL>>,
    tags ARRAY<TEXT>,
    metadata_tags MAP<VARCHAR(50), VARCHAR(255)>,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT customer_orders_pk PRIMARY KEY (order_id, company_id),
    CONSTRAINT customer_orders_customer_fk FOREIGN KEY (customer_id) REFERENCES warehouse.orders.customers (id)
)
PARTITIONED BY (order_date, MONTH(created_at))
LOCATION '/warehouse/orders/customer_orders'
TBLPROPERTIES (
    'table_type' = 'iceberg',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
