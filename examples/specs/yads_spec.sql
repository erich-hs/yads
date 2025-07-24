CREATE TABLE IF NOT EXISTS prod_db.fact_sales.customer_orders_pro (
  order_id UUID NOT NULL,
  customer_id INT NOT NULL,
  order_ts TIMESTAMPTZ NOT NULL,
  total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0.0,
  order_status TEXT DEFAULT 'pending',
  product_category TEXT,
  shipping_details STRUCT<address TEXT NOT NULL, city TEXT, postal_code TEXT>,
  tags MAP<TEXT, TEXT>,
  CONSTRAINT pk_customer_orders_pro PRIMARY KEY (order_id)
)
USING iceberg
LOCATION '/warehouse/sales/customer_orders_pro'
TBLPROPERTIES (
  'write.target-file-size-bytes' = '536870912',
  'read.split.target-size' = '268435456'
)
PARTITIONED BY (MONTH(order_ts), order_status)