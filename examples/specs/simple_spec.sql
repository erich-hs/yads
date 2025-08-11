CREATE TABLE customer_orders (
    original_col TEXT,
    generated_col TEXT GENERATED ALWAYS AS (truncate(50, original_col))
)
PARTITIONED BY (truncate(50, original_col))