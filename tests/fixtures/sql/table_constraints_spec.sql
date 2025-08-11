CREATE TABLE catalog.db.test_spec (
  id INT,
  name TEXT,
  order_id UUID,
  product_id INT,
  CONSTRAINT test_pk PRIMARY KEY (id, name),
  CONSTRAINT fk_order_items FOREIGN KEY (order_id, product_id) REFERENCES order_items (order_id, product_id)
); 