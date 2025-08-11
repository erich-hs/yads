CREATE TABLE catalog.db.nested_spec (
  items ARRAY<STRUCT<product_id: INT, name: TEXT(100)>>
); 