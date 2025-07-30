CREATE TABLE catalog.db.nested_schema (
  items ARRAY<STRUCT<product_id: INT, name: TEXT(100)>>
); 