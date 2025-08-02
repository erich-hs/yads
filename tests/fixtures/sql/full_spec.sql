CREATE EXTERNAL TABLE catalog.db.full_schema (
  c_uuid UUID NOT NULL,
  c_string TEXT DEFAULT 'default_string',
  c_string_len TEXT(255),
  c_string_generated TEXT GENERATED ALWAYS AS (UPPER(c_string)),
  c_int8 TINYINT,
  c_int16 SMALLINT,
  c_int32_identity INT GENERATED ALWAYS AS IDENTITY(1, 1),
  c_int64 BIGINT,
  c_float32 FLOAT,
  c_float64 DOUBLE,
  c_boolean BOOLEAN,
  c_decimal DECIMAL,
  c_decimal_ps DECIMAL(10, 2),
  c_date DATE NOT NULL,
  c_date_generated DATE GENERATED ALWAYS AS (ADD_MONTHS(c_date, 1)),
  c_timestamp TIMESTAMP,
  c_binary BINARY,
  c_interval_ym INTERVAL YEAR TO MONTH,
  c_interval_d INTERVAL DAY,
  c_array ARRAY<INT>,
  c_map MAP<TEXT, DOUBLE>,
  c_struct STRUCT<nested_int: INT, nested_string: TEXT>,
  CONSTRAINT pk_full_schema PRIMARY KEY (c_uuid, c_date),
  CONSTRAINT fk_other_table FOREIGN KEY (c_int64) REFERENCES other_table (id)
)
LOCATION '/data/full.schema'
USING parquet
TBLPROPERTIES (
  'write_compression' = 'snappy'
)
PARTITIONED BY (c_string_len, month(c_date))