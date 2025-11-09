CREATE EXTERNAL TABLE catalog.db.full_spec (
  c_string STRING DEFAULT 'default_string',
  c_string_len VARCHAR(255),
  c_string_upper STRING GENERATED ALWAYS AS (UPPER(c_string)),
  c_int8 TINYINT,
  c_int16 SMALLINT,
  c_int32_identity INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  c_int64 BIGINT,
  c_float32 FLOAT,
  c_float64 DOUBLE,
  c_decimal DECIMAL,
  c_decimal_ps DECIMAL(10, 2),
  c_boolean BOOLEAN,
  c_binary BINARY,
  c_binary_len BINARY,
  c_date DATE NOT NULL,
  c_date_month INT GENERATED ALWAYS AS (MONTH(c_date)),
  c_time TIMESTAMP,
  c_timestamp TIMESTAMP,
  c_timestamp_date DATE GENERATED ALWAYS AS (CAST(c_timestamp AS DATE)),
  c_timestamp_tz TIMESTAMP,
  c_timestamp_ltz TIMESTAMP_LTZ,
  c_timestamp_ntz TIMESTAMP_NTZ,
  c_interval_ym INTERVAL YEAR TO MONTH,
  c_interval_d INTERVAL DAY,
  c_array ARRAY<INT>,
  c_array_sized ARRAY<STRING>,
  c_struct STRUCT<nested_int: INT, nested_string: STRING>,
  c_map MAP<STRING, DOUBLE>,
  c_json STRING,
  c_geometry STRING,
  c_geography STRING,
  c_uuid STRING NOT NULL,
  c_void VOID,
  c_variant VARIANT,
  CONSTRAINT pk_full_spec PRIMARY KEY (c_uuid, c_date),
  CONSTRAINT fk_other_table FOREIGN KEY (c_int64) REFERENCES other_table (
    id
  )
)
USING PARQUET
LOCATION '/data/full.spec'
PARTITIONED BY (
  c_string_len,
    TRUNCATE(10, c_string),
    MONTH(c_date)
)
TBLPROPERTIES (
  'write_compression'='snappy'
)