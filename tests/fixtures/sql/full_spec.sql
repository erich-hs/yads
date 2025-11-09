CREATE EXTERNAL TABLE catalog.db.full_spec (
  c_string TEXT DEFAULT 'default_string',
  c_string_len TEXT(255),
  c_string_upper TEXT GENERATED ALWAYS AS (UPPER(c_string)),
  c_int8 TINYINT,
  c_int16 SMALLINT,
  c_int32_identity INT GENERATED ALWAYS AS IDENTITY(1, 1),
  c_int64 BIGINT,
  c_float32 FLOAT,
  c_float64 DOUBLE,
  c_decimal DECIMAL,
  c_decimal_ps DECIMAL(10, 2),
  c_boolean BOOLEAN,
  c_binary BINARY,
  c_binary_len BINARY(8),
  c_date DATE NOT NULL,
  c_date_month INT GENERATED ALWAYS AS (MONTH(c_date)),
  c_time TIME,
  c_timestamp TIMESTAMP,
  c_timestamp_date DATE GENERATED ALWAYS AS (CAST(c_timestamp AS DATE)),
  c_timestamp_tz TIMESTAMPTZ,
  c_timestamp_ltz TIMESTAMPLTZ,
  c_timestamp_ntz TIMESTAMPNTZ,
  c_interval_ym INTERVAL YEAR TO MONTH,
  c_interval_d INTERVAL DAY,
  c_array ARRAY<INT>,
  c_array_sized ARRAY<TEXT>,
  c_struct STRUCT<nested_int: INT, nested_string: TEXT>,
  c_map MAP<TEXT, DOUBLE>,
  c_json JSON,
  c_geometry GEOMETRY(4326),
  c_geography GEOGRAPHY(4326),
  c_uuid UUID NOT NULL,
  c_void VOID,
  c_variant VARIANT,
  CONSTRAINT pk_full_spec PRIMARY KEY (c_uuid, c_date),
  CONSTRAINT fk_other_table FOREIGN KEY (c_int64) REFERENCES other_table (id)
)
USING parquet
LOCATION '/data/full.spec'
TBLPROPERTIES (
  'write_compression' = 'snappy'
)
PARTITIONED BY (c_string_len, TRUNCATE(c_string, 10), MONTH(c_date))
