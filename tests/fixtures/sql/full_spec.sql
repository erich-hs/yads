CREATE EXTERNAL TABLE catalog.db.full_schema (
  user_id UUID NOT NULL,
  score DECIMAL(5, 2),
  CONSTRAINT pk_full_schema PRIMARY KEY (user_id)
)
LOCATION '/data/full.schema'
USING parquet
TBLPROPERTIES (
  'write_compression' = 'snappy'
)
PARTITIONED BY (user_id, IDENTITY(score))