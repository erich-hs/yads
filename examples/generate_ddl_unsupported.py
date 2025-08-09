import yads
from yads.converters import SQLGlotConverter

simple_spec = yads.from_yaml("examples/specs/simple_spec.yaml")
# full_spec = yads.from_yaml("tests/fixtures/spec/valid/full_spec.yaml")

### DuckDB
# import duckdb
# duckdb_converter = SQLConverter(dialect="duckdb", pretty=True)
sqlglot_converter = SQLGlotConverter()

# duckdb_sql_ddl = duckdb_converter.convert(full_spec)

ast = sqlglot_converter.convert(simple_spec)

print(repr(ast))

# with open("tests/fixtures/sql/full_spec.sql", "r") as f:
#     sql_from_fixture = f.read()

# ast_from_fixture = parse_one(sql_from_fixture)
# print(repr(ast_from_fixture))

# print(duckdb_sql_ddl)

# print("\n--- Testing DDL with DuckDB ---")
# con = duckdb.connect()
# print("Creating table")
# con.sql(duckdb_sql_ddl)
# print("Table created")

# con.sql("SHOW ALL TABLES")
# con.close()

# print("-" * 100)

### Spark

# spark_converter = SQLConverter(dialect="spark", pretty=True)
# spark_converter = SparkSQLConverter(pretty=True)
# spark_sql_ddl = spark_converter.convert(full_spec, mode="warn")

# print(spark_sql_ddl)

# print("\n--- Testing DDL with Spark ---")
# spark = (
#     SparkSession.builder.appName("YADS DDL Test")
#     .config("spark.sql.warehouse.dir", "/Users/erich/projects/yads/examples/tmp/spark-warehouse")
#     .getOrCreate()
# )
# print("Spark session created.")

# print("Creating table")
# df = spark.sql(spark_sql_ddl)
# print("Table created")

# print("Verifying table")
# spark.sql(f"SHOW TABLES IN default LIKE '{full_spec.name}'").show()
# spark.sql(f"DESCRIBE TABLE {full_spec.name}").show(truncate=False)

# spark.stop()
# print("Spark session stopped.")
