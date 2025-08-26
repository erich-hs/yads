import yads

# from sqlglot import parse_one, transpile
from yads.converters import SQLGlotConverter

simple_spec = yads.from_yaml("examples/specs/simple_spec.yaml")

with open("examples/specs/simple_spec.sql", "r") as f:
    simple_spec_sql = f.read()

sqlglot_converter = SQLGlotConverter()

ast_from_spec = sqlglot_converter.convert(simple_spec)
print("AST from spec:")
print(repr(ast_from_spec))

# ast_from_sql = parse_one(simple_spec_sql)
# print("AST from sql:")
# print(repr(ast_from_sql))

# transpiled_sql = transpile(simple_spec_sql, read="spark", write="duckdb", pretty=True)
# print(transpiled_sql[0])

### DuckDB
# import duckdb
# from yads.converters import SQLConverter
# duckdb_converter = SQLConverter(dialect="duckdb", pretty=True)

# duckdb_sql_ddl = duckdb_converter.convert(simple_spec)

# print(duckdb_sql_ddl)

# print("\n--- Testing DDL with DuckDB ---")
# con = duckdb.connect()
# con.sql("INSTALL spatial; LOAD spatial;")
# print("Creating table")
# con.sql(duckdb_sql_ddl)
# print("Table created")

# con.sql("SHOW ALL TABLES")
# con.close()

# print("-" * 100)


### Spark
# from pyspark.sql import SparkSession
# from yads.converters import SparkSQLConverter

# spark_converter = SparkSQLConverter(pretty=True)
# spark_sql_ddl = spark_converter.convert(simple_spec, mode="warn")

# print(spark_sql_ddl)

# print("\n--- Testing DDL with Spark ---")
# spark = (
#     SparkSession.builder.appName("YADS DDL Test")
#     .config("spark.sql.warehouse.dir", "/Users/erich/projects/yads/examples/tmp/spark-warehouse")
#     .getOrCreate()
# )
# print("Spark session created.")
# catalog_impl = spark.conf.get("spark.sql.catalogImplementation")
# print(f"Catalog implementation: {catalog_impl}")

# print("Creating table")
# df = spark.sql(spark_sql_ddl)
# print("Table created")

# print("Verifying table")
# spark.sql(f"SHOW TABLES IN default LIKE '{simple_spec.name}'").show()
# spark.sql(f"DESCRIBE TABLE {simple_spec.name}").show(truncate=False)

# spark.stop()
# print("Spark session stopped.")
