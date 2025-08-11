"""
Example script to generate DDL from a yads table specification.
"""

import yads
from yads.converters.sql import SQLGlotConverter
from sqlglot import parse_one


def main():
    spec = yads.from_yaml("examples/specs/simple_spec.yaml")
    converter = SQLGlotConverter()

    ast_from_spec = converter.convert(spec)

    print("--- AST ---")
    print(repr(ast_from_spec))

    print("\n--- Spark SQL ---")
    print(ast_from_spec.sql(dialect="spark", pretty=True))

    print("\n--- AST from SQL ---")
    with open("examples/specs/simple_spec.sql", "r") as f:
        sql_str = f.read()
    ast_from_sql = parse_one(sql_str)
    print(repr(ast_from_sql))

    print("\n--- AST are equal? ---")
    print(ast_from_spec == ast_from_sql)


### TODO:
# Cluster by examples to be implemented
#     sql_str = """
# -- cluster by expressions
# CREATE TABLE t2 (c1 timestamp, c2 STRING, c3 NUMBER) CLUSTER BY (TO_DATE(C1), substring(c2, 0, 10));
# """
#     print(sql_str)
#     ast = parse_one(sql_str)
#     print("Generated AST:")
#     print(repr(ast))

#     print("\n" + "=" * 80 + "\n")

#     sql_str = """
# --Use `CLUSTERED BY` clause to create bucket table with `SORTED BY`
# CREATE TABLE clustered_by_test2 (ID INT, NAME STRING)
#     PARTITIONED BY (YEAR STRING)
#     CLUSTERED BY (ID, NAME)
#     SORTED BY (ID ASC)
#     INTO 3 BUCKETS
#     STORED AS PARQUET;
# """
#     print(sql_str)
#     ast = parse_one(sql_str)
#     print("Generated AST:")
#     print(repr(ast))

if __name__ == "__main__":
    main()
