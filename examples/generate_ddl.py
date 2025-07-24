"""
Example script to generate DDL from a yads table specification.
"""

from sqlglot import parse_one


def main():
    """
    Reads a `yads` YAML specification and prints the generated DDL.
    """
    sql_str = """
-- DDL with file format defined with STORED AS
CREATE TABLE student (id INT, name STRING)
    STORED AS ORC
"""
    print(sql_str)
    ast = parse_one(sql_str)
    print("Generated AST:")
    print(repr(ast))

    print("\n" + "=" * 80 + "\n")

    sql_str = """
-- DDL with file format defined with TBLPROPERTIES
CREATE TABLE student (id INT, name STRING)
    TBLPROPERTIES (
        'format'='parquet',
        'compression'='snappy'
    )
"""
    print(sql_str)
    ast = parse_one(sql_str)
    print("Generated AST:")
    print(repr(ast))

    print("\n" + "=" * 80 + "\n")

    sql_str = """
-- DDL with open table format defined with USING
CREATE TABLE student (id INT, name STRING)
    USING iceberg
    LOCATION 's3://my-bucket/student'
"""
    print(sql_str)
    ast = parse_one(sql_str)
    print("Generated AST:")
    print(repr(ast))

    print("\n" + "=" * 80 + "\n")

    sql_str = """
-- DDL with open table format defined with USING and TBLPROPERTIES
CREATE TABLE student (id INT, name STRING)
    USING iceberg
    LOCATION 's3://my-bucket/student'
    TBLPROPERTIES (
        'write.target-file-size-bytes'='536870912',
        'read.split.target-size'='268435456'
    )
"""
    print(sql_str)
    ast = parse_one(sql_str)
    print("Generated AST:")
    print(repr(ast))

    print("\n" + "=" * 80 + "\n")

    sql_str = """
-- DDL with open table format defined via TBLPROPERTIES
CREATE TABLE student (id INT, name STRING)
    TBLPROPERTIES (
        'format'='iceberg',
        'location'='s3://my-bucket/student'
    )
"""
    print(sql_str)
    ast = parse_one(sql_str)
    print("Generated AST:")
    print(repr(ast))

    print("\n" + "=" * 80 + "\n")


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
