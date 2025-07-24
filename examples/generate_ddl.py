"""
Example script to generate DDL from a yads table specification.
"""

from sqlglot import parse_one


def main():
    """
    Reads a `yads` YAML specification and prints the generated DDL.
    """
    sql_str = """
CREATE TABLE default.people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  dateOfBirth DATE GENERATED ALWAYS AS (CAST(birthDate AS DATE)),
  ssn STRING,
  salary INT
)
"""
    print(sql_str)
    ast = parse_one(sql_str)
    print("Generated AST:")
    print(repr(ast))

    print("\n" + "=" * 80 + "\n")

    sql_str = """
CREATE TABLE events(
eventId BIGINT,
data STRING,
eventType STRING,
eventTime TIMESTAMP,
eventDate DATE GENERATED ALWAYS AS (CAST(eventTime AS DATE))
)
PARTITIONED BY (eventType, eventDate)
"""
    print(sql_str)
    ast = parse_one(sql_str)
    print("Generated AST:")
    print(repr(ast))

    print("\n" + "=" * 80 + "\n")

    sql_str = """
CREATE TABLE events(
eventId BIGINT,
data STRING,
eventType STRING,
eventTime TIMESTAMP,
year INT GENERATED ALWAYS AS (YEAR(eventTime)),
month INT GENERATED ALWAYS AS (MONTH(eventTime)),
day INT GENERATED ALWAYS AS (DAY(eventTime))
)
PARTITIONED BY (eventType, year, month, day)
"""
    print(sql_str)
    ast = parse_one(sql_str)
    print("Generated AST:")
    print(repr(ast))

    print("\n" + "=" * 80 + "\n")

    sql_str = """
CREATE TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  order_date DATE,
  total_amount DOUBLE
)
USING iceberg
PARTITIONED BY (bucket(customer_id, 10), bucket(order_date, 5))
"""
    print(sql_str)
    ast = parse_one(sql_str)
    print("Generated AST:")
    print(repr(ast))

    print("\n" + "=" * 80 + "\n")

    sql_str = """
CREATE TABLE user_data (
  user_id BIGINT,
  email STRING,
  name STRING,
  created_at TIMESTAMP
) 
USING iceberg
PARTITIONED BY (TRUNCATE(3, email));
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
