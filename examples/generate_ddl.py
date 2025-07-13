"""
Example script to generate DDL from a yads table specification.
"""

import os
from yads import TableSpecification


def main():
    """
    Reads a YAML specification and prints the generated DDL.
    """
    # Construct the path to the YAML file relative to this script
    current_dir = os.path.dirname(__file__)
    yaml_path = os.path.join(current_dir, "specs", "dim_user.yaml")

    # Load the table specification from the YAML file
    table_spec = TableSpecification(yaml_path)

    # Generate the DDL statement for the Spark dialect
    ddl = table_spec.to_ddl(dialect="spark", with_database=False)

    # Print the generated DDL
    print("--- Generated DDL ---")
    print(ddl)
    print("---------------------")

    # Demonstrate attribute access
    print("\n--- Accessing spec attributes ---")
    print(f"Table Name: {table_spec.table_name}")
    print(f"Database: {table_spec.database}")
    print(f"Database Schema: {table_spec.database_schema}")
    print(f"Owner: {table_spec.owner}")
    print(f"First column: {table_spec.schema[0].name}")
    print("---------------------------------")


if __name__ == "__main__":
    main()
