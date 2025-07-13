"""
Example script to generate a PySpark DataFrame schema from a yads table
specification.
"""

import os
from yads import TableSpecification


def main():
    """
    Reads a YAML specification and prints the generated PySpark DataFrame schema.
    """
    # Construct the path to the YAML file relative to this script
    current_dir = os.path.dirname(__file__)
    yaml_path = os.path.join(current_dir, "specs", "fact_purchase_order.yaml")

    # Load the table specification from the YAML file
    table_spec = TableSpecification(yaml_path)

    # Generate the PySpark schema
    try:
        spark_schema = table_spec.to_spark_schema()
        # Print the generated schema
        print("--- Generated PySpark Schema ---")
        print(spark_schema)
        print("------------------------------")

    except ImportError as e:
        print(f"Error: {e}")
        print(
            "Please install the required dependencies with `pip install --upgrade 'yads[spark]'` to run this example."
        )


if __name__ == "__main__":
    main()
