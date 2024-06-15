# validate-json-with-schema
This repository contains PySpark scripts designed to validate a list of JSON objects against a predefined schema. The scripts use PySpark's DataFrame API to filter JSON data into two categories: valid JSON objects that match the schema and invalid JSON objects that do not.

# Key Components:
# 1. Spark Session Initialization: 
     Initializes a Spark session with the application name "validate json".
# 2. Sample JSON Data: 
     Defines a sample list of JSON objects representing software details. Some objects are valid (matching the schema), while others are invalid (missing required fields).
# 3. DataFrame Creation: 
     Converts the JSON data into a PySpark DataFrame with a single row using spark.createDataFrame.
# 4. Schema Definition: 
     Defines a custom schema using StructType and StructField from PySpark's SQL types. Alternatively, allows using a list of column names for schema validation.
# 5. Schema Validation Function:
     a). Implements a function filter_schema_matching_non_matching_json to filter valid and invalid JSON objects based on schema validation.
     b). Uses a user-defined function (UDF) to check if each JSON object matches the specified schema.
# 6. DataFrame Operations:
     a). Parses the JSON string into an array of JSON objects, then explodes them into individual rows.
     b). Filters JSON objects into two DataFrames:
              i). schema_matching_df: Contains JSON objects that match the defined schema.
              ii). schema_non_matching_df: Contains JSON objects that do not match the defined schema.
# 7. Usage: 
     a). This script is useful for validating JSON data against a schema when working with semi-structured data in PySpark.
     b). Modify the custom_schema variable to adjust the schema requirements or validation rules as needed for different datasets.
# 8. Dependencies:
     PySpark (pyspark): Ensure PySpark is installed and configured correctly to run the script.
# 9. Running the Script:
     a). Clone the repository and navigate to the project directory.
     b). Ensure PySpark environment is set up.
     c). Execute the script using spark-submit or interactively in a PySpark environment.

# 10. Example:
  See the provided script (filter_json_with_custom_schema.py or filter_json_with_custom_column_names.py) for an example implementation and usage.
  
