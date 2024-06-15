from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, BooleanType
from pyspark.sql.functions import explode, udf, from_json, to_json, col
import json

# Create a Spark session
spark = SparkSession.builder \
    .appName("vaidate json") \
    .getOrCreate()

# This is an example list of JSON objects. The first two JSON objects contain all the defined fields in the custom schema and should be filtered as schema_matching_df (valid JSON). The last two JSON objects have IDs starting with 'reject_test' and do not contain all the required column names in the custom schema, so they should be filtered as schema_non_matching_df (invalid JSON).
json_data = """[{"id":1,"name":"Software_1","version":"1.0","active":true,"features":["feature-A","feature-B"],"metadata":{"developer":"Dev-1","release_date":"2024-06-15"}},{"id":2,"name":"Software_2","version":"2.1","active":false,"features":["feature-C","feature-D","feature-E"],"metadata":{"developer":"Dev-2","release_date":"2024-06-15"}},{"id":"rejet_test_1","name":"Software_3","version":"3.0","metadata":{"developer":"Dev-3","release_date":"2024-06-15"}},{"id":"rejet_test_2","name":"Software_4","version":"4.2","active":false,"features":["feature-C"]}]
"""


# Sample DataFrame with a single row which value is a list of JSON.
response_df = spark.createDataFrame([(json_data,)], ["response"])

# Custom Schema  => We can use this Spark schema when we need to work further on the DataFrame, and these columns are required to flatten the data.
custom_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("version", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("features", ArrayType(StringType()), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

# If want to use custom schema as below instead of Spark schema as above.
# Define the schema column names
custom_schema = ["id", "name", "version", "active", "features", "metadata"]

# Extract column names for json validation.
#schema_column_names = set([field.name for field in custom_schema]) # When we have custom_schema as Spark Schema. 
schema_column_names = set(custom_schema) # When we have custom_schema as list of column names instead of Spark Schema.


# Function to filter schema-matching and schema-non-matching json.
def filter_schema_matching_non_matching_json(response_df, schema_column_names):
    try:
        # Define UDF for JSON validation.
        def is_matching_schema_json(response_json):
            try:
                json_obj = json.loads(response_json)
                if not isinstance(json_obj, dict):
                    return False
                json_schema = set(json_obj.keys())
                if json_schema != schema_column_names:
                    return False
                return True
            except (ValueError, TypeError):
                return False

        # Register UDF for JSON validation.
        is_matching_schema_json_udf = udf(is_matching_schema_json, BooleanType())

        # Parse the JSON string into an array of JSON objects.
        response_array_df = response_df.withColumn("response_array", from_json(response_df["response"], ArrayType(StringType())))

        # Explode the array of JSONs into individual rows.
        exploded_df = response_array_df.select(explode(response_array_df.response_array).alias("response"))

        # Filter valid and rejected JSONs separately.
        schema_matching_df = exploded_df.filter(is_matching_schema_json_udf(exploded_df.response))
        schema_non_matching_df = exploded_df.filter(~is_matching_schema_json_udf(exploded_df.response))
        #schema_non_matching_df = exploded_df.subtract(schema_matching_df) # This line is alternate option with above line.
       
        return schema_matching_df, schema_non_matching_df
    except Exception as e:
        print("An error occurred:", e)
        return None, None


# Call filter_valid_and_rejected_json to get valid and rejected DataFrames.
schema_matching_df, schema_non_matching_df = filter_schema_matching_non_matching_json(response_df, schema_column_names)

# Show the content of schema_matching_df DataFrame.
print("Schema Matching DataFrame:")
schema_matching_df.show(truncate=False)

# Show the content of schema_non_matching_df DataFrame.
print("Schema Non Matching DataFrame:")
schema_non_matching_df.show(truncate=False)



