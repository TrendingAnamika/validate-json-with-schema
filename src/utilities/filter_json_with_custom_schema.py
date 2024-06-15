from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, BooleanType
from pyspark.sql.functions import explode, udf, from_json, to_json, col
import json

# Create a Spark session
spark = SparkSession.builder \
    .appName("vaidate json") \
    .getOrCreate()

# This is an example list of JSON objects. The first four JSON objects contain all the defined fields in the custom schema and should be filtered as schema_matching_df (valid JSON). The last four JSON objects have IDs starting with 'reject_test' and do not contain all the required column names in the custom schema, so they should be filtered as schema_non_matching_df (invalid JSON).
json_data = """[{"id": 1, "name": "stub", "status": "ACTIVE", "isPci": false, "isGdpr": false, "sourceRepository": "https://github", "description": "Invoice", "lastUpdatedOn": "2024-02-27 12:03:22", "serviceTier": "Tier 3", "products": [{"name": "EGE", "domain": "", "capability": ""}], "department": {"l1DepartmentName": "IT", "l2DepartmentName": "Admin"}, "discoverySource": null, "applicationType": {"applicationType": null}, "applicationCategory": null, "team": {"name": "product-invoicing", "owner": "Samatar"}},{"id": 2, "name": "stub", "status": "ACTIVE", "isPci": false, "isGdpr": false, "sourceRepository": "https://github", "description": "Invoice", "lastUpdatedOn": "2024-02-27 12:03:22", "serviceTier": "Tier 3", "products": [{"name": "EGE", "domain": "", "capability": ""}], "department": {"l1DepartmentName": "IT", "l2DepartmentName": "Data"}, "discoverySource": null, "applicationType": "test", "applicationCategory": null, "team": {"name": "product-invoicing", "owner": "Samar"}},{"id": 3, "name": "stub", "status": "ACTIVE", "isPci": false, "isGdpr": false, "sourceRepository": "https://github", "description": "Invoice", "lastUpdatedOn": "2024-02-27 12:03:22", "serviceTier": "Tier 3", "products":"name", "department": {"l1DepartmentName": "IT", "l2DepartmentName": "Test"}, "discoverySource": null, "applicationType": "", "applicationCategory": null, "team": {"name": "product-invoicing", "owner": "Samar"}},{"id": 4, "name": "new", "status": "ACTIVE", "isPci": false, "isGdpr": false, "sourceRepository": "https://github", "description": "The repo Team", "lastUpdatedOn": "2024-02-27 12:03:59", "serviceTier": "Tier 3", "products": [{"name": "data-monitoring", "status": "ACTIVE"}], "department": {"l1DepartmentName": "Technology", "l2DepartmentName": "Data"}, "discoverySource": null, "applicationType": {"applicationType": null}, "applicationCategory": null, "team": {"name": "Data Monitoring", "owner": "Seema Garg"}}, {"id": "reject_test_1", "name": "graph", "status": "ACTIVE", "isPci": false, "isGdpr": false, "sourceRepository": "https://github", "description": "graph-property-shell-experience", "lastUpdatedOn": "2024-02-27 10:40:55","products": [{"name": "-partner-experience", "domain": "Supply Partner"}], "department": {"l1DepartmentName": "Test"}, "discoverySource": null, "team": {"name": "OB Core", "contactDl": "OBCore@test.com", "costCenter": "66040", "owner": "Harish"}},{"id":"reject_test_2","domain": "Supply Partner", "test":"test"},{"id":"reject_test_3"},{"id":"reject_test_4","test":"test"}]"""


# Sample DataFrame with a single row which value is a list of JSON.
response_df = spark.createDataFrame([(json_data,)], ["response"])

# Custom Schema  => We can use this Spark schema when we need to work further on the DataFrame, and these columns are required to flatten the data.
custom_schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("status", StringType(), True),
    StructField("isPci", StringType(), True),
    StructField("isGdpr", StringType(), True),
    StructField("sourceRepository", StringType(), True),
    StructField("description", StringType(), True),
    StructField("lastUpdatedOn", StringType(), True),
    StructField("serviceTier", StringType(), True),
    StructField("products", ArrayType(MapType(StringType(), StringType())), True),
    StructField("department", MapType(StringType(), StringType()), True),
    StructField("discoverySource", StringType(), True),
    StructField("applicationType", MapType(StringType(), StringType()), True),
    StructField("applicationCategory", StringType(), True),
    StructField("team", MapType(StringType(), StringType()), True)
])

# If want to use custom schema as below instead of Spark schema as above.
# Define the schema column names
#custom_schema = ["isPci", "applicationCategory", "isGdpr", "name", "products", "description", "team", "status", "applicationType", "id", "sourceRepository", "serviceTier", "department", "lastUpdatedOn", "discoverySource"]

# Extract column names for json validation.
schema_column_names = set([field.name for field in custom_schema]) # When we have custom_schema as Spark Schema. 
#schema_column_names = set(custom_schema) # When we have custom_schema as list of column names instead of Spark Schema.


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

