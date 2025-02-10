import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read from Trusted Zone
trainer_landing_to_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedidatabase",
    table_name="trainer_landing_to_trusted"
)

accelerometer_landing_to_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedidatabase",
    table_name="accelerometer_landing_to_trusted"
)

# Perform INNER JOIN on sensorReadingTime and timestamp
machine_learning_curated = Join.apply(
    frame1=trainer_landing_to_trusted,
    frame2=accelerometer_landing_to_trusted,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"]
)

# Write to S3 Curated Zone
glueContext.write_dynamic_frame.from_options(
    frame=machine_learning_curated,
    connection_type="s3",
    connection_options={"path": "s3://vineethabuckets/machine_learning_curated/"},
    format="json"
)
