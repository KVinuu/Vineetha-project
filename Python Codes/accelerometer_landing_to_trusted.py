import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read from Landing Zone
accelerometer = glueContext.create_dynamic_frame.from_catalog(
    database="stedidatabase",
    table_name="accelerometer"
)

# Read Customer Trusted data
customer_landing_to_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedidatabase",
    table_name="customer_landing_to_trusted"
)

# Join Accelerometer and Customer Trusted tables
accelerometer_landing_to_trusted = Join.apply(
    frame1=accelerometer_landing_to_trusted, 
    frame2=customer_landing_to_trusted, 
    keys1=["user"], 
    keys2=["email"]
)

# Print row count to debug
print("Row count after join:", accelerometer_landing_to_trusted.count())

# Convert DynamicFrame to JSON format
json_dynamic_frame = accelerometer_landing_to_trusted.repartition(1)

# Write to Trusted Zone
glueContext.write_dynamic_frame.from_options(
    frame=filtered_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://vineethabuckets/accelerometer_landing_to_trusted/"
    "partitionKeys": []
    },
    format="json"
    additional_options={"updateBehavior": "UPDATE_IN_DATABASE"} 
)

job.commit()
