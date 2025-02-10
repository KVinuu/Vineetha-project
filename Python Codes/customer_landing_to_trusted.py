import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read from Landing Zone
customer = glueContext.create_dynamic_frame.from_catalog(
    database="stedidatabase",
    table_name="customer"
)

# Filter customers who have "shareWithResearchAsOfDate"
customer_landing_to_trusted = Filter.apply(
    frame=customer_landing_to_trusted,
    f=lambda x: x["shareWithResearchAsOfDate"] is not None
)
# Convert DynamicFrame to JSON format
json_dynamic_frame = customer_landing_to_trusted.repartition(1)

# Write to Trusted Zone
glueContext.write_dynamic_frame.from_options(
    frame=customer_landing_to_trusted,
    connection_type="s3",
    connection_options={"path": "s3://vineethabuckets/customer_landing_to_trusted/"
    "partitionKeys": []
    },
    format="json"
    additional_options={"updateBehavior": "UPDATE_IN_DATABASE"}
)

job.commit()
