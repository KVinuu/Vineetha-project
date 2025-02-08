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
customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedidatabase",
    table_name="customer"
)

# Filter customers who have "shareWithResearchAsOfDate"
customer_trusted = Filter.apply(
    frame=customer_landing,
    f=lambda x: x["shareWithResearchAsOfDate"] is not None
)

# Write to Trusted Zone
glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted,
    connection_type="s3",
    connection_options={"path": "s3://vineethabuckets/customer_trusted/"},
    format="json"
)

job.commit()
