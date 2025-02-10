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

# Read from Trusted Zones
customer_trusted_to_curated = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://vineethabuckets/customer_landing_to_trusted/"], "recurse": True},
    transformation_ctx="customer_landing_to_trusted"
)

accelerometer_landing_to_trusted = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://vineethabuckets/accelerometer_landing_to_trusted/"], "recurse": True},
    transformation_ctx="accelerometer_landing_to_trusted"
)
trainer_landing_to_trusted= glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://vineethabuckets/trainer_landing_to_trusted/"], "recurse": True},
    transformation_ctx="trainer_landing_to_trusted"
)
# Join customer and accelerometer data on "email" and "user"
customer_curated = Join.apply(
    frame1=customer_trusted,
    frame2=accelerometer_trusted,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="customer_trusted_to_curated"
)

# Drop unwanted fields
customer_curated_cleaned = DropFields.apply(
    frame=customer_curated,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="customer_curated_cleaned"
)

# Write to Curated Zone
glueContext.write_dynamic_frame.from_options(
    frame=customer_curated_cleaned,
    connection_type="s3",
    connection_options={"path": "s3://vineethabuckets/customer_trusted_to_curated/"},
    format="json"
)

job.commit()
