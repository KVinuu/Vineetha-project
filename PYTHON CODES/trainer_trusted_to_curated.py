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

# Read from Trusted Zone
trainer_landing_to_trusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://vineethabuckets/trainer_landing_to_trusted/"], "recurse": True},
    transformation_ctx="trainer_landing_to_trusted"
)

# Write to Curated Zone
glueContext.write_dynamic_frame.from_options(
    frame=trainer_landing_to_trusted,
    connection_type="s3",
    connection_options={"path": "s3://vineethabuckets/trainer_trusted_curated/"},
    format="json"
)

job.commit()
