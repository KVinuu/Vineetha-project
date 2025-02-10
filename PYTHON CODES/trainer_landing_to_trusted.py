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
trainer_landing_to_trusted= glueContext.create_dynamic_frame.from_catalog(
    database="stedidatabase",
    table_name="trainer_landing_to_trusted"
)

# Load Customer Trusted data
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedidatabase",
    table_name="customer_landing_to_trusted"
)

#  Corrected Join: Match using serialNumber
joined_data = step_trainer_landing.join(
    paths1=["serialNumber"],  # From step_trainer_landing
    paths2=["serialNumber"],  # From customer_trusted
    frame2=customer_landing_to_trusted
)

#  Convert to DataFrame to drop duplicates
joined_df = joined_data.toDF().dropDuplicates()

#  Convert back to DynamicFrame
deduplicated_data = DynamicFrame.fromDF(joined_df, glueContext)

#  Select only relevant columns
selected_columns = deduplicated_data.select_fields(["serialNumber", "sensorReadingTime", "distanceFromObject"])

# Write to Trusted Zone
glueContext.write_dynamic_frame.from_options(
    frame=trainer_landing_to_trusted,
    connection_type="s3",
    connection_options={"path": "s3://vineethabuckets/trainer_landing_to_trusted/"},
    format="json"
)

job.commit()
