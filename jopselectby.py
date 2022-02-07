import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "blog", table_name = "tb_parquet", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []



memberships = glueContext.create_dynamic_frame.from_catalog(database="blog", table_name="tb_input")
memberships.toDF().createOrReplaceTempView("memberships")
spark.sql("select * from memberships").show()


job.commit()
