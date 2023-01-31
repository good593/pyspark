import sys

from pyspark.context import SparkContext

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from layer.common import * # s3://poc-glue/script/batch/daliy/layer.zip
from libs.preprocessing import * # s3://poc-glue/script/daliy/libs.zip

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'target_dt', 'stage']) # step_function.asl.json
# init
glueContext = GlueContext(SparkContext())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# params
target_dt = args['target_dt']
stage = args['stage']
logger = glueContext.get_logger()
logger.info("aws glue start!!")

# pyspark 로직 구현 
# template.yml에 정의됨 >> layer.common & libs.preprocessing 사용 가능

# job start!!
job.commit()