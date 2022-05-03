from datetime import datetime, date 
import pandas as pd 
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType
from pyspark.sql.functions import col, when, udf, count, lit, collect_list, countDistinct

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

df = spark.read.parquet("/temp/out/people.parquet")
df.printSchema()
##########################################################################################
# select & drop
##########################################################################################

df.select(
  col("name")
  , col("score").alias('student_score')
  , col("gender").alias('student_gender')
).show(n=10, truncate=False)
df.select(df.name).show()
df.select(df['name']).show()
cols = [
  col for col in df.columns
]
df.select(cols).show()
df.select("*").show()
df.drop("name").show()  # name 제회한 모든 컬럼 조회


##########################################################################################
# count & countDistinct
##########################################################################################
df.select(count('score'), countDistinct('gender')).show()


##########################################################################################
# withColumnRenamed
##########################################################################################
df.withColumnRenamed('변경 전', '변경 후').show()