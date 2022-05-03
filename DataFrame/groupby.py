from datetime import datetime, date 
import pandas as pd 

from typing import Iterator, Tuple
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, MapType, LongType
from pyspark.sql.functions import col, when, udf, count, lit, collect_set, collect_list, countDistinct, pandas_udf, sum, avg

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

df = spark.read.parquet("/temp/out/people.parquet")
df.printSchema()

##########################################################################################
# gorupby
##########################################################################################

df.groupBy("department").count().show()
df.groupBy("department").sum("salary").show()
df.groupBy("department").min("salary").show() 
df.groupBy("department").max("salary").show() 
df.groupBy("department").avg("salary").show() 

# agg
(df.groupBy("department")
.agg(sum("salary").alias("sum_salary")
    , avg("salary").alias("avg_salary")
    , sum("bonus").alias("sum_bonus")
    , max("bonus").alias("max_bonus"))).show()

df.groupBy("department","state").sum("salary","bonus").show()

##########################################################################################
# collect_list & collect_set
##########################################################################################

df.groupby('id').agg(collect_list('code'), collect_set('name')).show()

##########################################################################################
# applyInPandas
##########################################################################################

df = spark.createDataFrame([
  ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
  ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
  ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
df.show()
# +-----+------+---+---+
# |color| fruit| v1| v2|
# +-----+------+---+---+
# |  red|banana|  1| 10|
# | blue|banana|  2| 20|
# |  red|carrot|  3| 30|
# | blue| grape|  4| 40|
# |  red|carrot|  5| 50|
# |black|carrot|  6| 60|
# |  red|banana|  7| 70|
# |  red| grape|  8| 80|
# +-----+------+---+---+

def plus_mean(pandas_df):
  return pandas_df.assign(v1=pandas_df.v1 - pandas_df.v1.mean())

df.groupby('color').applyInPandas(plus_mean, schema=df.schema).show()
# +-----+------+---+---+
# |color| fruit| v1| v2|
# +-----+------+---+---+
# |  red|banana| -3| 10|
# |  red|carrot| -1| 30|
# |  red|carrot|  0| 50|
# |  red|banana|  2| 70|
# |  red| grape|  3| 80|
# |black|carrot|  0| 60|
# | blue|banana| -1| 20|
# | blue| grape|  1| 40|
# +-----+------+---+---+

##########################################################################################
# cogroup
##########################################################################################

df1 = spark.createDataFrame(
    [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
    ('time', 'id', 'v1'))

df2 = spark.createDataFrame(
    [(20000101, 1, 'x'), (20000101, 2, 'y')],
    ('time', 'id', 'v2'))

def asof_join(l, r):
    return pd.merge_asof(l, r, on='time', by='id')

df1.groupby('id').cogroup(df2.groupby('id')).applyInPandas(
    asof_join, schema='time int, id int, v1 double, v2 string').show()

# +--------+---+---+---+
# |    time| id| v1| v2|
# +--------+---+---+---+
# |20000101|  1|1.0|  x|
# |20000102|  1|3.0|  x|
# |20000101|  2|2.0|  y|
# |20000102|  2|4.0|  y|
# +--------+---+---+---+