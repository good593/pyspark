from datetime import datetime, date 
import pandas as pd 

from typing import Iterator, Tuple
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, MapType, LongType
from pyspark.sql.functions import col,avg,sum,min,max,row_number, asc 
from pyspark.sql.window import Window

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

df = spark.read.parquet("/temp/out/people.parquet")
df.printSchema()

window_spec = Window.partitionBy('department').orderBy(asc('amount'))

##########################################################################################
# https://sparkbyexamples.com/pyspark/pyspark-window-functions/
# cume_dist
# : 
##########################################################################################

windowSpecAgg  = Window.partitionBy("department")

df.withColumn("row",row_number().over(window_spec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()

# +----------+------+-----+----+----+
# |department|   avg|  sum| min| max|
# +----------+------+-----+----+----+
# |     Sales|3760.0|18800|3000|4600|
# |   Finance|3400.0|10200|3000|3900|
# | Marketing|2500.0| 5000|2000|3000|
# +----------+------+-----+----+----+


