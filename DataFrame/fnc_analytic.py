from datetime import datetime, date 
import pandas as pd 

from typing import Iterator, Tuple
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, MapType, LongType
from pyspark.sql.functions import desc, asc, cume_dist, lag, lead
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
# : window function is used to get the cumulative distribution of values within a window partition.
# This is the same as the DENSE_RANK function in SQL
##########################################################################################
df.withColumn("cume_dist",cume_dist().over(window_spec)).show()
# +-------------+----------+------+------------------+
# |employee_name|department|salary|         cume_dist|
# +-------------+----------+------+------------------+
# |        James|     Sales|  3000|               0.4|
# |        James|     Sales|  3000|               0.4|
# |       Robert|     Sales|  4100|               0.8|
# |         Saif|     Sales|  4100|               0.8|
# |      Michael|     Sales|  4600|               1.0|
# |        Maria|   Finance|  3000|0.3333333333333333|
# |        Scott|   Finance|  3300|0.6666666666666666|
# |          Jen|   Finance|  3900|               1.0|
# |        Kumar| Marketing|  2000|               0.5|
# |         Jeff| Marketing|  3000|               1.0|
# +-------------+----------+------+------------------+


##########################################################################################
# https://sparkbyexamples.com/pyspark/pyspark-window-functions/
# lag
# : 이전 행의 값을 리턴
# This is the same as the LAG function in SQL
##########################################################################################
df.withColumn("lag",lag('salary', 2).over(window_spec)).show()
# +-------------+----------+------+----+
# |employee_name|department|salary| lag|
# +-------------+----------+------+----+
# |        James|     Sales|  3000|null|
# |        James|     Sales|  3000|null|
# |       Robert|     Sales|  4100|3000|
# |         Saif|     Sales|  4100|3000|
# |      Michael|     Sales|  4600|4100|
# |        Maria|   Finance|  3000|null|
# |        Scott|   Finance|  3300|null|
# |          Jen|   Finance|  3900|3000|
# |        Kumar| Marketing|  2000|null|
# |         Jeff| Marketing|  3000|null|
# +-------------+----------+------+----+


##########################################################################################
# https://sparkbyexamples.com/pyspark/pyspark-window-functions/
# lead
# : 다음 행의 값을 리턴
# This is the same as the LEAD function in SQL
##########################################################################################
df.withColumn("lag",lead('salary', 2).over(window_spec)).show()
# +-------------+----------+------+----+
# |employee_name|department|salary|lead|
# +-------------+----------+------+----+
# |        James|     Sales|  3000|4100|
# |        James|     Sales|  3000|4100|
# |       Robert|     Sales|  4100|4600|
# |         Saif|     Sales|  4100|null|
# |      Michael|     Sales|  4600|null|
# |        Maria|   Finance|  3000|3900|
# |        Scott|   Finance|  3300|null|
# |          Jen|   Finance|  3900|null|
# |        Kumar| Marketing|  2000|null|
# |         Jeff| Marketing|  3000|null|
# +-------------+----------+------+----+
