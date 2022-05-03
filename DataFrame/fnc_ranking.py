from datetime import datetime, date 
import pandas as pd 

from typing import Iterator, Tuple
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, MapType, LongType
from pyspark.sql.functions import row_number, desc, rank, asc, dense_rank
from pyspark.sql.window import Window

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

df = spark.read.parquet("/temp/out/people.parquet")
df.printSchema()

window_spec = Window.partitionBy('department').orderBy(asc('amount'))
##########################################################################################
# https://sparkbyexamples.com/pyspark/pyspark-window-functions/
# row_number
# : window function is used to give the sequential row number starting from 1 to the result of each window partition.
##########################################################################################
df.withColumn('row_number', row_number().over(window_spec))
# +-------------+----------+------+----------+
# |employee_name|department|salary|row_number|
# +-------------+----------+------+----------+
# |James        |Sales     |3000  |1         |
# |James        |Sales     |3000  |2         |
# |Robert       |Sales     |4100  |3         |
# |Saif         |Sales     |4100  |4         |
# |Michael      |Sales     |4600  |5         |
# |Maria        |Finance   |3000  |1         |
# |Scott        |Finance   |3300  |2         |
# |Jen          |Finance   |3900  |3         |
# |Kumar        |Marketing |2000  |1         |
# |Jeff         |Marketing |3000  |2         |
# +-------------+----------+------+----------+

##########################################################################################
# rank
# : window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.
##########################################################################################
df.withColumn('rank', rank().over(window_spec))
# +-------------+----------+------+----+
# |employee_name|department|salary|rank|
# +-------------+----------+------+----+
# |        James|     Sales|  3000|   1|
# |        James|     Sales|  3000|   1|
# |       Robert|     Sales|  4100|   3|
# |         Saif|     Sales|  4100|   3|
# |      Michael|     Sales|  4600|   5|
# |        Maria|   Finance|  3000|   1|
# |        Scott|   Finance|  3300|   2|
# |          Jen|   Finance|  3900|   3|
# |        Kumar| Marketing|  2000|   1|
# |         Jeff| Marketing|  3000|   2|
# +-------------+----------+------+----+


##########################################################################################
# dense_rank
# : window function is used to get the result with rank of rows within a window partition without any gaps. 
#   This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.
##########################################################################################
df.withColumn('rank', dense_rank().over(window_spec))
# +-------------+----------+------+----------+
# |employee_name|department|salary|dense_rank|
# +-------------+----------+------+----------+
# |        James|     Sales|  3000|         1|
# |        James|     Sales|  3000|         1|
# |       Robert|     Sales|  4100|         2|
# |         Saif|     Sales|  4100|         2|
# |      Michael|     Sales|  4600|         3|
# |        Maria|   Finance|  3000|         1|
# |        Scott|   Finance|  3300|         2|
# |          Jen|   Finance|  3900|         3|
# |        Kumar| Marketing|  2000|         1|
# |         Jeff| Marketing|  3000|         2|
# +-------------+----------+------+----------+








