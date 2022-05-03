from datetime import datetime, date 
import pandas as pd 

from typing import Iterator, Tuple
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, MapType, LongType
from pyspark.sql.functions import col, when, udf, count, lit, collect_list, countDistinct, pandas_udf, expr

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

df = spark.read.parquet("/temp/out/people.parquet")
df.printSchema()

##########################################################################################
# pivot
##########################################################################################
pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.show(truncate=False)
# +-------+------+-----+------+----+
# |Product|Canada|China|Mexico|USA |
# +-------+------+-----+------+----+
# |Orange |null  |4000 |null  |4000|
# |Beans  |null  |1500 |2000  |1600|
# |Banana |2000  |400  |null  |1000|
# |Carrots|2000  |1200 |null  |1500|
# +-------+------+-----+------+----+

# pivot_values를 정의해주면, 더 가벼운 연산으로 계산을 할 수 있음!!!!
pivot_values = ["USA","China","Canada","Mexico"]
pivotDF = df.groupBy("Product").pivot("Country", pivot_values).sum("Amount")
pivotDF.show(truncate=False)

##########################################################################################
# unpivot
##########################################################################################
unpivotExpr = "stack(4,'USA',USA,'Canada',Canada,'China',China,'Mexico',Mexico) as (Country, Total)"
unPivotDF = (pivotDF
  .select("Product", expr(unpivotExpr))
  .filter("total is not null")
  .orderBy("Product")
)
unPivotDF.show()
# +-------+-------+-----+
# |Product|Country|Total|
# +-------+-------+-----+
# | Banana|  China|  400|
# | Banana| Canada| 2000|
# | Banana|    USA| 1000|
# |  Beans|    USA| 1600|
# |  Beans| Mexico| 2000|
# |  Beans|  China| 1500|
# |Carrots|  China| 1200|
# |Carrots|    USA| 1500|
# |Carrots| Canada| 2000|
# | Orange|    USA| 4000|
# | Orange|  China| 4000|
# +-------+-------+-----+

