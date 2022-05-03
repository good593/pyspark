from datetime import datetime, date 
import pandas as pd 

from typing import Iterator, Tuple
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, MapType, LongType
from pyspark.sql.functions import col, when, udf, count, lit, collect_list, countDistinct, pandas_udf

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

df1 = spark.read.parquet("/temp/out/people.parquet")
df1.printSchema()

df2 = spark.read.parquet("/temp/out/people.parquet")
df2.printSchema()

for col in [ col for col in df2.columns if col not in df1.columns ]:
  df1 = df1.withColumn(col, lit(None))

for col in [ col for col in df1.columns if col not in df2.columns ]:
  df2 = df2.withColumn(col, lit(None))

##########################################################################################
# union: 컬럼 순서대로 union을 하지 않으면 잘못된 결과를 얻음. 
# - 컬럼의 개수가 같아야 하며,
# - 컬럼 순서도 동일하게 두어야 제대로된 결과를 얻을 수 있음
##########################################################################################
df1.union(df2).show() # 잘못된 결과
df1.select('name', 'dept', 'state', 'age').union(df2.select('name', 'dept', 'state', 'age')).show() 

##########################################################################################
# unionByName: 컬럼 순서를 조정할 필요 없이 컬럼 이름에 맞게 union을 해줌
##########################################################################################
df1.unionByName(df2).show()


