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

# create view table
df.createOrReplaceTempView('tableA')
spark.sql("select count(*) from tableA").show()

# pandas_udf
@pandas_udf('integer')
def add_one(s: pd.Series) -> pd.Series:
  return s + 1

spark.udf.register('add_one', add_one)
spark.sql("select add_one(v1) from tableA").show()

# expr 
df.selectExpr('add_one(v1)').show()
df.select(expr('count(*)') > 0).show()

