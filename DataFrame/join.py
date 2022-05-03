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

##########################################################################################
# join >> inner, left, right, full
##########################################################################################
df1.join(df2, df1.name == df2.name, how='inner')
df1.join(df2, df1.name == df2.name, how='left')
df1.join(df2, df1.name == df2.name, how='right')
df1.join(df2, df1.name == df2.name, how='full')

cond = [df1.name == df2.new_name, df1.score == df2.age]
df = df1.alias("a").join(df2.withColumnRenamed('name', 'new_name').alias("b"), cond, how='left').select("a.*", col("b.new_name"), col("b.age").alias('new_age'))

##########################################################################################
# 조인 키가 여러개인 경우
##########################################################################################
df1.join(df2, on = ['key1', 'key2'], how='inner')
df1.join(df2, on = (df1.key11 == df2.key12) & (df1.key21 == df2.key22), how='inner')
