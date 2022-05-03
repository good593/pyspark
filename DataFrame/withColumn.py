from datetime import datetime, date 
import pandas as pd 
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, MapType
from pyspark.sql.functions import col, when, udf, count, lit, collect_list, countDistinct, pandas_udf

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

##########################################################################################
# withColumn
##########################################################################################
df = spark.read.parquet("/temp/out/people.parquet")
df.printSchema()

df = df.withColumn('test', df.score - df.age)

# datatype 변경
df.withColumn('gender', df.gender.cast('String')).show()

# add column
df.withColumn('Country', lit("USA")).show()

# condition
condition = when(df.gender == 1, "Male").when(df.gender == 0, "Female").when(df.gender < 0 | df.gender > 1, None).otherwise(df.gender)
df.withColumn('new_gender', condition).show()

##########################################################################################
# withColumnRenamed
##########################################################################################
df.withColumnRenamed('변경 전', '변경 후').show()
