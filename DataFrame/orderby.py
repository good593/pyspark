from datetime import datetime, date 
import pandas as pd 

from typing import Iterator, Tuple
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, MapType, LongType
from pyspark.sql.functions import col, when, udf, count, lit, collect_list, countDistinct, pandas_udf, asc, desc

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

df = spark.read.parquet("/temp/out/people.parquet")
df.printSchema()

##########################################################################################
# asc, desc
##########################################################################################

df.orderBy(asc('state'), desc('gender')).show()
