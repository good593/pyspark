from datetime import datetime, date 
import pandas as pd 
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType
from pyspark.sql.functions import col, when, udf, count, lit, collect_list, countDistinct, array_contains

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
##########################################################################################
# filter / where
##########################################################################################
data = [
  (("James","","Smith"),["Java","Scala","C++"], 8.0, True, 1),
  (("Anna","Rose",""),["Spark","Java","C++"], 2.0, False, 1),
  (("Julia","","Williams"),["CSharp","VB"], 7.0, True, 2),
  (("Maria","Anne","Jones"),["CSharp","VB"], 10.0, True, 1),
  (("Jen","Mary","Brown"),["CSharp","VB"], 5.0, False, 2),
  (("Mike","Mary","Williams"),["Python","VB"], 1.0, True, 2)
]

schema = StructType([
  StructField('name', StructType([
    StructField('firstname', StringType(), True),
    StructField('middlename', StringType(), True),
    StructField('lastname', StringType(), True)
  ])),
  StructField('languages', ArrayType(StringType()), True),
  StructField('score', DoubleType(), True),
  StructField('isstudent', BooleanType(), True),
  StructField('gender', IntegerType(), True)
])

df = spark.createDataFrame(data = data, schema = schema)
df.show(truncate=False)

# null, not null
df.filter(col('name').isNull())
df.filter(col('name').isNotNull())

# 조건
df.filter(df.gender == "M")
df.filter("gender = 'M")
df.filter(df.gender != "M")
df.filter("gender <> 'M")
df.filter(df.score >= 5)

"""
여러 조건
- (조건1) & (조건2), (조건1) | (조건2)
- 조건1 and 조건2, 조건1 or 조건2
"""
df.filter((df.state == "OH") & (df.score < 5))
df.filter("state = 'OH' and gender = 'M'")

# in 조건
df.filter( df.state.isin(['OH', 'CA']) )
df.filter( ~df.state.isin(['OH', 'CA']) )
df.filter( df.state.isin(['OH', 'CA']) == False )

"""
like
- startswith
- endswith
- contains
- like
"""
df.filter(df.state.startswith('N'))
df.filter(df.state.like('N%'))

df.filter(df.state.endswith('H'))
df.filter(df.state.like('%H'))

df.filter(df.state.contains('H'))
df.filter(df.state.like('%H%'))

# StructType 조건
df.filter(df.name.lastname == 'Williams')
df.filter(df.name.lastname.contains('H'))

# ArrayType 조건
df.filter(df.languages[0] == 'Java')
df.filter(array_contains(df.languages, 'Java'))
