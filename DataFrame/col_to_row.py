from datetime import datetime, date 
import pandas as pd 

from typing import Iterator, Tuple
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, MapType, LongType
from pyspark.sql.functions import explode, collect_set, collect_list

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

arrayData = [
  ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
  ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
  ('Robert',['CSharp',''],{'hair':'red','eye':''}),
  ('Washington',None,None),
  ('Jefferson',['1','2'],{})
]
df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show()

# root
#  |-- name: string (nullable = true)
#  |-- knownLanguages: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- properties: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)

# +----------+--------------+--------------------+
# |      name|knownLanguages|          properties|
# +----------+--------------+--------------------+
# |     James| [Java, Scala]|[eye -> brown, ha...|
# |   Michael|[Spark, Java,]|[eye ->, hair -> ...|
# |    Robert|    [CSharp, ]|[eye -> , hair ->...|
# |Washington|          null|                null|
# | Jefferson|        [1, 2]|                  []|
# +----------+--------------+--------------------+

##########################################################################################
# columns to rows
# https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/
##########################################################################################
df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()

# root
#  |-- name: string (nullable = true)
#  |-- col: string (nullable = true)

# +---------+------+
# |     name|   col|
# +---------+------+
# |    James|  Java|
# |    James| Scala|
# |  Michael| Spark|
# |  Michael|  Java|
# |  Michael|  null|
# |   Robert|CSharp|
# |   Robert|      |
# |Jefferson|     1|
# |Jefferson|     2|
# +---------+------+

##########################################################################################
# collect_list & collect_set
##########################################################################################

df.groupby('name').agg(collect_set('knownLanguages')).show()


