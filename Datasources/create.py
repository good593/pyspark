from datetime import datetime, date 
import pandas as pd 
from pyspark.sql import Row, SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType

# https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou
spark = SparkSession.builder.appName("Your App").config("spark.sql.broadcastTimeout", "36000").getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

##########################################################################################
# parquet
# https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/
##########################################################################################
df = spark.read.parquet("/temp/out/people.parquet")

df.createOrReplaceTempView("ParquetTable")
parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")

df.write.mode('append').parquet("/tmp/output/people.parquet")
df.write.mode('overwrite').parquet("/tmp/output/people.parquet")

df.write.partitionBy("gender","salary").mode("overwrite").parquet("/tmp/output/people2.parquet")


##########################################################################################
# csv
##########################################################################################
df = spark.read.csv("/tmp/resources/zipcodes.csv")
df = spark.read.format("csv").load("/tmp/resources/zipcodes.csv")

# Read Multiple CSV Files
df = spark.read.csv("path1,path2,path3")
# Read all CSV Files in a Directory
df = spark.read.csv("Folder path")
# delimiter & header
df3 = spark.read.options(delimiter=',', header='True').csv("/tmp/resources/zipcodes.csv")

df.printSchema()


##########################################################################################
# orc
##########################################################################################
df.write.orc('zoo.orc')
spark.read.orc('zoo.orc').show()

##########################################################################################
# createDataFrame
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

