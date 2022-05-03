
### spark.sql.broadcastTimeout
https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou

# [pyspark](https://assaeunji.github.io/python/2022-03-26-pyspark/)
- https://sparkbyexamples.com/pyspark-tutorial/
- https://spark.apache.org/docs/latest/
- https://www.tutorialspoint.com/pyspark/index.htm
> 1. 컬럼을 참조할 때  다음과 같이 표현이 가능합니다.
````python
import pyspark.sql.functions as F

df.col
df['col']
F.col('col')
````
> 2. `orderBy` 와 `sort`는 같은 결과를 나타냅니다. 
> 3. `filter` 와 `where`는 같은 결과를 나타냅니다.
> 4. `union` 과 `unionAll`은 같은 결과를 나타냅니다.  
> - pyspark에서는 union, unionAll 모두 unionAll을 의미합니다. 

## pyspark 함수
- 기본 함수
```python
df.printSchema() # 테이블의 스키마를 보여주는 함수
df.collect() # 테이블에서 행을 가져오는 함수
df.show(truncate=False) # 테이블 결과를 보여주는 함수, truncate=False를 사용하면 테이블 내용이 잘맂 않도록 보여줍니다. 
df.describe() # 서머리 결과를 보여주는 함수 
```  
- SQL 함수  
```python
df.select()
df.drop()

df.count()
df.countDistinct()
df.distinct() # 중복 데이터 제거

df.withColumn()
df.withColumnRenamed()

df.filter()
df.groupBy()
df.orderBy()

df.join(how = {inner, full, left, right})
df.unionByName() # union()은 사용하지 마시오!!
df.pivot()
df.row_number().over(Window.partitionBy().orderBy()) 
```


