# my_cheatsheets
### examples
pyspark

sql query on dataframe
```
```

left join
```
from pyspark.sql import SparkSession
sc  = SparkSession.builder.getOrCreate()
query = "SELECT * FROM samples.tpch.orders LEFT JOIN samples.tpch.customer ON samples.tpch.orders.o_custkey = samples.tpch.customer.c_custkey"
df = spark.sql(query)
df.show()
```

broadcast join
```
from pyspark.sql.functions import broadcast 
df = spark.sql("SELECT * FROM samples.tpch.orders")
df2 = spark.sql("SELECT * FROM samples.tpch.customer")
# left join customer on orders
result = df.join(broadcast(df2),df.o_custkey == df2.c_custkey,'left')
result.show()
```
