# my_cheatsheets
### examples
## sql
window function example
```
%sql
SELECT trip_distance, fare_amount, dropoff_zip, COUNT(dropoff_zip) OVER(PARTITION BY dropoff_zip) AS dropoff_zip_count FROM samples.nyctaxi.trips ORDER BY dropoff_zip ASC;
```
## pyspark
accessing nested fields (json data)
+ 
explode
```
df = spark.read.json("/databricks-datasets/COVID/CORD-19/2020-06-04/document_parses/pdf_json/d33a044bbb52673f1eeeb792b0376b0987fe02f6.json")

# acess nested field
df2 = df.select("abstract.text")
# explode text column
df2 = df2.withColumn("text", explode("text"))
df2.show()
```

explode
```
from pyspark.sql.functions import explode

df = df.withColumn("Number", explode("Numbers")).drop("Numbers").show()
```

sql query on dataframe
```
from pyspark.sql import SparkSession
df.createOrReplaceTempView("temp_table")
temp_df = spark.sql("SELECT * FROM temp_table")
temp_df.show()
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
from pyspark.sql import SparkSession

df = spark.sql("SELECT * FROM samples.tpch.orders")
df2 = spark.sql("SELECT * FROM samples.tpch.customer")
# left join customer on orders
result = df.join(broadcast(df2),df.o_custkey == df2.c_custkey,'left')
result.show()
```

autoloader to delta lake example
```
# import functions
from pyspark.sql.functions import col, current_timestamp

# initialize variables
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

df = spark.read.table(table_name)
display(df)
```


