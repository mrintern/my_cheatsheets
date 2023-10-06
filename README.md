# my_cheatsheets
### examples
## sql
CASE statement
```
ALTER TABLE your_table ADD COLUMN triangle_type VARCHAR(50);

UPDATE your_table
SET triangle_type = CASE
    WHEN side1 = side2 AND side2 = side3 THEN 'Equilateral'
    WHEN side1 = side2 OR side2 = side3 OR side1 = side3 THEN 'Isosceles'
    ELSE 'Scalene'
END;
```
inner join
```
SELECT employees.name, departments.department_name
FROM employees
INNER JOIN departments ON employees.department_id = departments.id;
```
left join
```
SELECT customers.name, orders.order_date
FROM customers
LEFT JOIN orders ON customers.id = orders.customer_id;
```

window function example
```
%sql
SELECT trip_distance, fare_amount, dropoff_zip, COUNT(dropoff_zip) OVER(PARTITION BY dropoff_zip) AS dropoff_zip_count FROM samples.nyctaxi.trips ORDER BY dropoff_zip ASC;
```
scd type 2
Create a temporary view for the existing table and new table:
```
df.createOrReplaceTempView("existing_table")
new_data.createOrReplaceTempView("new_data")

```
Update the date_of_birth for the records that will be expired:
```
spark.sql("""
    UPDATE existing_table 
    SET date_of_birth = (SELECT date_of_birth FROM new_data WHERE new_data.message_id = existing_table.message_id)
    WHERE message_id IN (SELECT message_id FROM new_data)
""")
```
Insert the new data into the table and set date_of_birth as NULL to mark them as the latest data changes:
```
spark.sql("""
    INSERT INTO existing_table (city, date_of_birth, email, message_id, name)
    SELECT city, NULL, email, message_id, name FROM new_data
""")
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


