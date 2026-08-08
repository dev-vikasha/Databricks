# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC For Spark-Connect needed Libraries
# MAGIC
# MAGIC     pip install pyspark pandas pyarrow grpcio protobuf grpcio-status zstandard

# COMMAND ----------

# DBTITLE 1,Ready Spark-Connect Session
# Import SparkSession
from pyspark.sql import SparkSession

# Generate Spark Connect Session
spark = (SparkSession
    .builder 
    .config("spark.jars", "mysql-connector-j-9.7.0.jar")
    .remote("sc://192.168.1.39:15002")
    .getOrCreate()
)
spark

# COMMAND ----------

# DBTITLE 1,Establish Connection With MYSQL
# Reading with the help of spark.read.jdbc
host = '192.168.1.39'

cnx  = {
    'user': 'vikas',
    'password': 'vikas',
    'driver': 'com.mysql.cj.jdbc.Driver'
}

jdbc_url = f"jdbc:mysql://{host}:3306/my_db" # Passed Database Info
query = "(select * from employee) as subquery" # Have to Provide query as Subquery  
df = spark.read.jdbc(url=jdbc_url, table=query, properties=cnx) # Pass it as Table

# COMMAND ----------

# DBTITLE 1,Complex Query for Performance Test
query = "(select country, city , count(*) from customers group by country,city order by count(*) desc) as subquery"
cust_sql = spark.read.jdbc(url=jdbc_url, table=query, properties=cnx)
cust_sql.show(truncate=False,n=10000)

# COMMAND ----------

# DBTITLE 1,Reading with option&format
#Reading with the help of spark.read.format("jdbc")
df = (spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://192.168.1.39/mysql") \
    .option("user", "vikas") \
    .option("password", "vikas") \
    .option("query", "select user,host from user") \
    .load()
)
df.show()

# COMMAND ----------

#Reading with the help of spark.read.format("jdbc")
df = (spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://192.168.1.39/mysql") \
    .option("user", "vikas") \
    .option("password", "vikas") \
    .option("query", "Select * from my_db.customers") \
    .load()
)
df.show()

# COMMAND ----------

# DBTITLE 1,Complex Query Using API's
from pyspark.sql.functions import count, col
df = df.groupBy("country","city").agg(count("*").alias("ct")).orderBy(col("ct").desc())
df.show(n=10000)

# COMMAND ----------

# DBTITLE 1,Try Writing into the DB using Spark- Connect
# Along with Low Level Limitation there are more limit on Spark Connect is there Writing into DB is working with Native Spark Session but not with the Spark-Connect Session as of Now Spark 4.2.0

df = spark.read.csv(r"C:\Users\Vikas Mishra\Documents\Python\Dataset\customers-10000.csv",header=True, inferSchema=True)

df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", "vikas") \
    .option("password", "vikas") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "customers") \
    .mode("append") \
    .save()
