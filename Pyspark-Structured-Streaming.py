# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Invoke Spark Session

# COMMAND ----------

# Famous word count problem to Learn Batch to Streaming 
# Generate spark session

from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Reading From Sockets")
    .getOrCreate()
)
spark

# COMMAND ----------

spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",12)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Batch Code 

# COMMAND ----------

#Read Input Data
df_raw = spark.read.format("text").load(r"C:\Users\vikas.mishra07\Documents\Custom Office Templates\Networking.txt")

# COMMAND ----------

# Consists only one column name 'value' this value will hold line as rows
df_raw.printSchema()

# COMMAND ----------

df_raw.show()

# COMMAND ----------

# Split the line into the list of words
from pyspark.sql.functions import split

df_words = df_raw.withColumn("words",split("value"," "))

# COMMAND ----------

df_words.show()

# COMMAND ----------

# Explode the list of Words
from pyspark.sql.functions import explode

df_word = df_words.withColumn("word",explode("words")).drop("words","value")

# COMMAND ----------

df_word.show()

# COMMAND ----------

# Aggregate the words to generate count
from pyspark.sql.functions import count,lit, sum
df_agg = df_word.groupBy("word").agg(count("*").alias("cnt"), sum(lit(1)).alias("sum")).orderBy("cnt",ascending = False)

# COMMAND ----------

df_agg.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert this Code into Streaming

# COMMAND ----------

#Read Input Data # Nothing Happens Its a Transformation
df_raw = (  spark.readStream.format("socket")
            .option("host", "3.6.199.114")
            .option("port","9999")
            .load()
)

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------

# Split the line into the list of words
from pyspark.sql.functions import split

df_words = df_raw.withColumn("words",split("value"," "))

# COMMAND ----------

# Explode the list of Words
from pyspark.sql.functions import explode

df_word = df_words.withColumn("word",explode("words")).drop("words","value")

# COMMAND ----------

# Aggregate the words to generate count
from pyspark.sql.functions import count,lit, sum
df_agg = df_word.groupBy("word").agg(count("*").alias("cnt"))

# COMMAND ----------

# Write the Output to the Console Straeming

df_agg.writeStream.format("console").outputMode("complete").start().awaitTermination()

# COMMAND ----------

from pyspark.sql.functions import split, explode, count

# Read from socket
df_raw = (
    spark.readStream
         .format("socket")
         .option("host", "3.6.199.114")
         .option("port", "9999")
         .load()
)

# Transform
df_words = df_raw.withColumn("words", split("value", " "))
df_word = df_words.withColumn("word", explode("words")).drop("words", "value")
df_agg  = df_word.groupBy("word").agg(count("*").alias("cnt"))

# Write to console in COMPLETE mode
query = (
    df_agg.writeStream
          .outputMode("complete")          # <-- correct placement
          .format("console")
          .start()
)

# COMMAND ----------

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07 Spark Streaming Read from Files | Flatten JSON data

# COMMAND ----------

# Invoke the Spark Session 
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Reading JSON Data")
    .config("spark.sql.shuffle.partitions", "12")
    .config("spark.streaming.stopGracefullyOnShutdown", True)
    .master("local[*]")
    .getOrCreate()
)

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading JSON Data as a Batch Job

# COMMAND ----------

# Create the streaming_df to read from input directory

streaming_df = (
    spark.read
    .format('json')
    .load(r"C:\Users\vikas.mishra07\Documents\Code\InputData\device.json")
)

streaming_df.printSchema()

# COMMAND ----------

# Let's Explode the data as Devices contains list/array of device reading
from pyspark.sql.functions import explode,col

# Array type is needed to be Exploded as they can be seprarated in Rows
exploded_df = streaming_df.withColumn("data_devices",explode("data.devices"))

exploded_df.printSchema()
exploded_df.show(truncate=False)

# COMMAND ----------

# Flatten the exploded DF - Struct need to be flatten as it consists corresponding Data
flatten_df = ( exploded_df.drop("data")
              .withColumn("deviceId", col("data_devices.deviceId"))
              .withColumn("measure", col("data_devices.measure"))
              .withColumn("status", col("data_devices.status"))
              .withColumn("temperature", col("data_devices.temperature"))
              .drop("data_devices")
)

flatten_df.printSchema()
flatten_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading JSON Files as a Streaming JOB

# COMMAND ----------

# To allow automatic schemaInference while reading
spark.conf.set("spark.sql.streaming.schemaInference",True)

# Create a streaming_df to read from input Directory
streaming_df = (
    spark.readStream
    .format("json")
    # .option("multiLine",True)             
    .option("cleanSource","archive")        # Default is OFF other option is archive & delete
    .option("sourceArchiveDir","archive_dir")
    .option("maxFilesPerTrigger",1)
    .load(r"C:\Users\vikas.mishra07\Documents\Code\InputData-EwD")
)

# COMMAND ----------

# Let's Explode the data as Devices contains list/array of device reading
from pyspark.sql.functions import explode,col

exploded_df = streaming_df.withColumn("data_devices",explode("data.devices"))

exploded_df.printSchema()

# COMMAND ----------

# Flatten the exploded DF
flatten_df = ( exploded_df.drop("data")
              .withColumn("deviceId", col("data_devices.deviceId"))
              .withColumn("measure", col("data_devices.measure"))
              .withColumn("status", col("data_devices.status"))
              .withColumn("temperature", col("data_devices.temperature"))
              .drop("data_devices")
)

flatten_df.printSchema()

# COMMAND ----------

# Write the output to the console Sink to Check the Output
query = (
    flatten_df
    .writeStream
    .format("console")
    .option("outputMode","append").start()    # complete not supported in fileStream
)

# COMMAND ----------

# write the output into the csv File-Format in Streaming Mode
query = (
    flatten_df
    .writeStream.format("csv")
    .option("outputMode","append")
    .option("path",r"C:\Users\vikas.mishra07\Documents\Code\archive_dir")
    .option("checkpointLocation",r"C:\Users\vikas.mishra07\Documents\Code\checkpointLocation")
    .start()
)
#Checkpoint Deirectory -> it helps to Store metadata of Streaming Applications. Allow application to be falut Tolerant incase or restarts

# COMMAND ----------

query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basics of Kafka/Pub-Sub Architechture

# COMMAND ----------

import findspark
findspark.init()

# COMMAND ----------

spark.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC     Kafka : **Open-Source Distributed Streaming Event Service** initially Developed by LinkedIn and Donated to Apache Foundation.

# COMMAND ----------

# Create the Spark Session
from pyspark.sql import  SparkSession

spark = (
   SparkSession.builder
   .appName("Streaming from kafka")
   .config("spark.sql.stopGracefullyOnShutdown",True)
   .config("spark.sql.shuffle.partitions",12)       # fixing at max Number of Cores
   .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
   # .master("local[*]")
   .getOrCreate()
)
spark

# COMMAND ----------

# Extreme Cases to Support Kafka without Library
import os, glob, sys
from pyspark.sql import SparkSession

# --- 1) Configure paths for Windows ---
JAR_DIR = r"C:\spark4.1\jars"  # where you copied the 11 jars

jar_files = [
    "com.google.code.findbugs_jsr305-3.0.0.jar",
    "org.apache.commons_commons-pool2-2.12.1.jar",
    "org.apache.hadoop_hadoop-client-api-3.4.2.jar",
    "org.apache.hadoop_hadoop-client-runtime-3.4.2.jar",
    "org.apache.kafka_kafka-clients-3.9.1.jar",
    "org.apache.spark_spark-sql-kafka-0-10_2.13-4.1.1.jar",
    "org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.1.1.jar",
    "org.lz4_lz4-java-1.8.0.jar",
    "org.scala-lang.modules_scala-parallel-collections_2.13-1.2.0.jar",
    "org.slf4j_slf4j-api-2.0.17.jar",
    "org.xerial.snappy_snappy-java-1.1.10.8.jar",
]

abs_jars = [os.path.join(JAR_DIR, f) for f in jar_files]
missing = [p for p in abs_jars if not os.path.exists(p)]
if missing:
    raise FileNotFoundError("Missing jars:\n" + "\n".join(missing))
print("All 11 jars found ✔")

# --- 2) Guard: detect duplicate/conflicting Kafka/Spark-Kafka jars on the classpath roots ---
#    Scan a few common locations; adjust if your setup differs.
search_roots = [
    JAR_DIR,
    os.path.join(os.path.dirname(sys.executable), "Lib", "site-packages", "pyspark", "jars"),  # PyPI pyspark jars (if installed)
    r"C:\spark4.1\jars",  # Spark distro jars
]
dups = []
for root in search_roots:
    if not os.path.isdir(root): 
        continue
    for pat in ["*spark-sql-kafka-0-10*.jar", "*kafka-clients*.jar", "*spark-token-provider-kafka-0-10*.jar"]:
        for p in glob.glob(os.path.join(root, pat)):
            dups.append(p)
if dups:
    print("Info: Kafka-related jars found on disk (check for duplicates across different versions):")
    for p in sorted(dups):
        print("  -", p)

# --- 3) Build classpath strings (Windows uses semicolons) ---
jars_csv = ",".join(abs_jars)        # for spark.jars (comma-separated)
cp_sep   = ";"                       # Windows driver/executor classpath separator
cp_all   = cp_sep.join(abs_jars)

# --- 4) (Optional) Java sanity check: prefer JDK 17 for Spark 4.1.x ---
print("JAVA_HOME =", os.environ.get("JAVA_HOME", "<not set>"))

# --- 5) Create SparkSession offline (no --packages) ---
spark = (
    SparkSession.builder
      .appName("Kafka offline (Windows notebook)")
      .master("local[*]")
      .config("spark.jars", jars_csv)                          # ship to executors
      .config("spark.driver.extraClassPath", cp_all)           # make driver see the jars
      .config("spark.executor.extraClassPath", cp_all)         # make executors see the jars
      .config("spark.sql.stopGracefullyOnShutdown", True)
      .config("spark.sql.shuffle.partitions", 12)
      .getOrCreate()
)

spark

# COMMAND ----------

# host = "3.6.199.114:9092"
host = "117.194.98.197:9094"

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", host)
    .option("subscribe", "test-topic")
    .option("startingOffsets", "latest")
    .load()
)

# View Schema for Raw kafka_df
kafka_df.printSchema()
# Key:NULL we have not Mentioned any Key in Topic
# Value : consists our MSG in binary Format
# All other column is generated by Kafka for Audits
df_raw = kafka_df.selectExpr("CAST(value AS STRING)")

# COMMAND ----------


df = kafka_df.selectExpr("CAST(value AS STRING)")
# (
#     df.writeStream.format("csv").m,
#     # .outputMode("append")
#     # .trigger(processingTime="10 seconds")
#     # .option("path","/Volumes/main/default/kafka/kafka_csv")
#     # .option("checkpointLocation","/Volumes/main/default/kafka/kafka_chk")
#     # .trigger(availableNow=True)
#     .start()
# )

df.show()

df.write.format("csv").mode("overwrite").option("path","/Volumes/main/default/kafka/kafka_csv").option("header","true").save()

# COMMAND ----------

from pyspark.sql.functions import * 
df_csv = spark.read.format("csv").option("header","true").load("/Volumes/main/default/kafka/kafka_csv/")

df = df_csv.orderBy(col("timestamp").desc())
display(df)

# COMMAND ----------

# Checking JAR Path for Kafka
import pyspark, os
print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))


# COMMAND ----------

# Use for Batch Only One Shot All commands
kafka_df = (spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", "117.194.98.197:9094")
          .option("subscribe", "test-topic")
          .option("startingOffsets", "earliest")
          .load())

kafka_df.printSchema()
value_df = kafka_df.selectExpr("CAST(value AS STRING)")
value_df.show(10, truncate=False)

# COMMAND ----------

# Parse Value from Binary to String into kafka_json_df
from pyspark.sql.functions import expr
kafka_decoded_df = kafka_df.withColumn("value",expr("cast(value as string)"))

# COMMAND ----------

value_df = kafka_decoded_df.select("value")
value_df.printSchema()

# COMMAND ----------

#Schema of the payload

from pyspark.sql.types import (
    StructType, StructField, StringType,
    ArrayType, LongType
)

schema = StructType([
    StructField("customerId", StringType(), True),
    StructField("data", StructType([
        StructField("devices", ArrayType(
            StructType([
                StructField("deviceId", StringType(), True),
                StructField("measure", StringType(), True),
                StructField("status", StringType(), True),
                StructField("temperature", LongType(), True)
            ]),
            True
        ), True)
    ]), True),
    StructField("eventId", StringType(), True),
    StructField("eventOffset", LongType(), True),
    StructField("eventPublisher", StringType(), True),
    StructField("eventTime", StringType(), True)
])

# COMMAND ----------

from pyspark.sql.functions import from_json, col

streaming_df = value_df.withColumn("values_json",from_json(col("value"),schema))

# COMMAND ----------

# to get same data structure of JSON 
# streaming_df = streaming_df.selectExpr("values_json.*")
streaming_df.printSchema()

# COMMAND ----------

streaming_df.show(truncate=False)

# COMMAND ----------

# Let's Explode the data as Devices contains list/array of device reading
from pyspark.sql.functions import explode,col
exploded_df = ( streaming_df
            .withColumn("data_device",explode(col("values_json.data.devices")))
            .withColumn("customerId",col("values_json.customerId"))
            .withColumn("deviceId",col("data_device.deviceId"))
            .withColumn("measure",col("data_device.measure"))
            .withColumn("status",col("data_device.status"))
            .withColumn("temperature",col("data_device.temperature"))
            .withColumn("eventId",col("values_json.eventId"))
            # .withColumn("eventOffset",col("values_json.eventOffset"))
            # .withColumn("eventPublisher",col("values_json.eventPublisher"))
            .withColumn("eventTime",col("values_json.eventTime"))
            .drop("value","values_json","data_device")
)

exploded_df.printSchema()
exploded_df.show(truncate=False)

# COMMAND ----------

# Consume the data in console as Sink 
query = (
    exploded_df
    .writeStream
    .format("console")
    .option("outputMode","complete")
    .start()
)

# COMMAND ----------

# File Writer in Streaming Mode
query = (
    exploded_df.
    writeStream.format("csv")
    .outputMode("append")
    .option("path",r"C:\Users\vikas.mishra07\Documents\Code\Kafka_logs")
    .option("checkpointLocation",r"C:\Users\vikas.mishra07\Documents\Code\chk_kafka_loc")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC     Q. What is Checkpoint in Structured-Streaming ? 
# MAGIC     Ans. Checkpointing is a mechanism in Spark Structured Streaming that allows the system to save the state of the streaming application at regular intervals. This is crucial for fault tolerance and recovery. If a failure occurs, Spark can restart the streaming application from the last checkpoint, ensuring that no data is lost and processing can continue seamlessly. Checkpointing typically involves saving metadata about the streaming query, as well as any intermediate state information needed to resume processing.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Spark Streaming Triggers - TUNE KAFKA 
# MAGIC     . AUTOMATE STREAMING DATA FROM KAFKA -  Program which continuously generating the Data in Producer
# MAGIC     
# MAGIC     · TRIGGERS IN SPARK STREAMING
# MAGIC         · ONCE / AVAILABLENOW - (Make the Pipeline behave as Batch use of KAPPA Architechture)
# MAGIC         · PROCESSINGTIME (Job Triggers in a Mentioned time and consumes & process all available data)
# MAGIC         · CONTINUOUS (EXPERIMENTAL still not supported all Operations like explode and SINK like console)
# MAGIC         - supported SINK is "memory" and can be queried later with SQL 
# MAGIC
# MAGIC     · TUNE KAFKA STREAMING JOB (Increse Parallelism via Increasing number of partitions in Kafka Topic)
# MAGIC         --alter --topic device-event --partitions 8

# COMMAND ----------

query = (
    exploded_df.
    writeStream.format("csv")
    .outputMode("append")
    .trigger(once=True)
    .trigger(processingTime="10 seconds")
    .option("path",r"\Code\Kafka_logs")
    .option("checkpointLocation",r"C:\Users\vikas.mishra07\Documents\Code\chk_kafka_loc")
    .start()
)

# COMMAND ----------

query = (
    exploded_df.
    writeStream.format("memory")
    .outputMode("append")
    .trigger(continuous="10 seconds")
    .queryName("kafka_streaming_query")
    .option("checkpointLocation",r"Code\chk_kafka_loc")
    .start()
)

# COMMAND ----------

# Qurey to get the data from Memory Sink
query_df = spark.sql("select * from kafka_streaming_query")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Writing Data to the Multiple SINK
# MAGIC     . Parquet
# MAGIC     . MYSQL/POStgres
# MAGIC ### Problem with Multiple SINK WriteStream 
# MAGIC - SINK's can have different latency to they are not processing microbatch in same time this causes for every microbatch both SINK will process whole microDF DAG differently leads to more processing.
# MAGIC ### Solution
# MAGIC -  add foreachBatch(python_function) it calls python_function & sends microbatch_DF,batch_ID as a Parameter which behaves like Batch processing and we can write as many as SINK's inside the python_function

# COMMAND ----------

def device_data_writing(df,batch_id):
    df.show(truncate=False)

    # Print Batch ID
    print(f"Batch ID: {str(batch_id)}")
    print(f"Number of Records in Batch: {df.count()}")
    print(f"Number of Partitions in Batch: {df.rdd.getNumPartitions()}")
    from datetime import datetime
    print("Current Time: ", datetime.now())
    # Write the data into Parquet Format in Append Mode
    df.write.format("parquet").mode("append").save(r"Code\device_data_parquet")

    # Write the data into MYSQL Database in Append Mode
    (
        df.write.format("jdbc").mode("append")
        .option("url", "jdbc:mysql://192.168.1.39:3306/my_db")
        .option("dbtable", "device_data")
        .option("user", "vikas")
        .option("password", "vikas")
        .save()
    )

# COMMAND ----------

try:
    (
        exploded_df
        .writeStream
        .foreachBatch(device_data_writing)
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation",r"Code\chk_kafka_multiple_SINK")
        .start()
        .awaitTermination()
    )

except KeyboardInterrupt:
    print("🔴 Producer Stopped By User")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Spark Event Streaming vs Processing Time
# MAGIC     . LATE ARRIVAL OF DATA - arrived late for processing when event generated due to network/error
# MAGIC
# MAGIC     . EVENT TIME VS PROCESSSING TIME - when Event generated vs when event actually reached in processing stage
# MAGIC         * in case of sreaming we always prefer to do aggregations based on event time not on processing time beacause the reason is in case of Late arrival of data processing time can make the windows different than the exact event time windows which can lead to wrong results.
# MAGIC     
# MAGIC     . STATEFUL PROCESSING - some processing is happening in windows like Avg temprature in every hour so hour is a window now consider we are processing based on event time so we have to maintain the state of the window for every hour and when late data arrives we can update the state of that window and give the correct result. 
# MAGIC
# MAGIC         *Cons: Suppose if event is extremely late, it might be dropped or might cause recomputation of multiple windows, leading to increased resource usage and potential performance issues.
# MAGIC
# MAGIC         *Solution: To mitigate the issues of late data, Spark Structured Streaming provides a feature called watermarking. Watermarking allows you to specify a threshold for how late data can arrive and still be considered for processing. By setting a watermark, you can limit the amount of state that needs to be maintained and reduce the risk of unbounded state growth.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Window Operation in Streaming
# MAGIC     . TUMBLING OR FIXED WINDOW :
# MAGIC         * A tumbling window is a type of windowing operation in stream processing where the data is divided into fixed-size, non-overlapping intervals. Each interval, or "window," contains all the events that occur within that time frame. Once a window is closed, it does not overlap with the next window, and a new window starts immediately after the previous one ends.
# MAGIC
# MAGIC         * Example: If you have a tumbling window of 5 minutes, the first window will cover events from 00:00 to 00:05, the second window from 00:05 to 00:10, and so on.
# MAGIC
# MAGIC     . SLIDING OR HOPPING WINDOW :
# MAGIC         * A sliding window is a type of windowing operation where the windows can overlap. The window "slides" over the data at a specified interval, called the slide duration. This allows events to be included in multiple windows.
# MAGIC
# MAGIC         * Example: If you have a sliding window of 5 minutes with a slide duration of 1 minute, the first window will cover events from 00:00 to 00:05, the second window from 00:01 to 00:06, the third window from 00:02 to 00:07, and so on.
# MAGIC         - OVERLAPPING Duration = Window Duration - Slide Duration
# MAGIC          
# MAGIC     . SESSION WINDOW :
# MAGIC         * A session window is a type of windowing operation where the window is defined by periods of activity separated by periods of inactivity, called gaps. The window starts when an event arrives and closes if no new events arrive within the specified gap duration.
# MAGIC
# MAGIC         * Example: If you have a session window with a gap duration of 5 minutes, a new window will start with the first event, and the window will close if no events arrive for 5 minutes. If a new event arrives after the gap, a new session window will start.

# COMMAND ----------

import findspark
findspark.init()

# Create the Spark Session
from pyspark.sql import  SparkSession

spark = (
   SparkSession.builder
   .appName("Streaming from kafka")
   .config("spark.sql.stopGracefullyOnShutdown",True)
   .config("spark.sql.shuffle.partitions",12)       # fixing at max Number of Cores
   # .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
   .master("local[*]")
   .getOrCreate()
)
spark

# COMMAND ----------

host = "localhost:9092"

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", host)
    .option("subscribe", "device_events_window")
    .option("startingOffsets", "earliest")
    .load()
)

# View Schema for Raw kafka_df
kafka_df.printSchema()
# Key:NULL we have not Mentioned any Key in Topic
# Value : consists our MSG in binary Format
# All other column is generated by Kafka for Audits

# COMMAND ----------

from pyspark.sql.functions import window, col, from_json, explode, count, split

json_schema = "event_time string, data string"
kafka_decoded_df = kafka_df.selectExpr("CAST(value AS STRING) as value")

kafka_json_df = kafka_decoded_df.withColumn("json_data", from_json(col("value"), json_schema)) \

flatten_df = kafka_json_df.select("json_data.event_time", "json_data.data")

#Split the data into the Words
words_df = flatten_df \
            .withColumn("words", split(col("data"), " ")) \
            .withColumn("word",explode("words")) \
            .withColumn("event_time", col("event_time").cast("timestamp"))

df_agg  = words_df \
            .withWatermark("event_time", "10 minutes") \
            .groupBy(window("event_time", "10 minutes") , "word")  \
            .agg(count("*").alias("cnt"))

window_df = df_agg.selectExpr("window.start as window_start", "window.end as window_end", "word", "cnt")

window_df.printSchema()

# COMMAND ----------

def device_data_writing(df,batch_id):
    df.show(truncate=False)

    # Print Batch ID
    print(f"Batch ID: {str(batch_id)}")
    df.show(truncate=False)

# COMMAND ----------

try:
    (
        window_df
        .writeStream
        .foreachBatch(device_data_writing)
        # .format("console")
        # .outputMode("update")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation",r"C:\Users\vikas.mishra07\Documents\Code\window_chk_kafka")
        .start()
    )

except KeyboardInterrupt:
    print("🔴 Producer Stopped By User")

# COMMAND ----------

query = (
        window_df
        .writeStream
        # .foreachBatch(device_data_writing)
        .format("console")
        .outputMode("complete")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation",r"C:\Users\vikas.mishra07\Documents\Code\window_chk_kafka")
        .start()
    )

# COMMAND ----------

query.stop()
