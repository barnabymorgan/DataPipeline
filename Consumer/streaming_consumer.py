import findspark
findspark.init()
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'
# specify the topic we want to stream data from.
KAFKA_TOPIC = "datapipeline"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:29092'

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("includeHeaders", "true") \
        .load()



# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value AS STRING)")

words = stream_df.select(
    get_json_object('value', '$.unique_id').alias("uuid")
   ,get_json_object('value', '$.category').alias("category")
   ,get_json_object('value', '$.downloaded').alias("downloaded")

)

# outputting the messages to the console 
query = words.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() 
    
query.awaitTermination()

if __name__ == "__main__":
    print("main")
