from ipaddress import collapse_addresses
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
# org.postgresql:postgresql:42.2.10
spark = SparkSession.builder \
    .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.jars", "/Users/barnabymorgan/postgresql-42.4.0.jar") \
    .master("local") \
    .appName("KafkaStreaming") \
    .getOrCreate() 

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

jdbc_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres")\
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "pintrest_streaming_pipeline") \
    .option("user", "newuser") \
    .option("password", "password") \
    .load()

jdbc_df.printSchema()

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

# Select the value part of the kafka message and cast it to a string.

stream_df = stream_df.selectExpr("CAST(value AS STRING)")

cols_output = stream_df.select(
    get_json_object('value', '$.unique_id').alias("uuid")
   ,get_json_object('value', '$.category').alias("category")
   ,get_json_object('value', '$.downloaded').alias("download_count")
)


# outputting the messages to the console 
#query = cols_output.writeStream \
#    .format("console") \
#    .outputMode("append") \
#    .start() 
    
#query.awaitTermination()

   
def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF in this foreach

    #cols_df = stream_df.withColumn("value",from_json(stream_df.value,MapType(StringType(),StringType())))

    # Transform the dataframe so that it will have individual columns 
    #cols_output = cols_df.select(["value.unique_id","value.category","value.downloaded"]) 

    stream_df = df.selectExpr("CAST(value AS STRING)")

    cols_output = stream_df.select(
         get_json_object('value', '$.unique_id').alias("uuid")
        ,get_json_object('value', '$.category').alias("category")
        ,get_json_object('value', '$.downloaded').alias("download_count")
    ) 

    # Send the dataframe into MongoDB which will create a BSON document out of it
    cols_output.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres")\
        .option("dbtable", "pintrest_streaming_pipeline") \
        .option("user", "newuser") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    pass

stream_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()


if __name__ == "__main__":
    print("main")
