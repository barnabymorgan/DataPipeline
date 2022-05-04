from kafka import KafkaConsumer
import findspark

from API.project_pin_API import KAFKA_TOPIC

findspark.init()

import multiprocessing
import pyspark

KAFKA_TOPIC = "datapipeline"

consumer = KafkaConsumer(
    KAFKA_TOPIC, 
    bootstrap_servers="localhost:29092"
)

# We should always start with session in order to obtain
# context and session if needed
session = pyspark.sql.SparkSession.builder.config(
    conf=pyspark.SparkConf()
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    .setAppName("TestApp")
).getOrCreate()

print(session)

from pyspark.streaming import StreamingContext

# This context can be used with PySpark streaming
# You might have to specify batchDuration (e.g. on which time window operation will be run)
# By default data is collected every 0.5 seconds
ssc = StreamingContext(session.sparkContext, batchDuration=30)

# We will send lines of data to this socketTextStream
lines = ssc.socketTextStream("localhost", 9999)

unique_words = lines.flatMap(lambda text: text.split()).countByValue()

unique_words.pprint()


"""
from kafka import KafkaConsumer
from json import loads

# create our consumer to retrieve the message from the topics
data_stream_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

"""
