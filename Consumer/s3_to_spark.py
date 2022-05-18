from pyspark.sql import SparkSession
import pyspark.sql
from pyspark import SparkContext, SparkConf
import os

# Adding the packages required to get data from S3 and cassandra
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
accessKeyId=""
secretAccessKey=""
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session
spark=SparkSession(sc)

# Read from the S3 bucket
# df = spark.read.format('json').load('s3a://project-data-pipeline/fbe53c66-3442-4773-b19e-d3ec6f54dddf.json')
df = spark.read.json('s3a://project-data-pipeline/8fb2af68-543b-4639-8119-de33d28706ed.json')
df.show()

if __name__ == "__main__":
    print("main")