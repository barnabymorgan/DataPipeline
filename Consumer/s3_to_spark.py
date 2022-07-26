import configparser
from datetime import datetime, timedelta
from email import header
from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf, SparkFiles
import os
import sys
import boto3
from sqlalchemy import true


config = configparser.ConfigParser()
configFilePath = '/Users/barnabymorgan/Desktop/DataPipeline/aws.conf'
config.read(configFilePath)

# Adding the packages required to get data from S3 and cassandra
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# os.environ['JAVA_HOME'] = "/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home"
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions pyspark-shell'

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 pyspark-shell"

# Creating our Spark configuration
# cluster_seeds = ['172.18.0.6', '172.18.0.8']

cluster_seeds = '127.0.0.1'

# Create our Spark session
# spark = SparkSession(sc)
#    .config("spark.cassandra.auth.username", "cassandra") \
#    .config("spark.cassandra.auth.password", "cassandra") \
spark = SparkSession.builder \
    .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-s3:1.12.196,\
        org.apache.hadoop:hadoop-aws:3.3.1,\
        com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .master("local") \
    .appName("S3toSpark") \
    .getOrCreate() 

# Configure the setting to read from the S3 bucket
accessKeyId=config['read']['accessKeyId']
secretAccessKey=config['read']['secretAccessKey']
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

    
spark.sparkContext.setLogLevel("WARN")
# Read from the S3 bucket and load into spark dataframe

def read_from_s3():
    session = boto3.Session(aws_access_key_id=accessKeyId, 
                            aws_secret_access_key=secretAccessKey
                            )
    s3 = session.resource('s3')
    bucket = s3.Bucket('project-data-pipeline')
    consumed_at = (datetime.strftime(datetime.today() 
                    # - timedelta(1)
                    ,'%Y-%m-%d'))

    for obj in bucket.objects.filter(Prefix=f'{consumed_at}'):
        data_schema = StructType([
            StructField('unique_id', StringType(), True),
            StructField('index', IntegerType(), True),
            StructField('title', StringType(), True),
            StructField('description', StringType(), True),
            StructField('poster_name', StringType(), True),
            StructField('follower_count', StringType(), True),
            StructField('tag_list', StringType(), True),
            StructField('is_image_or_video', StringType(), True),            
            StructField('image_src', StringType(), True),
            StructField('downloaded', IntegerType(), True),   
            StructField('save_location', StringType(), True),
            StructField('category', StringType(), True)
        ])
        print(obj)
        df = spark.read.json(f's3a://project-data-pipeline/{obj.key}', schema=data_schema)
        df.show()
        
        write_to_cassandra(df, "batch_data", "pintrest_pipeline")


def read_from_cassandra(table, keyspace) -> DataFrame:
    table_df = spark.read\
      .format("org.apache.spark.sql.cassandra")\
      .options(table=table, keyspace=keyspace)\
      .load()
    print(table_df)
    return table_df

def write_to_cassandra(df, table_name, keyspace_name):
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode('append') \
      .options(table=table_name, keyspace=keyspace_name) \
      .save()


def main():
    read_from_s3()
    read_from_cassandra("demo", "pintrest_pipeline")


if __name__ == "__main__":
    print("main")
    main()