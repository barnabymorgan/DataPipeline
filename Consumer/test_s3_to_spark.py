import pyspark
from pyspark.sql import SparkSession

cfg = (
        pyspark.SparkConf()
        .setMaster("local")
        .setAppName("fromS3")
    )

spark = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

print(spark)

sc = spark.sparkContext

print(sc)

df1 = spark.read.json('python/test_support/sql/people.json')




import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"

import findspark
findspark.init()
from pyspark.sql import SparkSession

import configparser
config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
access_id = config.get(aws_profile, "aws_access_key_id") 
access_key = config.get(aws_profile, "aws_secret_access_key")

sc=spark.sparkContext
hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

df=spark.read.json("s3n://your_file.json")
df.show()