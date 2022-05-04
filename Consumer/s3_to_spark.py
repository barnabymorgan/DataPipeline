import multiprocessing
import findspark
import operator
import pyspark
import json
import boto3
import requests

def write_to_hbase():
    cfg = (
        pyspark.SparkConf()
        .setAppName("fromHbase")
    )

    spark = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

    df = (
        pyspark
        .sql
        .read
        .format()
        .option()
        .option()
        .option()
    )
    
    df.show()