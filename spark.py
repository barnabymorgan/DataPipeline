import multiprocessing
import findspark
import operator
import pyspark
import json

findspark.init()

cfg = (
    pyspark.SparkConf()
    # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("TestApp")
    # Setting config value via string
    .set("spark.eventLog.enabled", False)
    # Setting environment variables for executors to use
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    # Setting memory if this setting was not set previously
    .setIfMissing("spark.executor.memory", "1g")
)

# Getting a single variable
print(cfg.get("spark.executor.memory"))
# Listing all of them in string readable format
print(cfg.toDebugString())

session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

sc = session.sparkContext

with open('User/data_file.json', 'r') as data:
    print(data.read())

    result = (
        sc.parallelize(data)
        .filter(lambda val: val % 3 == 0)
    )

    print(result)