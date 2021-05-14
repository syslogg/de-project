import os

## Download Jars for Spark

os.environ['PYSPARK_SUBMIT_ARGS'] = ' --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 pyspark-shell'


import findspark
findspark.init()
findspark.find()

import pyspark
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, desc, upper
from pyspark.sql.types import *


# Initialize Spark Session

spark = SparkSession.builder \
        .appName('Spark Structured Streaming') \
        .getOrCreate()

# Subscribe to Kafka topic

df = spark.readStream \
          .format('kafka') \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("subscribe", "twitter") \
          .load()

jsonSchema = StructType([StructField("created_at", DoubleType(), True), 
                         StructField("hashtags", ArrayType(StructType([
                            StructField("text", StringType(), True), 
                             StructField("indices", 
                                         ArrayType(IntegerType(), True))])), True),
                         StructField("favorite_count", DoubleType(), True), 
                         StructField("retweet_count", DoubleType(), True),
                         StructField("text", StringType(), True), 
                         StructField("id", StringType(), True),
                         StructField("geo", StructType([
                             StructField("type", StringType(), True), 
                             StructField("coordinates", 
                                         ArrayType(LongType(), True))]), True), 
                         StructField("lang", StringType(), True)])

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
       .withColumn("json", from_json(col('value').cast("string"), jsonSchema))

# Count by Language
lang_count = df.groupBy("json.lang") \
            .count() \
            .sort(desc("count")) \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .start() \
            .awaitTermination()


#lang_count.stop()