import time

from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from kafka.consumer.fetcher import log
import os
import json

from pyspark.sql import *
from pyspark.sql.functions import lit

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache'


if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "rer-b-computed") \
        .option("startingOffsets", "earliest")  \
        .load()

    df.show()