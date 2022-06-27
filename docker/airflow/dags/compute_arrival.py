import logging
import os
import warnings
import pyspark
from pyspark.sql import *
import boto3
import pendulum
import zipfile
import pandas as pd
from pathlib import Path
from urllib.request import urlretrieve
from airflow.decorators import dag, task
from airflow.models import Variable
from kafka import KafkaConsumer, TopicPartition
from kafka import KafkaProducer
from kafka.consumer.fetcher import log
import json
from io import StringIO

from botocore.exceptions import ClientError
from pyspark.sql.functions import lit

DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier
TRANSILIEN_TOKEN = Variable.get("TRANSILIEN_KEY")
AWS_REGION = Variable.get("AWS_REGION")
AWS_ACCESS_ID = Variable.get("AWS_ACCESS_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
BUCKET = "sncf-rer-b"
BROKER = Variable.get("BROKER")

default_args = {
    'owner': 'noobzik',
    'retries': 0,
    'retry_delay': pendulum.duration(minutes=30)
}


@dag(DAG_NAME, default_args=default_args, schedule_interval="*/1 5-23,0,1 * * *",
     start_date=pendulum.today('Europe/Paris').add(days=-1), catchup=False)
def compute_arrival():
    """
    Ce DAG est permet de récupérer les prochains départs de l'ensemble de la partie SNCF du RER B
    """

    def get_end_offsets(consumer, topic) -> dict:
        partitions_for_topic = consumer.partitions_for_topic(topic)
        if partitions_for_topic:
            partitions = []
            for partition in consumer.partitions_for_topic(topic):
                partitions.append(TopicPartition(topic, partition))
            # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.end_offsets
            # Get the last offset for the given partitions. The last offset of a partition is the offset of the upcoming message, i.e. the offset of the last available message + 1.
            end_offsets = consumer.end_offsets(partitions)
            return end_offsets


    def grab_topic_gare(gare: str = "87271460", num: int = 2):
        last_n_msg = num
        # kafka_server = "35.180.29.24:9092"
        kafka_server = BROKER
        # consumer
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_server,
            consumer_timeout_ms=10000)
        end_offsets = get_end_offsets(consumer, 'rer-b-' + gare)
        consumer.assign([*end_offsets])
        for key_partition, value_end_offset in end_offsets.items():
            new_calculated_offset = value_end_offset - last_n_msg
            new_offset = new_calculated_offset if new_calculated_offset >= 0 else 0
            consumer.seek(key_partition, new_offset)

        for msg in consumer:
            return msg

    def send_to_kafka(data: dict):
        """
        Méthode qui va envoyer vers un kafka le résultat du prétraitement pour pouvoir
        être consommé par les applications
        """
        topic = 'rer-b-computed'
        producer = KafkaProducer(bootstrap_servers="localhost:9092")
        producer.send(topic, value=(json.dumps(data, ensure_ascii=False).encode('utf-8')))
        producer.flush()
        print("All data sent to kafka")

    gares = [
        "87001479",  # Charles de Gaulles 2
        "87271460",  # Charles de Gaulles 1
        "87271486",  # Parc des expositions
        "87271452",  # Villepinte
        "87271445",  # Sevran Beaudottes

        "87271528",  # Mitry Clay
        "87271510",  # Villeparisis Mitry-le-Neuf
        "87271437",  # Vert Galant
        "87271429",  # Sevran Livry

        "87271411",  # Aulnay Sous bois
        "87271478",  # Le Blanc Mesnil
        "87271403",  # Drancy
        "87271395",  # Le Bourget
        "87271304",  # La Courneuve - Aubervilliers
        "87164798",  # La Plaine Stade-de-France
        "87271007"  # Paris Gare-du-Nord
    ]
    spark = SparkSession.builder \
        .appName("Compute-RER-B") \
        .getOrCreate()

    for i in gares:
        content_1 = grab_topic_gare(i, 1)
        content_2 = grab_topic_gare(i, 2)

        df_previous = spark.read.json(spark.parallelize([json.loads(content_1[6])]))
        df_latest = spark.read.json(spark.parallelize([json.loads(content_2[6])]))

        df_final = df_latest.join(df_previous, df_latest.num == df_previous.num, 'left_anti')
        df_final = df_final \
            .withColumn("gare", lit(int(i))) \
            .withColumn("time_arrived", lit(df_previous.collect()[0][-1]))

        pandas_df = df_final.select("gare", "date", "mode", "num", "miss", "direction", "term", "time_recorded",
                                    "time_arrived", "etat").toPandas()

        di = pandas_df.to_dict(orient="records")
        send_to_kafka(di)

    spark.stop()

dag_projet_instances = compute_arrival()  # Instanciation du DAG

# Pour run:
# airflow dags backfill --start-date 2019-01-02 --end-date 2019-01-03 --reset-dagruns daily_ml
# airflow tasks test aggregate_data_2 2019-01-02
