from kafka import KafkaConsumer, TopicPartition
import json


def lambda_handler(event, context):
    """
    Cette fonction récupère auprès du broker Kafka, la liste des prochains départs de la gare demandé
    Le restultat est restitué dans un objet JSON auprès de celui qui a appelé cette fonction lambda
    """
    gare = event['queryStringParameters']["gare"]
    transaction_response = grab_next_departure(gare)
    response_object = {}
    response_object['statusCode'] = 200
    response_object['headers'] = {}
    response_object['headers']['Content-Type'] = 'application/json'
    response_object['headers']['Access-Control- Allow-Origin'] = '*'
    response_object['body'] = json.dumps(transaction_response)
    return response_object


def get_end_offsets(consumer, topic) -> dict:
    """
    Cette méthode récuèpre le dernier record reçu dans le topic demandé
    """
    partitions_for_topic = consumer.partitions_for_topic(topic)
    if partitions_for_topic:
        partitions = []
        for partition in consumer.partitions_for_topic(topic):
            partitions.append(TopicPartition(topic, partition))
        # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.end_offsets
        # Get the last offset for the given partitions. The last offset of a partition is the offset of the upcoming message, i.e. the offset of the last available message + 1.
        end_offsets = consumer.end_offsets(partitions)
        return end_offsets


def grab_topic_gare(gare: str = "87271460", num : int = 1):
    """
    Cette méthode récupère à partir d'une gare, le nombre n de records d'un topic Kafka
    @todo : Affecter l'addresse IP Local du Kafka
    """
    last_n_msg = num
    kafka_server = "172.31.40.44:19092"
    # consumer
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_server,
        consumer_timeout_ms=10000)
    end_offsets = get_end_offsets(consumer, gare)
    consumer.assign([*end_offsets])
    for key_partition, value_end_offset in end_offsets.items():
        new_calculated_offset = value_end_offset - last_n_msg
        new_offset = new_calculated_offset if new_calculated_offset >= 0 else 0
        consumer.seek(key_partition, new_offset)
    for msg in consumer:
        return msg


def grab_next_departure(gare: str):
    """
    Cette méthode retourne le résultat du record kafka.
    Le résultat json se trouve à la position 6 du tableau
    """
    return json.loads(grab_topic_gare(gare)[6])
