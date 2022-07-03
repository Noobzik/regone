import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.kafka._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
  }
  /*
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
  */
  def get_end_offsets(consumer: KafkaConsumer[String, String], topic: String) {
    val partitionsList = consumer.partitionsFor(topic)
    val it = Iterator(partitionsList)
    if (partitionsList.size() != 0) {

    }
    //https: //kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.end_offsets
      //Get the last
    return consumer.endOffsets()

  }

  def grab_topic_gare(gare: String = "87271460", num : Int = 2) {

    val last_n_msg: Int = num
    // kafka_server = "35.180.29.24:9092"
    val kafka_server = "13.37.146.224:9092"
    // consumer
    val consumer = KafkaConsumer(
      bootstrap_servers = kafka_server,
      consumer_timeout_ms = 10000)
    end_offsets = get_end_offsets(consumer, "rer-b-" + gare)
    consumer.assign([* end_offsets]
    )
    for key_partition
    , value_end_offset in end_offsets.items():
      new_calculated_offset
    = value_end_offset - last_n_msg
    new_offset = new_calculated_offset
    if new_calculated_offset >= 0 else 0
    consumer.seek(key_partition, new_offset)

    for msg in consumer:
    return msg
  }

    def forgiving_json_deserializer(v) {

    if v is None:
    try
    :
    return json.loads(v.encode('utf - 8
    ') )
    except json
    .decoder.JSONDecodeError:
      log.exception
    ('Unable to decode: % s
    ', v
    )
    return None
  }
}