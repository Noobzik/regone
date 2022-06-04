package org.regone.streaming


import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.regone.streaming.models.nextDepartures

import java.io.InputStream
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = s"rer-b-consumer"

  // Topic definition
  val nextDepartureTopicName: String = "nextDepartures"

  // Store name definition
  val nextDepartureOfStationStoreName: String = "nextDepartureOfStation"
  val meanTravelTimeStationAStationBStoreName: String = "meanTravelTimeStationAStationB"

  val props = buildProperties
  val builder: StreamsBuilder = new StreamsBuilder

  // Source du topic

  val rerB: KStream[String, nextDepartures] = builder.stream[String, nextDepartures](nextDepartureTopicName)

  // Group By Gare

  val rerBGroupedByStation : KGroupedStream[String, nextDepartures] = rerB.map((_, station) => (station.station, station)).groupByKey

  // Compute the travel time (Hardest one)

  /**
   * 1- Get the previous records
   * 2- For each gare of both records:
   * 3- Unmatch Inner join. If train x is not in latest records gare, add it with latest timestamp
   * @return
   */
  /*
   * Two endpoints to develop :
   * 1- next trains at stations
   * 2- mean travel time of the last 5 trains
   */

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run {
        streams.close
      }
    }))
    streams
  }


  // auto loader from properties file in project
  def buildProperties: Properties = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    import org.apache.kafka.streams.StreamsConfig
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("kafka.properties")

    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    // Disable caching to print the aggregation value after each record
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.load(inputStream)
    properties
  }
}
