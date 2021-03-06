package org.regone.streaming


import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{Aggregator, SlidingWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.regone.streaming.models.nextDepartures

import java.io.InputStream
import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = s"rer-b-consumer"

  // Topic Gare definition

  // Store name definition
  val nextDepartureOfStationStoreName: String = "nextDepartureOfStation"
  val meanTravelTimeStationAStationBStoreName: String = "meanTravelTimeStationAStationB"

  // Topic Gare definition
  val rer_b_87001479_TopicName = "rer_b_87001479"
  val rer_b_87271460_TopicName = "rer_b_87271460"
  val rer_b_87271486_TopicName = "rer_b_87271486"
  val rer_b_87271452_TopicName = "rer_b_87271452"
  val rer_b_87271445_TopicName = "rer_b_87271445"
  val rer_b_87271528_TopicName = "rer_b_87271528"
  val rer_b_87271510_TopicName = "rer_b_87271510"
  val rer_b_87271437_TopicName = "rer_b_87271437"
  val rer_b_87271429_TopicName = "rer_b_87271429"
  val rer_b_87271411_TopicName = "rer_b_87271411"
  val rer_b_87271478_TopicName = "rer_b_87271478"
  val rer_b_87271403_TopicName = "rer_b_87271403"
  val rer_b_87271395_TopicName = "rer_b_87271395"
  val rer_b_87271304_TopicName = "rer_b_87271304"
  val rer_b_87164798_TopicName = "rer_b_87164798"
  val rer_b_87271007_TopicName = "rer_b_87271007"

  val rer_b_87001479_StoreName = "rer_b_87001479_StoreName"
  val rer_b_87271460_StoreName = "rer_b_87271460_StoreName"
  val rer_b_87271486_StoreName = "rer_b_87271486_StoreName"
  val rer_b_87271452_StoreName = "rer_b_87271452_StoreName"
  val rer_b_87271445_StoreName = "rer_b_87271445_StoreName"
  val rer_b_87271528_StoreName = "rer_b_87271528_StoreName"
  val rer_b_87271510_StoreName = "rer_b_87271510_StoreName"
  val rer_b_87271437_StoreName = "rer_b_87271437_StoreName"
  val rer_b_87271429_StoreName = "rer_b_87271429_StoreName"
  val rer_b_87271411_StoreName = "rer_b_87271411_StoreName"
  val rer_b_87271478_StoreName = "rer_b_87271478_StoreName"
  val rer_b_87271403_StoreName = "rer_b_87271403_StoreName"
  val rer_b_87271395_StoreName = "rer_b_87271395_StoreName"
  val rer_b_87271304_StoreName = "rer_b_87271304_StoreName"
  val rer_b_87164798_StoreName = "rer_b_87164798_StoreName"
  val rer_b_87271007_StoreName = "rer_b_87271007_StoreName"

  val props = buildProperties
  val builder: StreamsBuilder = new StreamsBuilder

  // Source du topic
  val rer_b_87001479 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87001479_TopicName)
  val rer_b_87271460 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271460_TopicName)
  val rer_b_87271486 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271486_TopicName)
  val rer_b_87271452 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271452_TopicName)
  val rer_b_87271445 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271445_TopicName)
  val rer_b_87271528 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271528_TopicName)
  val rer_b_87271510 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271510_TopicName)
  val rer_b_87271437 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271437_TopicName)
  val rer_b_87271429 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271429_TopicName)
  val rer_b_87271411 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271411_TopicName)
  val rer_b_87271478 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271478_TopicName)
  val rer_b_87271403 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271403_TopicName)
  val rer_b_87271395 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271395_TopicName)
  val rer_b_87271304 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271304_TopicName)
  val rer_b_87164798 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87164798_TopicName)
  val rer_b_87271007 : KStream[String, nextDepartures] = builder.stream[String, nextDepartures](rer_b_87271007_TopicName)


  // Group By Gare
//  val rer_b_87001479_next_departures : KTable[String, nextDepartures] = rer_b_87001479.groupBy((_, word) => word))(Materialized.as(rer_b_87001479_TopicName)
//  val rer_b_87271460_next_departures : KTable[String, nextDepartures] = rer_b_87271460
//  (Materialized.as(rer_b_87271460_StoreName))
//  val rer_b_87271486_next_departures : KTable[String, nextDepartures] = rer_b_87271486
//  (Materialized.as(rer_b_87271486_StoreName))
//  val rer_b_87271452_next_departures : KTable[String, nextDepartures] = rer_b_87271452
//  (Materialized.as(rer_b_87271452_StoreName))
//  val rer_b_87271445_next_departures : KTable[String, nextDepartures] = rer_b_87271445
//  (Materialized.as(rer_b_87271445_StoreName))
//  val rer_b_87271528_next_departures : KTable[String, nextDepartures] = rer_b_87271528
//  (Materialized.as(rer_b_87271528_StoreName))
//  val rer_b_87271510_next_departures : KTable[String, nextDepartures] = rer_b_87271510
//  (Materialized.as(rer_b_87271510_StoreName))
//  val rer_b_87271437_next_departures : KTable[String, nextDepartures] = rer_b_87271437
//  (Materialized.as(rer_b_87271437_StoreName))
//  val rer_b_87271429_next_departures : KTable[String, nextDepartures] = rer_b_87271429
//  (Materialized.as(rer_b_87271429_StoreName))
//  val rer_b_87271411_next_departures : KTable[String, nextDepartures] = rer_b_87271411
//  (Materialized.as(rer_b_87271411_StoreName))
//  val rer_b_87271478_next_departures : KTable[String, nextDepartures] = rer_b_87271478
//  (Materialized.as(rer_b_87271478_StoreName))
//  val rer_b_87271403_next_departures : KTable[String, nextDepartures] = rer_b_87271403
//  (Materialized.as(rer_b_87271403_StoreName))
//  val rer_b_87271395_next_departures : KTable[String, nextDepartures] = rer_b_87271395
//  (Materialized.as(rer_b_87271395_StoreName))
//  val rer_b_87271304_next_departures : KTable[String, nextDepartures] = rer_b_87271304
//  (Materialized.as(rer_b_87271304_StoreName))
//  val rer_b_87164798_next_departures : KTable[String, nextDepartures] = rer_b_87164798
//  (Materialized.as(rer_b_87164798_StoreName))
//  val rer_b_87271007_next_departures : KTable[String, nextDepartures] = rer_b_87271007
//  (Materialized.as(rer_b_87271007_StoreName))

  // Compute the travel time (Hardest one)
  val timeDifference : Duration = Duration.ofMinutes(2)
  val gracePeriod : Duration = Duration.ofSeconds(15)
  val rer_b_87001479_next_departures : TimeWindowedKStream[String, nextDepartures] = rer_b_87001479
    .groupByKey
    .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, gracePeriod))
    .reduce()

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
