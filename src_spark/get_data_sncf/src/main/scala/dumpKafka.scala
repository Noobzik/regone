package org.regone.sncf

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger._
import org.apache.spark.sql.types._
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder}

import java.util.Properties
import java.io.InputStream

object dumpKafka {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession  = SparkSession.builder.master("local").appName("test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rer-b-computed")
      .option("startingOffsetsByTimestamp", "{\"rer-b-computed\":{\"0\": 1656492880733}}") // From starting
      .load()

    val schema = new StructType()
      .add("gare",IntegerType)
      .add("date",StringType)
      .add("mode",StringType)
      .add("num",StringType)
      .add("miss",StringType)
      .add("direction",IntegerType)
      .add("term",StringType)
      .add("time_recorded",StringType)
      .add("time_arrived", StringType)
      .add("etat", StringType)

    val personStringDF = df.selectExpr("CAST(value AS STRING)")
    val personDF = personStringDF.select(from_json(col("value"), schema).as("data")) .select("data.*")

    println("Test")
    personStringDF.writeStream
      .format("console")
//     .trigger(ProcessingTime("10 seconds"))
//     .option("checkpointLocation", "checkpoint/")
//      .option("path", "./output_path/")
      .outputMode("append")
      .start()
      .awaitTermination()


    spark.close()
  }
  val props = buildProperties
  val builder: StreamsBuilder = new StreamsBuilder

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
    val applicationName = s"RER-B-Compute"
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    // Disable caching to print the aggregation value after each record
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.load(inputStream)
    properties
  }
}
