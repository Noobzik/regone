package org.regone.sncf

import com.databricks.spark.xml.XmlDataFrameReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


import scala.io.Source
import java.net.URL

object Clean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("testing")
      .getOrCreate()

    val baseUrl: String = "https://api.transilien.com/gare/"
    val gare : String = "87271445"
    val url : String = baseUrl + gare + "/depart"
    val headers = Map(
      "Authorization" -> "Basic "
    )


    val connectionProchainDepart = new URL(url).openConnection
    headers.foreach({
      case (name, value) => connectionProchainDepart.setRequestProperty(name, value)
    })

    val results = Source.fromInputStream(connectionProchainDepart.getInputStream).getLines.mkString("\n")

    val tag : String = "passage game=\""+gare+"\""
    import spark.implicits._
    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rootTag", tag)
      .option("rowTag", "train")
      .xml(Seq(results).toDS())

    val df2 = df.select(explode($"train").as("train"))

    val df3 = df2.select($"train.miss", $"train.num", $"train.term", $"train.date._VALUE".as("Départ"), $"train.date._mode".as("Type_horraire"), $"train.etat")
    val df4 = df3
      .filter($"num" rlike "^[^0-9]")
      .withColumn("Direction", substring($"num", -1, 1) % 2)

    df4.show(5, false)
    df4
      .select(to_json(struct("*")).as("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rer_b_"+gare)
      .option("includeHeaders", true)
      .save()

    spark.close()
  }

}
