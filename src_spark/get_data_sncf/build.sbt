ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"


lazy val root = (project in file("."))
  .settings(
    name := "get_data_sncf",
    idePackagePrefix := Some("org.regone.sncf")
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1" % "compile"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "compile"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.1.0"

// https://mvnrepository.com/artifact/com.databricks/spark-xml
libraryDependencies += "com.databricks" %% "spark-xml" % "0.15.0" % "compile"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1" % "compile"

