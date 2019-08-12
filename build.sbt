name := "spark-trade"

version := "0.1"

// Latest spark 2.4.3 supports scala 2.12
scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  // Scala logging
  //"ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "com.typesafe" % "config" % "1.3.4"
)

// Spark related
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.3"
libraryDependencies += "org.apache.spark" %% s"spark-hive" % "2.4.3" //% "provided"
libraryDependencies += "org.apache.spark" %% s"spark-mllib" % "2.4.3" //% "provided"
