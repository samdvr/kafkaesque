name := "Kafkaesque"

version := "1.0"

scalaVersion := "2.12.2"
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % "0.10.1.0",
  "com.typesafe" % "config" % "1.3.1"
)