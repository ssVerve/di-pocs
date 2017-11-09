organization := "x"

name := "sparkStreamingNsq"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.hadoop" % "hadoop-client" % "2.7.2" % "provided",
	"org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
	"org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided",
	"org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided",
	"com.github.mitallast" % "scala-nsq_2.11" % "1.11",
	"com.typesafe" % "config" % "1.3.1",
	"com.databricks" % "spark-avro_2.11" % "4.0.0"
)

resolvers ++= Seq(
  "java m2" at "http://download.java.net/maven/2"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
