package x

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets
import org.apache.spark.internal.Logging
import com.github.mitallast.nsq._
import com.typesafe.config.ConfigFactory
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import java.util.UUID
import com.databricks.spark.avro._
import org.apache.spark.sql._

class CustomReceiver(host: String, port: Int, topic: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  import scala.concurrent.duration._

  var client: NSQClient = null

  def onStart() {
    client = NSQClient(new NSQLookupDefault(List(s"http://$host:$port")))
    val consumer = client.consumer(topic=topic) { msg =>
        log.error("received: {}", msg)
        store(new String(msg.data))
        msg.fin()
    }
  }

  def onStop() {
    //client.close()
  }
}


object RTBAvroLogger extends App {{
  val config = ConfigFactory.load()
  println(config)

  val host = config.getString("nsq.host")
  val port = config.getInt("nsq.port")
  val topic = config.getString("nsq.topic")
  val interval = config.getLong("batch.inverval.seconds")
  val outputDirectory = config.getString("output.directory")

  val sc = new SparkContext(new SparkConf())
  val ssc = new StreamingContext(sc, Seconds(interval))
  val spark = SparkSession.builder().getOrCreate()

  val stream = ssc.receiverStream(new CustomReceiver(host, port, topic))
  //val messages = stream.flatMap(_.split("\n"))

  stream.foreachRDD { rdd =>
    val df = spark.read.json(rdd)
    df.write.mode("append").avro(outputDirectory)
  }

  ssc.start()
  ssc.awaitTermination()
}}
