package x

import com.databricks.spark.avro._
import com.github.nscala_time.time.Imports._
import com.typesafe.config.ConfigFactory
import com.youzan.nsq.client.{Consumer, ConsumerImplV2, MessageHandler}
import com.youzan.nsq.client.entity._
import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}
import java.util.UUID
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver


class NSQReceiver(host: String, port: Int, topic: String, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
  extends Receiver[NSQMessageWrapper](storageLevel)
  with Logging
  with MessageHandler {

  @transient var consumer: Consumer = _

  def onStart() {
    val config = new NSQConfig()
    config.setLookupAddresses(s"$host:$port")
    config.setConsumerName("NSQReceiver")

    consumer = new ConsumerImplV2(config, this)
    consumer.setAutoFinish(false);
    consumer.subscribe(topic)
    consumer.start()
  }

  def onStop() {
    consumer.close()
  }

  def process(message: NSQMessage) {
    store(new NSQMessageWrapper(message))
  }

  def finish(message: NSQMessage) {
    consumer.finish(message)
  }
}

object RTBStreamLogger extends App {{
  val config = ConfigFactory.load()
  println(config)

  val host = config.getString("nsq.host")
  val port = config.getInt("nsq.port")
  val topic = config.getString("nsq.topic")
  val interval = config.getLong("batch.inverval.seconds")
  val outputDirectory = config.getString("output.directory")

  val format = DateTimeFormat.forPattern("yyyy-MM-dd/HH").withZone(DateTimeZone.UTC)

  val sc = new SparkContext(new SparkConf())
  val ssc = new StreamingContext(sc, Seconds(interval))
  val spark = SparkSession.builder().getOrCreate()

  val receiver = new NSQReceiver(host, port, topic)
  val stream = ssc.receiverStream(receiver)

  stream.foreachRDD { rdd =>
    val data = rdd.map(msg => new String(msg.getMessage.getMessageBody))

    val df = spark.read.json(data)
    val dateWithHour = format.print(DateTime.now)
    val outputLocation = s"$outputDirectory/$dateWithHour"

    df.write.mode("append").parquet(outputLocation)

    rdd.foreach(msg => receiver.finish(msg.getMessage))
  }

  ssc.start()
  ssc.awaitTermination()
}}
