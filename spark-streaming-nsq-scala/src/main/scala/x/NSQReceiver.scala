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
import org.apache.spark.sql.SparkSession

class NSQUnreliableReceiver(host: String, port: Int, topic: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  var client:NSQClient = null
  
  def onStart() {
    client = NSQClient(new NSQLookupDefault(List(s"http://$host:$port")))
    val consumer = client.consumer(topic=this.topic) { msg =>
      try {   
//          if (log.isDebugEnabled()) log.debug("received: {}", msg)
          log.warn("received: {}", msg)
          store(new String(msg.data))
          msg.fin()
      } catch {
        case t:Throwable => restart("error storing", t)
      }
    }
  }

  def onStop() {
    client.close()
  }
}

object VervathonTest extends App {{ // double braces fixes NPE https://issues.apache.org/jira/browse/SPARK-4170
  val sc = new SparkContext(new SparkConf())
  
//  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(sc, Seconds(10))
  
  // Create a DStream that will connect to hostname:port, like localhost:9999
  val lines = ssc.receiverStream(new NSQUnreliableReceiver("10.47.149.180", 4161, "test-ss"))

  // Split each line into words
  val words = lines.flatMap(_.split(" "))
  
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)
  
  // Print the first ten elements of each RDD generated in this DStream to the console
  wordCounts.print()  
  
  val spark = SparkSession.builder().getOrCreate()
  wordCounts.foreachRDD { rdd =>
//    val df = spark.read.json(rdd)
    val df = spark.createDataFrame(rdd)
    
//    val dateWithHour = format.print(DateTime.now)
    val outputDirectory = s"s3://verve-home/scottstewart/stream-parquet/blah"
    df.write.mode("append").parquet(outputDirectory)
//    df.write.mode("append").avro(outputDirectory)
  }  
  
  ssc.start()             // Start the computation
  ssc.stop(stopSparkContext=false)
//  ssc.awaitTermination()  // Wait for the computation to terminate
  
}}

object RTBParquetLogger extends App {{
  import com.typesafe.config.ConfigFactory
  import java.nio.file.{Paths, Files}
  import java.nio.charset.StandardCharsets
  import java.util.UUID
  import com.databricks.spark.avro._
  import org.apache.spark.sql._
  import com.github.nscala_time.time.Imports._
  
  val config = ConfigFactory.load()
  println(config)

  val host = config.getString("nsq.host")
  val port = config.getInt("nsq.port")
  val topic = config.getString("nsq.topic")
  val interval = config.getLong("batch.inverval.seconds")
  val outputDirectoryRoot = config.getString("output.directory")

  val format = DateTimeFormat.forPattern("yyyy-MM-dd/HH").withZone(DateTimeZone.UTC)

  val sc = new SparkContext(new SparkConf())
  val ssc = new StreamingContext(sc, Seconds(interval))
  val spark = SparkSession.builder().getOrCreate()

  val stream = ssc.receiverStream(new NSQUnreliableReceiver(host, port, topic))

  stream.foreachRDD { rdd =>
    val df = spark.read.json(rdd)
    val dateWithHour = format.print(DateTime.now)
    val outputDirectory = s"$outputDirectoryRoot/$dateWithHour"
    df.write.mode("append").parquet(outputDirectory)
//    df.write.mode("append").avro(outputDirectory)
  }

  ssc.start()
  ssc.awaitTermination()
}}
