import java.util.{Date, UUID}

import _root_.kafka.serializer.StringDecoder
import com.datastax.spark.connector.SomeColumns
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{InputDStream, DStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.io.Source
import org.apache.spark._
import twitter4j.Status
import org.apache.spark.streaming._
import com.datastax.spark.connector.streaming._
import org.joda.time.{DateTime, DateTimeZone}
import com.datastax.driver.core.utils.UUIDs


object TwitterProcessor {

  var batch_interval_in_seconds = 1
  var window_in_minutes = 60
  var slide_in_seconds = 30
  var alertID: UUID = _
  var requiredKeys: Array[String] = _
  var niceToHaveKeys: Array[String] = _

  val cassandraHost: String = "marketwatcher.tech"

  def main(args: Array[String]) {

    println("args.length: " + args.length)
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args

    val conf = new SparkConf().setMaster("local[*]").setAppName("data-processor: ")
    conf.set("spark.cassandra.connection.host", cassandraHost)
    val ssc = new StreamingContext(conf, Seconds(batch_interval_in_seconds))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    lines.print()

    ssc.start()

    ssc.awaitTermination()

    alertID = UUIDs.timeBased()

    //getCountStreamOfTweets(lines).saveToCassandra("marketwatcher", "alert_trend", SomeColumns("id", "alert_id", "count", "process_date"))

  }

  def countByWindow(matchedTweets: DStream[String], windowDuration: Long, slideDuration: Long): DStream[Long] = {
    matchedTweets.countByWindow(Seconds(windowDuration), Seconds(slideDuration))
  }

  def setupStreamingContext(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TwitterProcessorApp")
    conf.set("spark.cassandra.connection.host", "marketwatcher.tech")

    val streamingContext = new StreamingContext(conf, Seconds(batch_interval_in_seconds))

    streamingContext.checkpoint("MWCheckPoint")

    streamingContext
  }

  def getCountStreamOfTweets(tweets: DStream[String]): DStream[(UUID, UUID, Long, Date)] = {

    val countStream = tweets.countByWindow(Minutes(window_in_minutes), Seconds(slide_in_seconds)).map(count => {
      (UUIDs.timeBased(), alertID, count, new Date())
    })

    countStream
  }
}