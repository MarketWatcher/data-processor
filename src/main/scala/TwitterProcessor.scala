import java.util.{Date, UUID}

import _root_.kafka.serializer.StringDecoder
import com.datastax.spark.connector.SomeColumns
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.io.Source
import org.apache.spark._
import twitter4j.Status
import org.apache.spark.streaming._
import com.datastax.spark.connector.streaming._
import org.joda.time.{DateTime, DateTimeZone}
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.{Config, ConfigFactory}
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobValidation, ContextLike}


object TwitterProcessor extends SparkJob {

  var batch_interval_in_seconds = 1
  var window_in_minutes = 60
  var slide_in_seconds = 10
  var alertID: UUID = _
  var requiredKeys: Array[String] = _
  var niceToHaveKeys: Array[String] = _

  def main(args: Array[String]) {
/*
    println("args.length: " + args.length)
    if (args.length < 5) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads> <cassandraHost>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads, cassandraHost) = args*/

   // val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val conf = new SparkConf().setMaster("local[4]").setAppName("data-processor: ")
    conf.set("spark.cassandra.connection.host", "db")

    val sc = new SparkContext(conf)

    val config = ConfigFactory.parseString("")
    val results = runJob(sc, config)


  }


  override def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val topics = "test"

    val topicMap = topics.split(",").map((_, 2)).toMap

    val ssc = new StreamingContext(sc.getConf, Seconds(batch_interval_in_seconds))
    ssc.checkpoint("checkpoint")

    alertID = UUIDs.timeBased()
    val lines = KafkaUtils.createStream(ssc, "localhost:2181", "ayse-group", topicMap).map(_._2)
    val counts = getCountStreamOfTweets(lines)
    counts.saveToCassandra("trends", "alert_trend", SomeColumns("id", "alert_id", "count", "process_date"))

    ssc.start()

    ssc.awaitTermination()

  }


  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid


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