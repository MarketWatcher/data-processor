import java.util.{Date, UUID}

import com.datastax.spark.connector.SomeColumns
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.io.Source
import org.apache.spark._
import twitter4j.Status
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.joda.time.{DateTime, DateTimeZone}
import com.datastax.driver.core.utils.UUIDs


object ___TwitterProcessor {

  var process_time = 10
  var alertID: UUID = _
  var requiredKeys: Array[String] = _
  var niceToHaveKeys: Array[String] = _

  def main(args: Array[String]) {

    setupTwitter()

    Logger.getRootLogger.setLevel(Level.ERROR)

    val ssc = setupStreamingContext()

    alertID = UUIDs.timeBased()
    requiredKeys = Array("you")
    niceToHaveKeys = Array("they", "and")

    val tweets = TwitterUtils.createStream(ssc, None, requiredKeys)

    getCountStreamOfFilteredTweets(tweets).saveToCassandra("marketwatcher", "alert_trend", SomeColumns("id", "alert_id", "count", "process_date"))

    ssc.start()
    ssc.awaitTermination()
  }

  def filterForAtLeastOne(tweets: DStream[Status], keys: Array[String]): DStream[String] = {
    val texts = tweets.map(_.getText())
    texts.filter(text => keys.exists(text.contains))
  }

  def setupTwitter() = {
    for (line <- Source.fromInputStream(ClassLoader.getSystemResourceAsStream("twitter.txt.dist")).getLines) {
      val fields = line.split("=")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  def countByWindow(matchedTweets: DStream[String], windowDuration: Long, slideDuration: Long): DStream[Long] = {
    matchedTweets.countByWindow(Seconds(windowDuration), Seconds(slideDuration))
  }

  def setupStreamingContext(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TwitterProcessorApp")
    conf.set("spark.cassandra.connection.host", "marketwatcher.tech")

    val streamingContext = new StreamingContext(conf, Seconds(process_time))

    streamingContext.checkpoint("MWCheckPoint")

    streamingContext
  }

  def getCountStreamOfFilteredTweets(tweets: DStream[Status]): DStream[(UUID, UUID, Long, Date)] = {

    val filteredTweets = filterForAtLeastOne(tweets, niceToHaveKeys)

    val countStream = filteredTweets.countByWindow(Seconds(process_time), Seconds(process_time)).map(count => {
      (UUIDs.timeBased(), alertID, count, new Date())
    })

    countStream
  }
}