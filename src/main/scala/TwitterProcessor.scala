import java.util.{Date, UUID}

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}
import org.apache.spark.streaming.kafka.KafkaUtils


object TwitterProcessor {

  var batch_interval_in_seconds = 10
  var window_in_minutes = 60
  var slide_in_seconds = 10
  var alertID: UUID = _
  var requiredKeys: Array[String] = _
  var niceToHaveKeys: Array[String] = _

  def main(args: Array[String]) {
    val Array(kafkaBroker, group, topics, numThreads, cassandraHost) = Array(sys.env("kafkaBroker"), sys.env("group"), sys.env("topics"), sys.env("numThreads"), sys.env("cassandraHost"))
    val ssc: StreamingContext = createSparkStreamingContext(cassandraHost)

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, kafkaBroker, group, topicMap).map(_._1)
    val countStream = getCountStreamOfTweets(lines)
    saveToDb(countStream)

    ssc.start()
    ssc.awaitTermination()
  }

  def createSparkStreamingContext(cassandraHost: String): StreamingContext = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("data-processor: ")
    conf.set("spark.cassandra.connection.host", cassandraHost)

    val ssc = new StreamingContext(conf, Seconds(batch_interval_in_seconds))
    ssc.checkpoint("checkpoint")
    ssc
  }

  def countByWindow(matchedTweets: DStream[String], windowDuration: Long, slideDuration: Long): DStream[Long] = {
    matchedTweets.countByWindow(Seconds(windowDuration), Seconds(slideDuration))
  }

  def getCountStreamOfTweets(tweets: DStream[(String)]): DStream[(String, Int)] = {
    tweets.map((_, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Minutes(window_in_minutes), Seconds(slide_in_seconds))
  }

  def saveToDb(countStream: DStream[(String, Int)]) = {
    val toDb = countStream.map { case (alertId: String, count: Int) =>
      (UUIDs.timeBased(), UUID.fromString(alertId), count, new Date())
    }
    toDb.saveToCassandra("trends", "trend", SomeColumns("id", "alert_id", "count", "process_date"))
  }
}

