import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ClockWrapper, Duration, Seconds, StreamingContext}
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class TwitterProcessorSpec extends FlatSpec with BeforeAndAfter with GivenWhenThen with Matchers with Eventually {

  private val master = "local[*]"
  private val appName = "test-spark-streaming"

  var batch_interval_in_seconds = 1
  var window_in_seconds = 5
  var slide_in_seconds = 1
  var patienceConfigMillis = 10000

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _

  // default timeout for eventually trait
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(patienceConfigMillis, Millis)))

  case class AlertTrend(alertId: UUID, count: Int)

  before {
    ssc = new StreamingContext(TwitterProcessor.createSparkConf().set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock"), Seconds(batch_interval_in_seconds))
    ssc.checkpoint("checkPoint")
    sc = ssc.sparkContext
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
  }

  "TwitterProcess" should "group tweets by alert id and count" in {
    Given("sample alerts")

    val firstAlertId = "1197e038-3722-474c-a417-2fe6bdccc6a5"
    val secondAlertId = "8a8c2874-bf38-4d10-ad9b-faf089d865ab"
    val thirdAlertId = "566208ac-2a61-4d37-b176-eeaf0e46d7cd"

    val lines = mutable.Queue[RDD[String]]()
    val dstream = ssc.queueStream(lines)
    var results = new ListBuffer[List[(String, Long)]]

    val alertTrendList = TwitterProcessor.getCountStreamOfTweets(dstream)

    alertTrendList.foreachRDD(rdd => {
      results += rdd.collect().toList
    })

    ssc.start()

    When("time passes")
    lines += sc.makeRDD(List(firstAlertId.toString, firstAlertId.toString, secondAlertId.toString, thirdAlertId.toString, firstAlertId.toString))
    ClockWrapper.advance(ssc, Duration(patienceConfigMillis))

    Then("list contains tweets")
    val expectedList = List((firstAlertId, 3), (secondAlertId, 1), (thirdAlertId, 1))
    eventually {
      results.toList.take(1) should equal(List(expectedList))
    }
  }

}