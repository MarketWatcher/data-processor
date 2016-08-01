
import com.datastax.driver.core.utils.UUIDs
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class TwitterServiceSpec extends FlatSpec with MockFactory with BeforeAndAfter with Matchers {

  val sampleAlertId = UUIDs.timeBased()

  "TrendService.getTrend" should "return a trend with correct parameters" in {
    val trendService = TrendService
    val trend = trendService.getTrend(sampleAlertId)

    trend.alertId shouldBe sampleAlertId
    trend.date shouldBe "12313123"
    trend.count shouldBe 10
    trend.status shouldBe TrendStatus.Unknown
  }

}