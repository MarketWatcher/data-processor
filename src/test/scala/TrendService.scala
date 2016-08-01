import java.util.UUID

case class Trend(alertId: UUID, date: String, count: Int, status: TrendStatus.Value)

object TrendService {
  def getTrend(alertId: UUID) = {
    Trend(alertId, "12313123", 10, TrendStatus.Unknown)
  }
}

object TrendStatus extends Enumeration {
  val Unknown = Value("Unknown")
  val Stable = Value("Stable")
  val Rise = Value("Rise")
  val Fall = Value("Fall")

}
