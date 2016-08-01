package controllers

import java.util.{Date, UUID}

import javax.inject._
import play.api._
import play.api.mvc._
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs
import play.api.libs.json.Json

case class Alert(id: Int, name: String)

@Singleton
class ServiceController @Inject() extends Controller {

  var cluster = Cluster.builder()
    .addContactPoint(System.getenv("CASSANDRA_NODES"))
    .withPort(9042)
    .build()

  var session = cluster.connect()

  initDatabase()

  def index = Action {
    Ok("Trend Service")
  }

  def trends(alertId: String) = Action {
    val rows = session.execute("SELECT * FROM trends.trend where alert_id = " + UUID.fromString(alertId));
    val count = rows.one().getInt("count")
    Ok(Json.toJson(count))
  }

  def initDatabase(): Unit = {
    scala.io.Source
      .fromFile("init.cql", "utf-8")
      .getLines
      .mkString
      .split(';')
      .filter(l => l.trim.length() > 0)
      .foreach(l => {
        System.out.println("LINE: " + l)
        session.execute(l)
      })
  }

}
