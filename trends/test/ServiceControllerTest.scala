import org.scalatestplus.play._
import play.api.test._
import play.api.test.Helpers._

class ServiceControllerTest extends PlaySpec with OneAppPerTest {

  "Routes" should {

    "send 404 on a bad request" in  {
      route(app, FakeRequest(GET, "/nothing")).map(status(_)) mustBe Some(NOT_FOUND)
    }

  }

  "ServiceController" should {

    "render the index page" in {
      val home = route(app, FakeRequest(GET, "/")).get

      status(home) mustBe OK
      contentType(home) mustBe Some("text/plain")
      contentAsString(home) must include ("Hellooooo")
    }

  }

  "ServiceController" should {

    "return a trend count" in {
      contentAsString(route(app, FakeRequest(GET, "/trends/50554d6e-29bb-11e5-b345-feff819cdc9f")).get) mustBe "678"
    }

  }

}
