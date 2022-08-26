package org.broadinstitute.dsde.rawls.google

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.testkit.TestKit
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo
import com.google.api.client.googleapis.json.{GoogleJsonError, GoogleJsonResponseException}
import com.google.api.client.http._
import org.broadinstitute.dsde.rawls.metrics.StatsDTestUtils
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import spray.json._

import java.io.IOException
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class GoogleUtilitiesSpec
    extends TestKit(ActorSystem("MySpec"))
    with GoogleUtilities
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with Eventually
    with MockitoTestUtils
    with StatsDTestUtils {
  implicit val executionContext = ExecutionContext.global
  implicit def histo = ExpandedMetricBuilder.empty.asHistogram("histo")
  implicit override val patienceConfig = PatienceConfig(scaled(Span(1000, Millis)), scaled(Span(15, Millis)))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  // a total of 4 attempts (include the first one that has no delay)
  override def exponentialBackOffIntervals = Seq(10 milliseconds, 20 milliseconds, 40 milliseconds)

  def buildHttpResponseException(statusCode: Int): HttpResponseException =
    new HttpResponseException.Builder(statusCode, null, new HttpHeaders()).build

  def buildGoogleJsonResponseException(statusCode: Int,
                                       message: Option[String] = None,
                                       reason: Option[String] = None,
                                       domain: Option[String] = None
  ): GoogleJsonResponseException = {
    val httpExc = new HttpResponseException.Builder(statusCode, null, new HttpHeaders())
    val errInfo = new ErrorInfo()

    message foreach httpExc.setMessage
    reason foreach errInfo.setReason
    domain foreach errInfo.setDomain

    val gjError = new GoogleJsonError()
    gjError.setErrors(Seq(errInfo).asJava)
    new GoogleJsonResponseException(httpExc, gjError)
  }

  class Counter() {
    var counter = 0

    def alwaysBoom(): Int = {
      counter += 1
      throw new IOException("alwaysBoom")
    }

    def boomOnce(): Int = {
      counter += 1
      if (counter > 1) {
        42
      } else {
        throw new IOException("boomOnce")
      }
    }

    def httpBoom(): Int = {
      counter += 1
      throw buildHttpResponseException(503)
    }
  }

  "when500or400orGoogleError" should "return true for 500 or Google errors" in {
    when500or400orGoogleError(buildGoogleJsonResponseException(403)) shouldBe true
    when500or400orGoogleError(buildGoogleJsonResponseException(429)) shouldBe true
    when500or400orGoogleError(buildGoogleJsonResponseException(400)) shouldBe true
    when500or400orGoogleError(buildGoogleJsonResponseException(404)) shouldBe true

    when500or400orGoogleError(buildGoogleJsonResponseException(500)) shouldBe true
    when500or400orGoogleError(buildGoogleJsonResponseException(502)) shouldBe true
    when500or400orGoogleError(buildGoogleJsonResponseException(503)) shouldBe true

    when500or400orGoogleError(buildHttpResponseException(500)) shouldBe true
    when500or400orGoogleError(buildHttpResponseException(502)) shouldBe true
    when500or400orGoogleError(buildHttpResponseException(503)) shouldBe true

    when500or400orGoogleError(new IOException("boom")) shouldBe true
  }

  "when500orNon404GoogleError" should "return true for 500 or Google errors except 404s" in {
    when500orNon404GoogleError(buildGoogleJsonResponseException(400)) shouldBe true
    when500orNon404GoogleError(buildGoogleJsonResponseException(404)) shouldBe false
    when500orNon404GoogleError(buildGoogleJsonResponseException(500)) shouldBe true
    when500orNon404GoogleError(buildHttpResponseException(500)) shouldBe true
    when500orNon404GoogleError(new IOException("boom")) shouldBe true
  }

  "whenStatusDoesntContain" should "return false for the specified Google status codes" in {
    whenGoogleStatusDoesntContain(buildGoogleJsonResponseException(400), Set(StatusCodes.NotFound)) shouldBe true
    whenGoogleStatusDoesntContain(buildGoogleJsonResponseException(404), Set(StatusCodes.NotFound)) shouldBe false
    whenGoogleStatusDoesntContain(buildGoogleJsonResponseException(418),
                                  Set(StatusCodes.NotFound, StatusCodes.ImATeapot)
    ) shouldBe false
    whenGoogleStatusDoesntContain(buildHttpResponseException(500),
                                  Set(StatusCodes.Forbidden, StatusCodes.EnhanceYourCalm)
    ) shouldBe true
    whenGoogleStatusDoesntContain(new IOException("boom"),
                                  Set(StatusCodes.PreconditionFailed, StatusCodes.NotImplemented)
    ) shouldBe true
  }

  "when500orGoogleError" should "return true for 500 or Google errors" in {
    when500orGoogleError(buildGoogleJsonResponseException(403)) shouldBe false
    when500orGoogleError(buildGoogleJsonResponseException(429)) shouldBe false
    when500orGoogleError(buildGoogleJsonResponseException(400)) shouldBe false
    when500orGoogleError(buildGoogleJsonResponseException(404)) shouldBe false

    when500orGoogleError(buildGoogleJsonResponseException(500)) shouldBe true
    when500orGoogleError(buildGoogleJsonResponseException(502)) shouldBe true
    when500orGoogleError(buildGoogleJsonResponseException(503)) shouldBe true

    when500orGoogleError(buildHttpResponseException(500)) shouldBe true
    when500orGoogleError(buildHttpResponseException(502)) shouldBe true
    when500orGoogleError(buildHttpResponseException(503)) shouldBe true

    when500orGoogleError(new IOException("boom")) shouldBe true
  }

  "retryWhen500orGoogleError" should "retry once per backoff interval and then fail" in {
    withStatsD {
      val counter = new Counter()
      whenReady(retryWhen500orGoogleError(() => counter.alwaysBoom()).failed) { f =>
        f shouldBe a[IOException]
        counter.counter shouldBe 4 // extra one for the first attempt
      }
    } { capturedMetrics =>
      capturedMetrics should contain("test.histo.samples", "1")
      capturedMetrics should contain("test.histo.max", "4") // 4 exceptions
    }
  }

  it should "not retry after a success" in {
    withStatsD {
      val counter = new Counter()
      whenReady(retryWhen500orGoogleError(() => counter.boomOnce())) { s =>
        s shouldBe 42
        counter.counter shouldBe 2
      }
    } { capturedMetrics =>
      capturedMetrics should contain("test.histo.samples", "1")
      capturedMetrics should contain("test.histo.max", "1") // 1 exception
    }
  }

  "retryWithRecoverWhen500orGoogleError" should "stop retrying if it recovers" in {
    withStatsD {
      val counter = new Counter()

      def recoverIO: PartialFunction[Throwable, Int] = { case _: IOException =>
        42
      }

      whenReady(retryWithRecoverWhen500orGoogleError(() => counter.alwaysBoom())(recoverIO)) { s =>
        s shouldBe 42
        counter.counter shouldBe 1
      }
    } { capturedMetrics =>
      capturedMetrics should contain("test.histo.samples", "1")
      capturedMetrics should contain("test.histo.max", "0") // 0 exceptions
    }
  }

  it should "keep retrying and fail if it doesn't recover" in {
    withStatsD {
      val counter = new Counter()

      def recoverHttp: PartialFunction[Throwable, Int] = {
        case h: HttpResponseException if h.getStatusCode == 404 => 42
      }

      whenReady(retryWithRecoverWhen500orGoogleError(() => counter.httpBoom())(recoverHttp).failed) { f =>
        f shouldBe a[HttpResponseException]
        counter.counter shouldBe 4 // extra one for the first attempt
      }
    } { capturedMetrics =>
      capturedMetrics should contain("test.histo.samples", "1")
      capturedMetrics should contain("test.histo.max", "4") // 4 exceptions
    }
  }
}

class GoogleJsonSpec extends AnyFlatSpecLike with Matchers {
  "GoogleRequest" should "roundtrip json" in {
    import GoogleRequestJsonSupport._
    val gooRq = GoogleRequest("GET", "www.thegoogle.hooray", Some(JsString("you did a search")), 400, Some(200), None)

    val rqJson = gooRq.toJson
    val rqRead = rqJson.convertTo[GoogleRequest]

    rqRead shouldBe gooRq
  }
}
