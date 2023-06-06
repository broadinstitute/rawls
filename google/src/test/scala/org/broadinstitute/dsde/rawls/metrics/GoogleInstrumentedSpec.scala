package org.broadinstitute.dsde.rawls.metrics

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer, HttpResponseException}
import com.google.api.client.json.gson.GsonFactory
import com.google.api.client.testing.http.{HttpTesting, MockHttpTransport}
import com.google.api.services.storage.Storage
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by rtitle on 8/14/17.
  */
class GoogleInstrumentedSpec
    extends GoogleInstrumented
    with AnyFlatSpecLike
    with Matchers
    with Eventually
    with MockitoTestUtils
    with StatsDTestUtils {
  override val workbenchMetricBaseName = "test"

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val mockTransport = new MockHttpTransport()
  val jsonFactory = GsonFactory.getDefaultInstance
  val credential = new HttpRequestInitializer {
    override def initialize(request: HttpRequest): Unit = ()
  }

  def googleRequest =
    new Storage.Builder(httpTransport, jsonFactory, credential)
      .setApplicationName("test")
      .build()
      .buckets()
      .get("aBucket")

  def googleResponse =
    mockTransport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL).execute()

  def googleResponseException =
    try {
      googleRequest.execute()
      new IOException
    } catch {
      case exception: HttpResponseException => exception // return the exception
    }

  "GoogleInstrumented" should "get counters from a request/response" in {
    GoogleInstrumentedService.values.foreach { implicit service =>
      withStatsD {
        val counters = googleCounters
        val (counter, timer) = counters(googleRequest, Right(googleResponse))
        counter += 1
        timer.update(5 nanos)
        counter.count shouldBe 1
        timer.count shouldBe 1
        timer.max shouldBe 5
      } { capturedMetrics =>
        capturedMetrics should contain(
          s"test.googleService.$service.httpRequestMethod.get.httpResponseStatusCode.200.request",
          "1"
        )
        capturedMetrics should contain(
          s"test.googleService.$service.httpRequestMethod.get.httpResponseStatusCode.200.latency.samples",
          "1"
        )
      }
    }
  }

  it should "get counters from a request/HttpResponseException" in {
    GoogleInstrumentedService.values.foreach { implicit service =>
      withStatsD {
        val counters = googleCounters
        val (counter, timer) = counters(googleRequest, Left(googleResponseException))
        counter += 1
        timer.update(5 nanos)
        counter.count shouldBe 1
        timer.count shouldBe 1
        timer.max shouldBe 5
      } { capturedMetrics =>
        capturedMetrics should contain(
          s"test.googleService.$service.httpRequestMethod.get.httpResponseStatusCode.400.request",
          "1"
        )
        capturedMetrics should contain(
          s"test.googleService.$service.httpRequestMethod.get.httpResponseStatusCode.400.latency.samples",
          "1"
        )
      }
    }
  }

  it should "get counters from a request/exception" in {
    GoogleInstrumentedService.values.foreach { implicit service =>
      withStatsD {
        val counters = googleCounters
        val (counter, timer) = counters(googleRequest, Left(new IOException))
        counter += 1
        timer.update(5 nanos)
        counter.count shouldBe 1
        timer.count shouldBe 1
        timer.max shouldBe 5
      } { capturedMetrics =>
        // no status code
        capturedMetrics should contain(s"test.googleService.$service.httpRequestMethod.get.request", "1")
        capturedMetrics should contain(s"test.googleService.$service.httpRequestMethod.get.latency.samples", "1")
      }
    }
  }

}
