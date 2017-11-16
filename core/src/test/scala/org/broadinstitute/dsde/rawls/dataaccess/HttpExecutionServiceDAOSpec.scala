package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.implicits._
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, Subsystems, UserInfo}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import spray.http.OAuth2BearerToken
import spray.json.JsObject

import scala.concurrent.duration._

/**
  * Created by rtitle on 8/17/17.
  */
class HttpExecutionServiceDAOSpec extends TestKit(ActorSystem("HttpExecutionServiceDAOSpec"))
  with FlatSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with Eventually with MockitoTestUtils with RawlsStatsDTestUtils {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val mockServer = RemoteServicesMockServer()
  val userInfo = UserInfo(RawlsUserEmail("owner-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212345"))
  val test = new HttpExecutionServiceDAO(
    executionServiceURL = mockServer.mockServerBaseUrl,
    submissionTimeout = 5 seconds,
    workbenchMetricBaseName = "test")

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    system.shutdown()
    super.afterAll()
  }

  "HttpExecutionServiceDAO" should "submit workflows" in {
    withStatsD {
      val submitResult = test.submitWorkflows("wdl", Seq("input1", "input2"), None, userInfo).futureValue

      // results are hard-coded in RemoteServicesMockServer
      submitResult.size shouldBe 3
      val (lefts, rights) = submitResult.toList.separate
      lefts.size shouldBe 3
      rights shouldBe 'empty
      lefts.filter(_.status == "Submitted").size shouldBe 2
      lefts.filter(_.status == "Failed").size shouldBe 1
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("post", "api.workflows.v1.batch", 201, 1, Option(Subsystems.Cromwell)))
    }
  }

  it should "query for status" in {
    withStatsD {
      val result = test.status("foo", userInfo).futureValue
      result.status shouldBe "Running"
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get", "api.workflows.v1.redacted.status", 200, 1, Option(Subsystems.Cromwell)))
    }
  }

  it should "query for metadata" in {
    withStatsD {
      val result = test.callLevelMetadata("8afafe21-2b70-4180-a565-748cb573e10c", userInfo).futureValue
      result shouldBe a [JsObject]
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get", "api.workflows.v1.redacted.metadata", 201, 1, Option(Subsystems.Cromwell)))
    }
  }

  it should "query for outputs" in {
    withStatsD {
      val result = test.outputs("69d1d92f-3895-4a7b-880a-82535e9a096e", userInfo).futureValue
      result.id shouldBe "this_workflow_exists"
      result.outputs.size shouldBe 3
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get", "api.workflows.v1.redacted.outputs", 201, 1, Option(Subsystems.Cromwell)))
    }
  }

  it should "query for logs" in {
    withStatsD {
      val result = test.logs("8afafe21-2b70-4180-a565-748cb573e10c", userInfo).futureValue
      result.id shouldBe "8afafe21-2b70-4180-a565-748cb573e10c"
      result.calls shouldBe 'empty
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get", "api.workflows.v1.redacted.logs", 201, 1, Option(Subsystems.Cromwell)))
    }
  }

  it should "abort workflows" in {
    withStatsD {
      val result = test.abort("workflowA", userInfo).futureValue
      result.isSuccess shouldBe true
      result.get.id shouldBe "workflowA"
      result.get.status shouldBe "Aborted"
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("post", "api.workflows.v1.redacted.abort", 200, 1, Option(Subsystems.Cromwell)))
    }
  }

  it should "get the version" in {
    withStatsD {
      val result = test.version.futureValue
      result.cromwell shouldBe "25"
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get", "engine.v1.version", 200, 1, Option(Subsystems.Cromwell)))
    }
  }

}
