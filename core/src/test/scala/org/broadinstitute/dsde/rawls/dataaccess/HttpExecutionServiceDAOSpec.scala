package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.implicits._
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{
  MetadataParams,
  RawlsUserEmail,
  RawlsUserSubjectId,
  Subsystems,
  UserInfo,
  WdlSource
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import spray.json.JsObject

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by rtitle on 8/17/17.
  */
class HttpExecutionServiceDAOSpec
    extends TestKit(ActorSystem("HttpExecutionServiceDAOSpec"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually
    with MockitoTestUtils
    with RawlsStatsDTestUtils {

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val mockServer = RemoteServicesMockServer()
  val userInfo = UserInfo(RawlsUserEmail("owner-access"),
                          OAuth2BearerToken("token"),
                          123,
                          RawlsUserSubjectId("123456789876543212345")
  )
  val test =
    new HttpExecutionServiceDAO(executionServiceURL = mockServer.mockServerBaseUrl, workbenchMetricBaseName = "test")

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    Await.result(system.terminate(), Duration.Inf)
    super.afterAll()
  }

  "HttpExecutionServiceDAO" should "submit workflows" in {
    withStatsD {
      val submitResult =
        test.submitWorkflows(WdlSource("wdl"), Seq("input1", "input2"), None, None, None, userInfo).futureValue

      // results are hard-coded in RemoteServicesMockServer
      submitResult.size shouldBe 3
      val (lefts, rights) = submitResult.toList.separate
      lefts.size shouldBe 3
      rights shouldBe 'empty
      lefts.filter(_.status == "Submitted").size shouldBe 2
      lefts.filter(_.status == "Failed").size shouldBe 1
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("post",
                                                                               "api.workflows.v1.batch",
                                                                               201,
                                                                               1,
                                                                               Option(Subsystems.Cromwell)
      ))
    }
  }

  it should "query for status" in {
    withStatsD {
      val result = test.status("foo", userInfo).futureValue
      result.status shouldBe "Running"
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get",
                                                                               "api.workflows.v1.redacted.status",
                                                                               200,
                                                                               1,
                                                                               Option(Subsystems.Cromwell)
      ))
    }
  }

  it should "query for metadata" in {
    withStatsD {
      val result =
        test.callLevelMetadata("8afafe21-2b70-4180-a565-748cb573e10c", MetadataParams(), userInfo).futureValue
      result shouldBe a[JsObject]
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get",
                                                                               "api.workflows.v1.redacted.metadata",
                                                                               201,
                                                                               1,
                                                                               Option(Subsystems.Cromwell)
      ))
    }
  }

  it should "query for outputs" in {
    withStatsD {
      val result = test.outputs("69d1d92f-3895-4a7b-880a-82535e9a096e", userInfo).futureValue
      result.id shouldBe "this_workflow_exists"
      result.outputs.size shouldBe 3
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get",
                                                                               "api.workflows.v1.redacted.outputs",
                                                                               201,
                                                                               1,
                                                                               Option(Subsystems.Cromwell)
      ))
    }
  }

  it should "query for logs" in {
    withStatsD {
      val result = test.logs("8afafe21-2b70-4180-a565-748cb573e10c", userInfo).futureValue
      result.id shouldBe "8afafe21-2b70-4180-a565-748cb573e10c"
      result.calls shouldBe 'empty
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get",
                                                                               "api.workflows.v1.redacted.logs",
                                                                               201,
                                                                               1,
                                                                               Option(Subsystems.Cromwell)
      ))
    }
  }

  it should "abort workflows" in {
    withStatsD {
      val result = test.abort("workflowA", userInfo).futureValue
      result.isSuccess shouldBe true
      result.get.id shouldBe "workflowA"
      result.get.status shouldBe "Aborted"
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("post",
                                                                               "api.workflows.v1.redacted.abort",
                                                                               200,
                                                                               1,
                                                                               Option(Subsystems.Cromwell)
      ))
    }
  }

  it should "get labels and patch labels" in {
    withStatsD {
      val labels = Map("key1" -> "val1", "key2" -> "val2")

      val patchResult = test.patchLabels("8afafe21-2b70-4180-a565-748cb573e10c", userInfo, labels).futureValue
      patchResult.labels.get("key1") shouldBe Some("val1")
      patchResult.labels.get("key2") shouldBe Some("val2")

      val getResult = test.getLabels("8afafe21-2b70-4180-a565-748cb573e10c", userInfo).futureValue
      getResult.labels.get("key1") shouldBe Some("val1")
      getResult.labels.get("key2") shouldBe Some("val2")
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (
        expectedHttpRequestMetrics("get", "api.workflows.v1.redacted.labels", 200, 1, Option(Subsystems.Cromwell)) ++
          expectedHttpRequestMetrics("patch", "api.workflows.v1.redacted.labels", 200, 1, Option(Subsystems.Cromwell))
      )
    }
  }

  it should "get the version" in {
    withStatsD {
      val result = test.version.futureValue
      result.cromwell shouldBe "25"
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get",
                                                                               "engine.v1.version",
                                                                               200,
                                                                               1,
                                                                               Option(Subsystems.Cromwell)
      ))
    }
  }

}
