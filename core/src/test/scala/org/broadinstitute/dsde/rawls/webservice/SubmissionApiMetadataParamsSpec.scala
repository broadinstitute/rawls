package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.MetadataParamsFormat
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by davidan on 11/5/18.
 */
class SubmissionApiMetadataParamsSpec extends ApiServiceSpec {

  class ParamValidatingExecutionServiceDAO(executionServiceURL: String, override val workbenchMetricBaseName: String) extends HttpExecutionServiceDAO(executionServiceURL: String,  workbenchMetricBaseName: String) {
    override def callLevelMetadata(id: String, metadataParams: MetadataParams, userInfo: UserInfo): Future[JsObject] = {
      // returns the MetadataParams from argument list, so tests can validate them
      Future.successful(metadataParams.toJson.asJsObject)
    }
  }

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO, override val executionServiceCluster: MockShardedExecutionServiceCluster)
                           (implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives


  def withApiServices[T](dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test"))(testCode: TestApiService =>  T): T = {
    val apiService = new TestApiService(dataSource, gcsDAO, new MockGooglePubSubDAO, MockShardedExecutionServiceCluster.fromDAO(new ParamValidatingExecutionServiceDAO("unused", "unused"), dataSource))

    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService =>  T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  val submissionId = testData.submission1.submissionId
  val workflowId = testData.submission1.workflows.head.workflowId.get // assume it exists; if it doesn't, will throw exception so the test fails

  val basePath = s"${testData.workspace.path}/submissions/$submissionId/workflows/$workflowId"

  "SubmissionApi" should "use defaults for metadata params when no querystring" in withTestDataApiServices { services =>
    Get(s"$basePath") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        val actualParams = responseAs[MetadataParams]
        // get the defaults
        val expectedParams = new MetadataParams()

        actualParams.includeKeys should contain theSameElementsAs expectedParams.includeKeys
        actualParams.excludeKeys should contain theSameElementsAs expectedParams.excludeKeys
        actualParams.expandSubWorkflows shouldBe expectedParams.expandSubWorkflows
      }
  }

  it should "pass expandSubWorkflows=true param" in withTestDataApiServices { services =>
    Get(s"$basePath?expandSubWorkflows=true") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        val actualParams = responseAs[MetadataParams]
        // get the defaults
        val expectedParams = new MetadataParams(expandSubWorkflows=true)

        actualParams.includeKeys should contain theSameElementsAs expectedParams.includeKeys
        actualParams.excludeKeys should contain theSameElementsAs expectedParams.excludeKeys
        actualParams.expandSubWorkflows shouldBe expectedParams.expandSubWorkflows
      }
  }

  it should "pass expandSubWorkflows=false param" in withTestDataApiServices { services =>
    Get(s"$basePath?expandSubWorkflows=false") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        val actualParams = responseAs[MetadataParams]
        // get the defaults
        val expectedParams = new MetadataParams(expandSubWorkflows=false)

        actualParams.includeKeys should contain theSameElementsAs expectedParams.includeKeys
        actualParams.excludeKeys should contain theSameElementsAs expectedParams.excludeKeys
        actualParams.expandSubWorkflows shouldBe expectedParams.expandSubWorkflows
      }
  }

  it should "pass single includeKey param" in withTestDataApiServices { services =>
    Get(s"$basePath?includeKey=foo") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        val actualParams = responseAs[MetadataParams]
        // get the defaults
        val expectedParams = new MetadataParams(includeKeys=Seq("foo"))

        actualParams.includeKeys should contain theSameElementsAs expectedParams.includeKeys
        actualParams.excludeKeys should contain theSameElementsAs expectedParams.excludeKeys
        actualParams.expandSubWorkflows shouldBe expectedParams.expandSubWorkflows
      }
  }

  it should "pass multiple includeKey params" in withTestDataApiServices { services =>
    Get(s"$basePath?includeKey=foo&includeKey=bar") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        val actualParams = responseAs[MetadataParams]
        // get the defaults
        val expectedParams = new MetadataParams(includeKeys=Seq("foo", "bar"))

        actualParams.includeKeys should contain theSameElementsAs expectedParams.includeKeys
        actualParams.excludeKeys should contain theSameElementsAs expectedParams.excludeKeys
        actualParams.expandSubWorkflows shouldBe expectedParams.expandSubWorkflows
      }
  }

  it should "pass single excludeKey param" in withTestDataApiServices { services =>
    Get(s"$basePath?excludeKey=baz") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        val actualParams = responseAs[MetadataParams]
        // get the defaults
        val expectedParams = new MetadataParams(excludeKeys=Seq("baz"))

        actualParams.includeKeys should contain theSameElementsAs expectedParams.includeKeys
        actualParams.excludeKeys should contain theSameElementsAs expectedParams.excludeKeys
        actualParams.expandSubWorkflows shouldBe expectedParams.expandSubWorkflows
      }
  }

  it should "pass multiple excludeKey params" in withTestDataApiServices { services =>
    Get(s"$basePath?excludeKey=baz&excludeKey=qux") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        val actualParams = responseAs[MetadataParams]
        // get the defaults
        val expectedParams = new MetadataParams(excludeKeys=Seq("baz", "qux"))

        actualParams.includeKeys should contain theSameElementsAs expectedParams.includeKeys
        actualParams.excludeKeys should contain theSameElementsAs expectedParams.excludeKeys
        actualParams.expandSubWorkflows shouldBe expectedParams.expandSubWorkflows
      }
  }

  it should "pass all params when they are all specified" in withTestDataApiServices { services =>
    Get(s"$basePath?expandSubWorkflows=true&includeKey=foo&includeKey=bar&excludeKey=baz&excludeKey=qux") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        val actualParams = responseAs[MetadataParams]
        // get the defaults
        val expectedParams = new MetadataParams(includeKeys=Seq("foo", "bar"), excludeKeys=Seq("baz", "qux"), expandSubWorkflows=true)

        actualParams.includeKeys should contain theSameElementsAs expectedParams.includeKeys
        actualParams.excludeKeys should contain theSameElementsAs expectedParams.excludeKeys
        actualParams.expandSubWorkflows shouldBe expectedParams.expandSubWorkflows
      }
  }

  val testExecSvcDummyBase = "http://localhost:12345/unit-test"
  val testExecSvcDAO = new HttpExecutionServiceDAO(testExecSvcDummyBase, "unused")
  val testExecSvcDummyWorkflowId = java.util.UUID.randomUUID().toString

  "HttpExecutionServiceDAO.getExecutionServiceMetadataUri" should "generate default url to Cromwell given default metadata params" in {
    val params = new MetadataParams()
    val actualUri:Uri = testExecSvcDAO.getExecutionServiceMetadataUri(testExecSvcDummyWorkflowId, params)

    val query = actualUri.query()
    query.getAll("expandSubWorkflows") shouldBe List("false")
    query.getAll("includeKey") should contain theSameElementsAs List()
    query.getAll("excludeKey") should contain theSameElementsAs List()
  }

  it should "generate expandSubWorkflows=true url to Cromwell given appropriate metadata params" in {
    val params = new MetadataParams(expandSubWorkflows = true)
    val actualUri:Uri = testExecSvcDAO.getExecutionServiceMetadataUri(testExecSvcDummyWorkflowId, params)

    val query = actualUri.query()
    query.getAll("expandSubWorkflows") shouldBe List("true")
    query.getAll("includeKey") should contain theSameElementsAs List()
    query.getAll("excludeKey") should contain theSameElementsAs List()
  }

  it should "generate expandSubWorkflows=false url to Cromwell given appropriate metadata params" in {
    val params = new MetadataParams(expandSubWorkflows = false)
    val actualUri:Uri = testExecSvcDAO.getExecutionServiceMetadataUri(testExecSvcDummyWorkflowId, params)

    val query = actualUri.query()
    query.getAll("expandSubWorkflows") shouldBe List("false")
    query.getAll("includeKey") should contain theSameElementsAs List()
    query.getAll("excludeKey") should contain theSameElementsAs List()
  }

  it should "generate single includeKey=* url to Cromwell given appropriate metadata params" in {
    val params = new MetadataParams(includeKeys = Seq("foo"))
    val actualUri:Uri = testExecSvcDAO.getExecutionServiceMetadataUri(testExecSvcDummyWorkflowId, params)

    val query = actualUri.query()
    query.getAll("expandSubWorkflows") shouldBe List("false")
    query.getAll("includeKey") should contain theSameElementsAs List("foo")
    query.getAll("excludeKey") should contain theSameElementsAs List()
  }

  it should "generate multiple includeKey=* url to Cromwell given appropriate metadata params" in {
    val params = new MetadataParams(includeKeys = Seq("foo","bar"))
    val actualUri:Uri = testExecSvcDAO.getExecutionServiceMetadataUri(testExecSvcDummyWorkflowId, params)

    val query = actualUri.query()
    query.getAll("expandSubWorkflows") shouldBe List("false")
    query.getAll("includeKey") should contain theSameElementsAs List("foo","bar")
    query.getAll("excludeKey") should contain theSameElementsAs List()
  }

  it should "generate single excludeKey=* url to Cromwell given appropriate metadata params" in {
    val params = new MetadataParams(excludeKeys = Seq("baz"))
    val actualUri:Uri = testExecSvcDAO.getExecutionServiceMetadataUri(testExecSvcDummyWorkflowId, params)

    val query = actualUri.query()
    query.getAll("expandSubWorkflows") shouldBe List("false")
    query.getAll("includeKey") should contain theSameElementsAs List()
    query.getAll("excludeKey") should contain theSameElementsAs List("baz")
  }

  it should "generate multiple excludeKey=* url to Cromwell given appropriate metadata params" in {
    val params = new MetadataParams(excludeKeys = Seq("baz","qux"))
    val actualUri:Uri = testExecSvcDAO.getExecutionServiceMetadataUri(testExecSvcDummyWorkflowId, params)

    val query = actualUri.query()
    query.getAll("expandSubWorkflows") shouldBe List("false")
    query.getAll("includeKey") should contain theSameElementsAs List()
    query.getAll("excludeKey") should contain theSameElementsAs List("baz","qux")
  }

  it should "generate all query params to Cromwell when they are all specified in metadata params" in {
    val params = new MetadataParams(expandSubWorkflows = true, includeKeys = Seq("foo","bar"), excludeKeys = Seq("baz","qux"))
    val actualUri:Uri = testExecSvcDAO.getExecutionServiceMetadataUri(testExecSvcDummyWorkflowId, params)

    val query = actualUri.query()
    query.getAll("expandSubWorkflows") shouldBe List("true")
    query.getAll("includeKey") should contain theSameElementsAs List("foo","bar")
    query.getAll("excludeKey") should contain theSameElementsAs List("baz","qux")
  }

}

