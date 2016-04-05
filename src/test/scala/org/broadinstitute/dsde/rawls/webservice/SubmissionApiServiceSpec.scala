package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{SubmissionReportFormat, SubmissionRequestFormat, SubmissionStatusResponseFormat, SubmissionListResponseFormat}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.http._
import scala.concurrent.ExecutionContext
import java.util.UUID

/**
 * Created by dvoet on 4/24/15.
 */
class SubmissionApiServiceSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices(dataSource: SlickDataSource)(testCode: TestApiService => Any): Unit = {

    val gcsDAO = new MockGoogleServicesDAO("test")
    gcsDAO.storeToken(userInfo, "test_token")

    val apiService = new TestApiService(dataSource, gcsDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices(testCode: TestApiService => Any): Unit = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  "SubmissionApi" should "return 404 Not Found when creating a submission using a MethodConfiguration that doesn't exist in the workspace" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", httpJson(SubmissionRequest("dsde","not there","Pattern","pattern1", None))) ~>
      sealRoute(services.submissionRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 404 Not Found when creating a submission using an Entity that doesn't exist in the workspace" in withTestDataApiServices { services =>
    val mcName = MethodConfigurationName("three_step","dsde", testData.wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name,"Pattern", Map.empty, Map("three_step.cgrep.pattern"->AttributeString("String")), Map.empty, MethodRepoMethod("dsde","three_step",1))
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/methodconfigs", httpJson(methodConf)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check { assertResult(StatusCodes.Created) {status} }
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", httpJson(SubmissionRequest(mcName.namespace, mcName.name,"Pattern","pattern1", None))) ~>
      sealRoute(services.submissionRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  private def createAndMonitorSubmission(wsName: WorkspaceName, methodConf: MethodConfiguration,
                                         submissionEntity: Entity, submissionExpression: Option[String],
                                          services: TestApiService): SubmissionStatusResponse = {
      Post(s"/workspaces/${wsName.namespace}/${wsName.name}/methodconfigs", httpJson(methodConf)) ~>
          sealRoute(services.methodConfigRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      val submissionRq = SubmissionRequest(methodConf.namespace, methodConf.name, submissionEntity.entityType, submissionEntity.name, submissionExpression)
      Post(s"/workspaces/${wsName.namespace}/${wsName.name}/submissions", httpJson(submissionRq)) ~>
          sealRoute(services.submissionRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          val submission = responseAs[SubmissionReport]
          Get(s"/workspaces/${wsName.namespace}/${wsName.name}/submissions/${submission.submissionId}") ~>
                  sealRoute(services.submissionRoutes) ~>
            check {
              assertResult(StatusCodes.OK) {
                status
              }
              return responseAs[SubmissionStatusResponse]
            }
        }

    fail("Unable to create and monitor submissions")
  }

  it should "return 201 Created when creating and monitoring a submission with no expression" in withTestDataApiServices { services =>
  val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sample1, None, services)

    assertResult(1) {
      submission.workflows.size
    }
  }
  it should "return 201 Created when creating and monitoring a submission with valid expression" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sset1, Option("this.samples"), services)

    assertResult(3) {
      submission.workflows.size
    }
  }

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val z1 = Entity("z1", "Sample", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList))
  val workspace2Name = new WorkspaceName(testData.wsName.namespace + "2", testData.wsName.name + "2")
  val workspace2Request = WorkspaceRequest(
    workspace2Name.namespace,
    workspace2Name.name,
    None,
    Map.empty
  )

  it should "return 200 on getting a submission" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/${testData.submission1.submissionId}") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {status}
        assertResult(new SubmissionStatusResponse(testData.submission1, testData.userOwner)) {responseAs[SubmissionStatusResponse]}
      }
  }

  it should "return 404 on getting a nonexistent submission" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/unrealSubmission42") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {status}
      }
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/${UUID.randomUUID}") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {status}
      }
  }

  it should "return 200 when listing submissions" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(List(
          new SubmissionListResponse(testData.submissionTerminateTest, testData.userOwner),
          new SubmissionListResponse(testData.submission1, testData.userOwner),
          new SubmissionListResponse(testData.submission2, testData.userOwner),
          new SubmissionListResponse(testData.submissionUpdateEntity, testData.userOwner),
          new SubmissionListResponse(testData.submissionUpdateWorkspace, testData.userOwner))) {
          responseAs[List[SubmissionListResponse]]
        }
      }
  }

  it should "return 200 when counting submissions" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissionsCount") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(Map("Submitted" -> 5)) {
          responseAs[Map[String, Int]]
        }
      }
  }
}
