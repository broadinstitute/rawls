package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.billing.BillingAdminService
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.workspace.WorkspaceAdminService
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{
  ActiveSubmissionFormat,
  WorkflowQueueStatusByUserResponseFormat
}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.AttributeReferenceFormat
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import spray.json.DefaultJsonProtocol._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by tsharpe on 9/28/15.
 */
class AdminApiServiceSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(
    implicit override val executionContext: ExecutionContext
  ) extends ApiServices
      with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def withCompactConstantTestDataApiServices[T](testCode: TestApiService => T): T =
    withCompactConstantTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  "AdminApi" should "return 200 when listing active submissions" in withCompactConstantTestDataApiServices { services =>
    val expected = Seq(
      ActiveSubmission(compactConstantData.workspace.namespace,
                       compactConstantData.workspace.name,
                       compactConstantData.submissionNoWorkflows
      ),
      ActiveSubmission(compactConstantData.workspace.namespace,
                       compactConstantData.workspace.name,
                       compactConstantData.submission1
      ),
      ActiveSubmission(compactConstantData.workspace.namespace,
                       compactConstantData.workspace.name,
                       compactConstantData.submission2
      )
    )

    withStatsD {
      Get("/admin/submissions") ~>
        sealRoute(captureRequestMetrics(traceRequests(_ => services.adminRoutes(userInfo = userInfo)))) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertSameElements(expected, responseAs[Seq[ActiveSubmission]])
        }
    } { capturedMetrics =>
      val expected = expectedHttpRequestMetrics("get", "admin.submissions", StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  val project = "some-project"
  val bucket = "some-bucket"

  it should "return 200 when listing active submissions on deleted entities" in withCompactConstantTestDataApiServices {
    services =>
      Post(s"${compactConstantData.workspace.path}/entities/delete",
           httpJson(EntityDeleteRequest(compactConstantData.indiv1))
      ) ~>
        sealRoute(services.entityRoutes(userInfo = userInfo)) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }

      Get(s"/admin/submissions") ~>
        sealRoute(services.adminRoutes(userInfo = userInfo)) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }

          val resp = responseAs[Array[ActiveSubmission]]

          // entity name will be modified a la DriverComponent.renameForHiding

          val responseEntityNames = resp.map(_.submission).map(_.submissionEntity).map(_.get.entityName).toSet
          assertResult(1)(responseEntityNames.size)
          assert(responseEntityNames.head.contains(compactConstantData.indiv1.name + "_"))

          // check that the response contains the same submissions, with only entity names changed

          val expected = Seq(
            ActiveSubmission(compactConstantData.workspace.namespace,
                             compactConstantData.workspace.name,
                             compactConstantData.submissionNoWorkflows
            ),
            ActiveSubmission(compactConstantData.workspace.namespace,
                             compactConstantData.workspace.name,
                             compactConstantData.submission1
            ),
            ActiveSubmission(compactConstantData.workspace.namespace,
                             compactConstantData.workspace.name,
                             compactConstantData.submission2
            )
          )

          def withNewEntityNames(in: Seq[ActiveSubmission]): Seq[ActiveSubmission] =
            in.map { as =>
              as.copy(submission =
                as.submission.copy(submissionEntity =
                  Some(as.submission.submissionEntity.get.copy(entityName = "newName"))
                )
              )
            }

          assertSameElements(withNewEntityNames(expected), withNewEntityNames(resp))
        }
  }

  it should "return 204 when aborting an active submission" in withTestDataApiServices { services =>
    Delete(
      s"/admin/submissions/${testData.wsName.namespace}/${testData.wsName.name}/${testData.submissionTerminateTest.submissionId}"
    ) ~>
      sealRoute(services.adminRoutes(userInfo = userInfo)) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
      }
  }

  it should "return 404 when aborting a bogus active submission" in withTestDataApiServices { services =>
    Delete(s"/admin/submissions/${testData.wsName.namespace}/${testData.wsName.name}/fake") ~>
      sealRoute(services.adminRoutes(userInfo = userInfo)) ~>
      check {
        assertResult(StatusCodes.NotFound)(status)
      }
  }

  it should "get queue status by user" in withCompactConstantTestDataApiServices { services =>
    import driver.api._

    // Create a new test user and some new submissions
    val testUserEmail = "testUser"
    val testSubjectId = "0001"
    val testUserStatusCounts =
      Map(WorkflowStatuses.Submitted -> 1, WorkflowStatuses.Running -> 10, WorkflowStatuses.Aborting -> 100)
    withWorkspaceContext(compactConstantData.workspace) { ctx =>
      val testUser = RawlsUser(
        UserInfo(RawlsUserEmail(testUserEmail), OAuth2BearerToken("token"), 123, RawlsUserSubjectId(testSubjectId))
      )
      val inputResolutionsList = Seq(
        SubmissionValidationValue(
          Option(AttributeValueList(Seq(AttributeString("elem1"), AttributeString("elem2"), AttributeString("elem3")))),
          Option("message3"),
          "test_input_name3"
        )
      )
      testUserStatusCounts
        .flatMap { case (st, count) =>
          for (_ <- 0 until count)
            yield createTestSubmission(
              compactConstantData.workspace,
              compactConstantData.methodConfig,
              compactConstantData.sset1,
              WorkbenchEmail(testUser.userEmail.value),
              Seq(compactConstantData.sset1),
              Map(compactConstantData.sset1 -> inputResolutionsList),
              Seq.empty,
              Map.empty,
              st
            )
        }
        .foreach { sub =>
          runAndWait(submissionQuery.create(ctx, sub))
        }
    }

    Get("/admin/submissions/queueStatusByUser") ~>
      sealRoute(services.adminRoutes(userInfo = userInfo)) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val workflowRecs = runAndWait(workflowQuery.result)
        val groupedWorkflowRecs = workflowRecs
          .groupBy(_.status)
          .view
          .filterKeys((WorkflowStatuses.queuedStatuses ++ WorkflowStatuses.runningStatuses).map(_.toString).contains)
          .mapValues(_.size)

        val testUserWorkflows = testUserEmail -> testUserStatusCounts.map { case (k, v) => k.toString -> v }

        // userOwner workflow counts should be equal to all workflows in the system except for testUser's workflows.
        val userOwnerWorkflows = compactConstantData.userOwner.userEmail.value ->
          groupedWorkflowRecs
            .map { case (k, v) =>
              k -> (v - testUserStatusCounts.getOrElse(WorkflowStatuses.withName(k), 0))
            }
            .toMap
            .filter(_._2 > 0)

        val expectedResponse = WorkflowQueueStatusByUserResponse(groupedWorkflowRecs.toMap,
                                                                 Map(userOwnerWorkflows, testUserWorkflows),
                                                                 services.maxActiveWorkflowsTotal,
                                                                 services.maxActiveWorkflowsPerUser
        )
        assertResult(expectedResponse) {
          responseAs[WorkflowQueueStatusByUserResponse]
        }
      }
  }

  it should "get and set feature flags for a workspace" in withCompactConstantTestDataApiServices { services =>
    val flagApiUrl =
      s"/admin/workspaces/${compactConstantData.workspace.namespace}/${compactConstantData.workspace.name}/flags"
    // workspace should start with zero flags
    Get(flagApiUrl) ~>
      sealRoute(services.adminRoutes(userInfo = userInfo)) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        assertResult(List.empty[String])(responseAs[List[String]])
      }

    // we will put and then get a few sets of flags, in order
    val flagAttempts = List(
      List("foo"),
      List("foo", "bar"),
      List("baz", "foo", "qux"),
      List.empty[String],
      List("something", "else", "entirely", "different")
    )

    flagAttempts foreach { flags =>
      withClue(s"when attempting to put feature flags $flags ... ") {
        Put(flagApiUrl, flags) ~>
          sealRoute(services.adminRoutes(userInfo = userInfo)) ~>
          check {
            assertResult(StatusCodes.OK)(status)
            responseAs[List[String]] should contain theSameElementsAs flags
          }
      }

      withClue(s"when attempting to get feature flags, expecting $flags ... ") {
        Get(flagApiUrl) ~>
          sealRoute(services.adminRoutes(userInfo = userInfo)) ~>
          check {
            assertResult(StatusCodes.OK)(status)
            responseAs[List[String]] should contain theSameElementsAs flags
          }
      }
    }
  }

  it should "return workspace ID for a valid workspace" in {
    val workspaceAdminService = mock[WorkspaceAdminService]
    when(workspaceAdminService.getWorkspaceId(compactConstantData.workspace.toWorkspaceName))
      .thenReturn(Future.successful(Option(compactConstantData.workspace.workspaceId)))
    val service = new MockApiService(workspaceAdminServiceConstructor = _ => workspaceAdminService)

    val idApiUrl =
      s"/admin/workspaces/${compactConstantData.workspace.namespace}/${compactConstantData.workspace.name}/id"
    Get(idApiUrl) ~>
      sealRoute(service.adminRoutes(userInfo = userInfo)) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String])(status)
        println(responseAs[String])
        assertResult(s"\"${compactConstantData.workspace.workspaceId}\"")(responseAs[String])
      }
  }

  it should "return 404 when getting ID for a non-existent workspace" in {
    val workspaceAdminService = mock[WorkspaceAdminService]
    when(workspaceAdminService.getWorkspaceId(any())).thenReturn(Future.successful(None))
    val service = new MockApiService(workspaceAdminServiceConstructor = _ => workspaceAdminService)

    val nonExistentWorkspace = "nonexistent-workspace"
    val idApiUrl = s"/admin/workspaces/${compactConstantData.workspace.namespace}/$nonExistentWorkspace/id"

    Get(idApiUrl) ~>
      sealRoute(service.adminRoutes(userInfo = userInfo)) ~>
      check {
        assertResult(StatusCodes.NotFound)(status)
      }
  }

  it should "get a workspace with its current settings" in {
    val workspaceId = UUID.randomUUID
    val workspaceAdminService = mock[WorkspaceAdminService]

    when(workspaceAdminService.getWorkspaceById(workspaceId)).thenReturn(
      Future.successful(
        WorkspaceAdminResponse(
          WorkspaceDetails.fromWorkspaceAndOptions(compactConstantData.workspace, None, useAttributes = false),
          List.empty
        )
      )
    )
    val service = new MockApiService(workspaceAdminServiceConstructor = _ => workspaceAdminService)

    Get(
      s"/admin/workspaces/$workspaceId"
    ) ~> service.testRoutes ~> check {
      assertResult(StatusCodes.OK)(status)
    }

    verify(workspaceAdminService).getWorkspaceById(workspaceId)
  }

  it should "get a workspace with its current settings by googleProjectId" in {
    val googleProjectId = GoogleProjectId("google-project-id")
    val workspaceAdminService = mock[WorkspaceAdminService]

    when(workspaceAdminService.getWorkspaceByGoogleProjectId(googleProjectId)).thenReturn(
      Future.successful(
        WorkspaceAdminResponse(
          WorkspaceDetails.fromWorkspaceAndOptions(compactConstantData.workspace, None, useAttributes = false),
          List.empty
        )
      )
    )
    val service = new MockApiService(workspaceAdminServiceConstructor = _ => workspaceAdminService)

    Get(
      s"/admin/workspaces/googleProject/$googleProjectId"
    ) ~> service.testRoutes ~> check {
      assertResult(StatusCodes.OK)(status)
    }

    verify(workspaceAdminService).getWorkspaceByGoogleProjectId(googleProjectId)
  }

  it should "get a billing project with a list of its workspaces" in {
    val billingProjectName = RawlsBillingProjectName("project")
    val billingAdminService = mock[BillingAdminService]

    when(billingAdminService.getBillingProjectSupportSummary(billingProjectName)).thenReturn(
      Future.successful(
        BillingProjectAdminResponse(
          RawlsBillingProject(UUID.randomUUID(),
                              billingProjectName,
                              CreationStatuses.Ready,
                              Option(RawlsBillingAccountName("account")),
                              None
          ),
          Map("ws1" -> UUID.randomUUID, "ws2" -> UUID.randomUUID)
        )
      )
    )
    val service = new MockApiService(billingAdminServiceConstructor = _ => billingAdminService)

    Get(
      s"/admin/billing/${billingProjectName.value}"
    ) ~> service.testRoutes ~> check {
      assertResult(StatusCodes.OK)(status)
    }

    verify(billingAdminService).getBillingProjectSupportSummary(billingProjectName)
  }
}
