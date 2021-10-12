package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{ActiveSubmissionFormat, WorkflowQueueStatusByUserResponseFormat}
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectTransferFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.{AttributeReferenceFormat, WorkspaceDetailsAndCacheErrorFormat, WorkspaceDetailsFormat}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor.{MIN_CACHE_TIME, RESET_CACHE_TIME}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.ExecutionContext

/**
 * Created by tsharpe on 9/28/15.
 */
class AdminApiServiceSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService =>  T): T = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
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

  def withConstantTestDataApiServices[T](testCode: TestApiService =>  T): T = {
    withConstantTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  "AdminApi" should "return 200 when listing active submissions" in withConstantTestDataApiServices { services =>
    val expected = Seq(
      ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submissionNoWorkflows),
      ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submission1),
      ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submission2))

    withStatsD {
      Get("/admin/submissions") ~>
        sealRoute(instrumentRequest { services.adminRoutes }) ~>
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

  it should "return 201 when registering a billing project for the 1st time, and 500 for the 2nd" in withTestDataApiServices { services =>
    Post(s"/admin/project/registration", httpJson(RawlsBillingProjectTransfer(project, bucket, userInfo.userEmail.value, userInfo.accessToken.value))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created, responseAs[String]) {
          status
        }
      }

    Post(s"/admin/project/registration", httpJson(RawlsBillingProjectTransfer(project, bucket, userInfo.userEmail.value, userInfo.accessToken.value))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.InternalServerError) {
          status
        }
      }
  }

  it should "return 204 when unregistering a billing project" in withTestDataApiServices { services =>
    val projectName = "unregistered-bp"

    Post(s"/admin/project/registration", httpJson(RawlsBillingProjectTransfer(projectName, bucket, userInfo.userEmail.value, userInfo.accessToken.value))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created, responseAs[String]) {
          status
        }
      }

    Delete(s"/admin/project/registration/$projectName", httpJson(Map("newOwnerEmail" -> userInfo.userEmail.value, "newOwnerToken" -> userInfo.accessToken.token))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent, responseAs[String]) {
          status
        }
      }
  }

  it should "return 200 when listing active submissions on deleted entities" in withConstantTestDataApiServices { services =>
    Post(s"${constantData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(constantData.indiv1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }

    Get(s"/admin/submissions") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val resp = responseAs[Array[ActiveSubmission]]

        // entity name will be modified a la DriverComponent.renameForHiding

        val responseEntityNames = resp.map(_.submission).map(_.submissionEntity).map(_.get.entityName).toSet
        assertResult(1)(responseEntityNames.size)
        assert(responseEntityNames.head.contains(constantData.indiv1.name + "_"))

        // check that the response contains the same submissions, with only entity names changed

        val expected = Seq(
          ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submissionNoWorkflows),
          ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submission1),
          ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submission2))

        def withNewEntityNames(in: Seq[ActiveSubmission]): Seq[ActiveSubmission] = {
          in.map { as =>
            as.copy(submission = as.submission.copy(submissionEntity = Some(as.submission.submissionEntity.get.copy(entityName = "newName"))))
          }
        }

        assertSameElements(withNewEntityNames(expected), withNewEntityNames(resp))
      }
  }

  it should "return 204 when aborting an active submission" in withTestDataApiServices { services =>
    Delete(s"/admin/submissions/${testData.wsName.namespace}/${testData.wsName.name}/${testData.submissionTerminateTest.submissionId}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
  }

  it should "return 404 when aborting a bogus active submission" in withTestDataApiServices { services =>
    Delete(s"/admin/submissions/${testData.wsName.namespace}/${testData.wsName.name}/fake") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 when adding a library curator" in withTestDataApiServices { services =>
    val testUser = "foo@bar.com"
    Put(s"/admin/user/role/curator/${testUser}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 200 when removing a library curator" in withTestDataApiServices { services =>
    val testUser = "foo@bar.com"
    Put(s"/admin/user/role/curator/${testUser}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Delete(s"/admin/user/role/curator/${testUser}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 200 when listing all workspaces" in withTestDataApiServices { services =>
    Get(s"/admin/workspaces") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        // TODO: why is this result returned out of order?
        sortAndAssertWorkspaceResult(testData.allWorkspaces) { responseAs[Seq[WorkspaceDetails]].map(_.toWorkspace) }
      }
  }

  it should "return 200 when getting workspaces by a string attribute" in withConstantTestDataApiServices { services =>
    Get(s"/admin/workspaces?attributeName=string&valueString=yep%2C%20it's%20a%20string") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertWorkspaceResult(Seq(constantData.workspace)) { responseAs[Seq[WorkspaceDetails]].map(_.toWorkspace) }
      }
  }

  it should "return 200 when getting workspaces by a numeric attribute" in withConstantTestDataApiServices { services =>
    Get(s"/admin/workspaces?attributeName=number&valueNumber=10") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertWorkspaceResult(Seq(constantData.workspace)) { responseAs[Seq[WorkspaceDetails]].map(_.toWorkspace) }
      }
  }

  it should "return 200 when getting workspaces by a boolean attribute" in withTestDataApiServices { services =>
    Get(s"/admin/workspaces?attributeName=library%3Apublished&valueBoolean=true") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertWorkspaceResult(Seq(testData.workspacePublished)) { responseAs[Seq[WorkspaceDetails]].map(_.toWorkspace) }
      }
  }

  it should "get queue status by user" in withConstantTestDataApiServices { services =>
    import driver.api._

    // Create a new test user and some new submissions
    val testUserEmail = "testUser"
    val testSubjectId = "0001"
    val testUserStatusCounts = Map(WorkflowStatuses.Submitted -> 1, WorkflowStatuses.Running -> 10, WorkflowStatuses.Aborting -> 100)
    withWorkspaceContext(constantData.workspace) { ctx =>
      val testUser = RawlsUser(UserInfo(RawlsUserEmail(testUserEmail), OAuth2BearerToken("token"), 123, RawlsUserSubjectId(testSubjectId)))
      val inputResolutionsList = Seq(SubmissionValidationValue(Option(
        AttributeValueList(Seq(AttributeString("elem1"), AttributeString("elem2"), AttributeString("elem3")))), Option("message3"), "test_input_name3"))
      testUserStatusCounts.flatMap { case (st, count) =>
        for (_ <- 0 until count) yield {
          createTestSubmission(constantData.workspace, constantData.methodConfig, constantData.sset1, WorkbenchEmail(testUser.userEmail.value),
            Seq(constantData.sset1), Map(constantData.sset1 -> inputResolutionsList),
            Seq.empty, Map.empty, st)
        }
      }.foreach { sub =>
        runAndWait(submissionQuery.create(ctx, sub))
      }
    }

    Get("/admin/submissions/queueStatusByUser") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val workflowRecs = runAndWait(workflowQuery.result)
        val groupedWorkflowRecs = workflowRecs.groupBy(_.status)
          .filterKeys((WorkflowStatuses.queuedStatuses ++ WorkflowStatuses.runningStatuses).map(_.toString).contains)
          .mapValues(_.size)

        val testUserWorkflows = (testUserEmail -> testUserStatusCounts.map { case (k, v) => k.toString -> v })

        // userOwner workflow counts should be equal to all workflows in the system except for testUser's workflows.
        val userOwnerWorkflows = (constantData.userOwner.userEmail.value ->
          groupedWorkflowRecs.map { case (k, v) =>
            k -> (v - testUserStatusCounts.getOrElse(WorkflowStatuses.withName(k), 0))
          }.filter(_._2 > 0))

        val expectedResponse = WorkflowQueueStatusByUserResponse(
          groupedWorkflowRecs,
          Map(userOwnerWorkflows, testUserWorkflows),
          services.maxActiveWorkflowsTotal,
          services.maxActiveWorkflowsPerUser)
        assertResult(expectedResponse) {
          responseAs[WorkflowQueueStatusByUserResponse]
        }
      }
  }

  // utility functions used by the entity statistics cache tests, following
  def assertInvalidCacheCount(services: TestApiService, expectedCount: Int, expectedWorkspaces: Seq[Workspace] = Seq(),
                              clue: String = "") = {
    Get("/admin/workspaces/entities/cache") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        status shouldBe StatusCodes.OK
        val actual = responseAs[Seq[WorkspaceDetailsAndCacheError]]
        withClue(s"$clue, checking the API's count of invalid caches: ") {
          actual.length shouldBe expectedCount
          actual.map(_.workspace.toWorkspace.toWorkspaceName) should contain theSameElementsAs
            expectedWorkspaces.map(_.toWorkspaceName)
        }
      }
  }

  def assertDatabaseCacheCount(services: TestApiService, expectedCount: Int, clue: String = "") = {
    // ensure they inserted
    import driver.api._
    val cacheQuery = services.dataSource.dataAccess.entityCacheQuery
    val cacheRowCount = runAndWait(cacheQuery.length.result)
    withClue(s"$clue, checking the database row count for WORKSPACE_ENTITY_CACHE") {
      cacheRowCount shouldBe expectedCount
    }
  }

  def setValidCaches(services: TestApiService, workspaces: Seq[Workspace]) =
    setCaches(services, workspaces.map( w => (w.workspaceIdAsUUID, new Timestamp(w.lastModified.getMillis))))

  // TODO: why is this failing??????
  def setInvalidCaches(services: TestApiService, workspaces: Seq[Workspace]) =
//    workspaces.map(w => services.dataSource.dataAccess.entityCacheQuery.setCacheInvalid(Seq(w.workspaceIdAsUUID)))
    setCaches(services, workspaces.map( w => (w.workspaceIdAsUUID, MIN_CACHE_TIME)))

  def setCaches(services: TestApiService, workspaceIdsAndTimestamps: Seq[(UUID, Timestamp)]) = {
    workspaceIdsAndTimestamps foreach {
      case (workspaceId, timestamp) =>
        val upsert = runAndWait(services.dataSource.dataAccess.entityCacheQuery.updateCacheLastUpdated(workspaceId, timestamp))
        upsert shouldBe 1
    }
  }

  it should "return empty array if no invalid statistics caches" in withTestDataApiServices { services =>
    // at this point there should be zero rows in WORKSPACE_ENTITY_CACHE
    assertInvalidCacheCount(services, 0)
    // insert some valid rows into WORKSPACE_ENTITY_CACHE
    setValidCaches(services, Seq(testData.workspace, testData.workspaceNoAttrs, testData.workspacePublished))
    // check again
    assertInvalidCacheCount(services, 0, clue = "after inserting two valid cache rows")
  }

  it should "return all invalid statistics caches" in withTestDataApiServices { services =>
    // at this point there should be zero rows in WORKSPACE_ENTITY_CACHE
    assertInvalidCacheCount(services, 0)
    // insert one valid and two invalid caches into WORKSPACE_ENTITY_CACHE
    setValidCaches(services, Seq(testData.workspaceNoAttrs))

    val invalidCaches = Seq(testData.workspace, testData.workspacePublished)
    setInvalidCaches(services, invalidCaches)

    // ensure they inserted
    assertDatabaseCacheCount(services, 3, "after inserting one valid and two invalid cache rows")

    // check again
    assertInvalidCacheCount(services, 2, invalidCaches, "after inserting one valid and two invalid cache rows")
  }

  it should "translate timestamps correctly" in {
    // this test validates the timezone negotation/translation between mysql server and client.
    // in live environments, both server and client use UTC timezone; test behavior should mimic that.
    val min = MIN_CACHE_TIME
    min shouldBe new Timestamp(1000)
    min.toString shouldBe "1970-01-01 00:00:01.0"
  }

  it should "reset any invalid statistics caches" in withTestDataApiServices { services =>
    // at this point there should be zero rows in WORKSPACE_ENTITY_CACHE
    assertInvalidCacheCount(services, 0)
    // insert one valid and two invalid caches into WORKSPACE_ENTITY_CACHE
    setValidCaches(services, Seq(testData.workspaceNoAttrs))

    val invalidCaches = Seq(testData.workspace, testData.workspacePublished)
    setInvalidCaches(services, invalidCaches)

    // ensure they inserted
    assertDatabaseCacheCount(services, 3, "after inserting one valid and two invalid cache rows")

    // check again
    assertInvalidCacheCount(services, 2, invalidCaches, "after inserting one valid and two invalid cache rows")

    // now reset the cache value for one of the invalid workspaces
    val payload = Seq(testData.workspacePublished.workspaceId)
    Post("/admin/workspaces/entities/cache", payload) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        status shouldBe StatusCodes.OK
        val actual = responseAs[Map[String, Int]]
        actual.keySet should contain theSameElementsAs Set("numReset")
        actual("numReset") shouldBe 1
      }

    import driver.api._

    // query db directly for reset cache value
    val actualTimestamp = runAndWait(uniqueResult[Timestamp](entityCacheQuery.filter(_.workspaceId === testData.workspacePublished.workspaceIdAsUUID).map(_.entityCacheLastUpdated).result))
    actualTimestamp should contain (RESET_CACHE_TIME)

    // check API again
    assertInvalidCacheCount(services, 1, Seq(testData.workspace), "after resetting one workspace's cache validity")
  }

  it should "not reset any valid statistics caches" in withTestDataApiServices { services =>
    // at this point there should be zero rows in WORKSPACE_ENTITY_CACHE
    assertInvalidCacheCount(services, 0)
    // insert one valid and two invalid caches into WORKSPACE_ENTITY_CACHE
    setValidCaches(services, Seq(testData.workspaceNoAttrs))

    val invalidCaches = Seq(testData.workspace, testData.workspacePublished)
    setInvalidCaches(services, invalidCaches)

    // ensure they inserted
    assertDatabaseCacheCount(services, 3, "after inserting one valid and two invalid cache rows")

    // check again
    assertInvalidCacheCount(services, 2, invalidCaches, "after inserting one valid and two invalid cache rows")

    // now *attempt* to reset the cache value for the valid workspace
    val payload = Seq(testData.workspaceNoAttrs.workspaceId)
    Post("/admin/workspaces/entities/cache", payload) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        status shouldBe StatusCodes.OK
        val actual = responseAs[Map[String, Int]]
        actual.keySet should contain theSameElementsAs Set("numReset")
        actual("numReset") shouldBe 0
      }

    import driver.api._

    // query db directly for the cache time - should be unmodified
    val actualTimestamp = runAndWait(uniqueResult[Timestamp](entityCacheQuery.filter(_.workspaceId === testData.workspaceNoAttrs.workspaceIdAsUUID).map(_.entityCacheLastUpdated).result))
    actualTimestamp should contain (new Timestamp(testData.workspaceNoAttrs.lastModified.getMillis))

    // check API again
    assertInvalidCacheCount(services, 2, invalidCaches, "after resetting one workspace's cache validity")
  }
}
