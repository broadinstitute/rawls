package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.PoisonPill
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.testkit.TestActorRef
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, RemoteServicesMockServer}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.{Read, Write}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.{MockUserInfoDirectives, MockUserInfoDirectivesWithUser}
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsTestUtils}
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleBigQueryDAO
import org.mockserver.verify.VerificationTimes
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.rawls.config.MethodRepoConfig
import org.broadinstitute.dsde.workbench.model.WorkbenchGroupName

import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext, Future}


class WorkspaceServiceSpec extends FlatSpec with ScalatestRouteTest with Matchers with TestDriverComponent with RawlsTestUtils with Eventually with MockitoTestUtils with RawlsStatsDTestUtils with BeforeAndAfterAll {
  import driver.api._

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val s1 = Entity("s1", "samples", Map(
    AttributeName.withDefaultNS("foo") -> AttributeString("x"),
    AttributeName.withDefaultNS("bar") -> AttributeNumber(3),
    AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(Seq(AttributeEntityReference("participant", "p1"))),
    AttributeName.withDefaultNS("splat") -> attributeList))
  val workspace = Workspace(
    testData.wsName.namespace,
    testData.wsName.name,
    "aWorkspaceId",
    "aBucket",
    currentTime(),
    currentTime(),
    "test",
    Map.empty
  )

  val mockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    super.afterAll()
  }

  case class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit val executionContext: ExecutionContext) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with MockUserInfoDirectivesWithUser {
    private val userInfo1 = UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId)
    lazy val workspaceService: WorkspaceService = workspaceServiceConstructor(userInfo1)
    lazy val userService: UserService = userServiceConstructor(userInfo1)

    def actorRefFactory = system
    val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
    val samDAO = new MockSamDAO
    val gpsDAO = new MockGooglePubSubDAO

    val notificationTopic = "test-notification-topic"
    val notificationDAO = new PubSubNotificationDAO(gpsDAO, notificationTopic)

    val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName), slickDataSource)
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      executionServiceCluster,
      slickDataSource,
      gcsDAO.getBucketServiceAccountCredential,
      workbenchMetricBaseName = "test"
    ).withDispatcher("submission-monitor-dispatcher"))

    val userServiceConstructor = UserService.constructor(
      slickDataSource,
      gcsDAO,
      notificationDAO,
      samDAO,
      Seq("bigquery.jobUser")
    )_

    val genomicsServiceConstructor = GenomicsService.constructor(
      slickDataSource,
      gcsDAO
    )_

    val bigQueryDAO = new MockGoogleBigQueryDAO
    val submissionCostService = new MockSubmissionCostService("test", "test", bigQueryDAO)
    val execServiceBatchSize = 3
    val maxActiveWorkflowsTotal = 10
    val maxActiveWorkflowsPerUser = 2
    val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      new HttpMethodRepoDAO(
        MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
        MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
        workbenchMetricBaseName = workbenchMetricBaseName),
      executionServiceCluster,
      execServiceBatchSize,
      gcsDAO,
      samDAO,
      notificationDAO,
      userServiceConstructor,
      genomicsServiceConstructor,
      maxActiveWorkflowsTotal,
      maxActiveWorkflowsPerUser,
      workbenchMetricBaseName,
      submissionCostService,
      trackDetailedSubmissionMetrics = true
    )_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  def withTestDataServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }
  }

  def withEmptyTestDataServices[T](testCode: TestApiService => T): T = {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }
  }

  private def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiService) => T) = {
    val apiService = new TestApiService(dataSource, user)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  "WorkspaceService" should "add attribute to entity" in withEmptyTestDataServices { services =>
    assertResult(Some(AttributeString("foo"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo")))).attributes.get(AttributeName.withDefaultNS("newAttribute"))
    }
  }

  it should "update attribute in entity" in withEmptyTestDataServices { services =>
    assertResult(Some(AttributeString("biz"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("foo"), AttributeString("biz")))).attributes.get(AttributeName.withDefaultNS("foo"))
    }
  }

  it should "remove attribute from entity" in withEmptyTestDataServices { services =>
    assertResult(None) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveAttribute(AttributeName.withDefaultNS("foo")))).attributes.get(AttributeName.withDefaultNS("foo"))
    }
  }

  it should "add item to existing list in entity" in withEmptyTestDataServices { services =>
    assertResult(Some(AttributeValueList(attributeList.list :+ AttributeString("new")))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("splat"), AttributeString("new")))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "add item to non-existing list in entity" in withEmptyTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("new"))))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("bob"), AttributeString("new")))).attributes.get(AttributeName.withDefaultNS("bob"))
    }
  }

  it should "throw AttributeUpdateOperationException when trying to create a new empty list by inserting AttributeNull" in withEmptyTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("nolisthere"), AttributeNull))).attributes.get(AttributeName.withDefaultNS("nolisthere"))
    }
  }

  it should "create empty AttributeEntityReferenceList" in withEmptyTestDataServices { services =>
    assertResult(Some(AttributeEntityReferenceEmptyList)) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("emptyRefList")))).attributes.get(AttributeName.withDefaultNS("emptyRefList"))
    }
  }

  it should "not wipe existing AttributeEntityReferenceList when calling CreateAttributeEntityReferenceList" in withEmptyTestDataServices { services =>
    assertResult(Some(s1.attributes(AttributeName.withDefaultNS("refs")))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("refs")))).attributes.get(AttributeName.withDefaultNS("refs"))
    }
  }

  it should "create empty AttributeValueList" in withEmptyTestDataServices { services =>
    assertResult(Some(AttributeValueEmptyList)) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(CreateAttributeValueList(AttributeName.withDefaultNS("emptyValList")))).attributes.get(AttributeName.withDefaultNS("emptyValList"))
    }
  }

  it should "not wipe existing AttributeValueList when calling CreateAttributeValueList" in withEmptyTestDataServices { services =>
    assertResult(Some(s1.attributes(AttributeName.withDefaultNS("splat")))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(CreateAttributeValueList(AttributeName.withDefaultNS("splat")))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "do nothing to existing lists when adding AttributeNull" in withEmptyTestDataServices { services =>
    assertResult(Some(attributeList)) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("splat"), AttributeNull))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "remove item from existing listing entity" in withEmptyTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("b"), AttributeBoolean(true))))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember(AttributeName.withDefaultNS("splat"), AttributeString("a")))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "throw AttributeNotFoundException when removing from a list that does not exist" in withEmptyTestDataServices { services =>
    intercept[AttributeNotFoundException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember(AttributeName.withDefaultNS("bingo"), AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when remove from an attribute that is not a list" in withEmptyTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember(AttributeName.withDefaultNS("foo"), AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when adding to an attribute that is not a list" in withEmptyTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("foo"), AttributeString("a"))))
    }
  }

  it should "apply attribute updates in order to entity" in withEmptyTestDataServices { services =>
    assertResult(Some(AttributeString("splat"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(
        AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo")),
        AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar")),
        AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("splat"))
      )).attributes.get(AttributeName.withDefaultNS("newAttribute"))
    }
  }

  it should "pull entity records for a single entity given no expression" in withTestDataServices { services =>
    withWorkspaceContext(testData.workspace) { ctx =>
      val subRq = SubmissionRequest(testData.agoraMethodConfig.namespace, testData.agoraMethodConfig.name, Some("Sample"), Some("sample1"), None, false)

      //Lookup succeeds
      runAndWait(
        services.workspaceService.withSubmissionEntityRecs(subRq, ctx, Some("Sample"), this) { entityRecs =>
          assertResult(1) {
            entityRecs.size
          }
          assertResult("sample1") {
            entityRecs.get.head.name
          }
          DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
        })

      //Lookup fails because it's not there
      val notFoundExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(
          services.workspaceService.withSubmissionEntityRecs(subRq.copy(entityName = Some("sampel1")), ctx, Some("Sample"), this) { entityRecs =>
            DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
          })
      }
      assertResult(StatusCodes.NotFound) {
        notFoundExc.errorReport.statusCode.get
      }

      //Lookup fails because it doesn't match the method type
      val noMatchyMethodTypeExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(
          services.workspaceService.withSubmissionEntityRecs(subRq, ctx, Some("Pair"), this) { entityRecs =>
            DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
          })
      }
      assertResult(StatusCodes.BadRequest) {
        noMatchyMethodTypeExc.errorReport.statusCode.get
      }
    }
  }

  it should "pull multiple entity records given an entity expression" in withTestDataServices { services =>
    withWorkspaceContext(testData.workspace) { ctx =>
      val subRq = SubmissionRequest(testData.agoraMethodConfig.namespace, testData.agoraMethodConfig.name, Some("SampleSet"), Some("sset1"), Some("this.samples"), false)

      //Lookup succeeds
      runAndWait(
        services.workspaceService.withSubmissionEntityRecs(subRq, ctx, Some("Sample"), this) { entityRecs =>
          assertResult(Set("sample1", "sample2", "sample3")) {
            entityRecs.get.map(_.name).toSet
          }
          DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
        })

      //Lookup fails due to parse failure
      val badExpressionExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(
          services.workspaceService.withSubmissionEntityRecs(subRq.copy(expression = Some("nonsense!")), ctx, Some("Sample"), this) { entityRecs =>
            DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
          })
      }
      assertResult(StatusCodes.BadRequest) {
        badExpressionExc.errorReport.statusCode.get
      }

      //Lookup fails due to no results
      val noResultExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(
          services.workspaceService.withSubmissionEntityRecs(subRq.copy(expression = Some("this.bonk")), ctx, Some("Sample"), this) { entityRecs =>
            DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
          })
      }
      assertResult(StatusCodes.BadRequest) {
        noResultExc.errorReport.statusCode.get
      }

      //Lookup fails because it doesn't match the method type
      val noMatchyMethodTypeExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(
          services.workspaceService.withSubmissionEntityRecs(subRq, ctx, Some("Pair"), this) { entityRecs =>
            DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
          })
      }
      assertResult(StatusCodes.BadRequest) {
        noMatchyMethodTypeExc.errorReport.statusCode.get
      }
    }
  }

  it should "retrieve ACLs" in withTestDataServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    Await.result(services.samDAO.registerUser(userInfo), Duration.Inf)
    Await.result(services.samDAO.registerUser(UserInfo(user.userEmail, OAuth2BearerToken(""), 3600, user.userSubjectId)), Duration.Inf)

    val testWorkspaceName = WorkspaceName("test-project", "test-workspace")
    val workspaceRequest = WorkspaceRequest(testWorkspaceName.namespace, testWorkspaceName.name, Map.empty, None)

    Await.result(services.userService.registerBillingProject(RawlsBillingProjectTransfer("test-project", "fc-bla", userInfo.userEmail.value, "invalid")), Duration.Inf)
    Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val vComplete = Await.result(services.workspaceService.getACL(testWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]
    val (vStatus, vData) = vComplete.response

    assertResult(StatusCodes.OK) {
      vStatus
    }

    //todo: why does it return both access levels??????
    assertResult(WorkspaceACL(Map(
      testData.userOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true, true),
      testData.userOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true)))) {
      vData
    }
  }

  it should "patch ACLs and return updated acls" in withEmptyTestDataServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    val group = RawlsGroup(RawlsGroupName("test"), RawlsGroupEmail("group@whitehouse.gov"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])

    Await.result(services.samDAO.registerUser(userInfo), Duration.Inf)
    Await.result(services.samDAO.registerUser(UserInfo(user.userEmail, OAuth2BearerToken(""), 3600, user.userSubjectId)), Duration.Inf)
    Await.result(services.samDAO.createGroup(WorkbenchGroupName(group.groupEmail.value), userInfo), Duration.Inf)

    val testWorkspaceName = WorkspaceName("test-project", "test-workspace")
    val workspaceRequest = WorkspaceRequest(testWorkspaceName.namespace, testWorkspaceName.name, Map.empty, None)

    Await.result(services.userService.registerBillingProject(RawlsBillingProjectTransfer("test-project", "fc-bla", userInfo.userEmail.value, "invalid")), Duration.Inf)
    Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    //add ACL
    val aclAdd = Set(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.Owner, None), WorkspaceACLUpdate(group.groupEmail.value, WorkspaceAccessLevels.Read, None))
    val aclAddResponse = Await.result(services.workspaceService.updateACL(testWorkspaceName, aclAdd, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromAdd = WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.Owner), WorkspaceACLUpdate(group.groupEmail.value, WorkspaceAccessLevels.Read)), Set.empty, Set.empty)

    assertResult((StatusCodes.OK, responseFromAdd), "Add ACL shouldn't error") {
      aclAddResponse.response
    }

    //check result
    val (_, addedACLs) = Await.result(services.workspaceService.getACL(testWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]].response

    assertResult(WorkspaceACL(Map(
      userInfo.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
      user.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
      group.groupEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false, false))), "Add ACL should actually do so") {
      addedACLs
    }

    //update ACL
    val aclUpdates = Set(WorkspaceACLUpdate(group.groupEmail.value, WorkspaceAccessLevels.Write, None))
    val aclUpdateResponse = Await.result(services.workspaceService.updateACL(testWorkspaceName, aclUpdates, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate = WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate(group.groupEmail.value, WorkspaceAccessLevels.Write)), Set.empty, Set.empty)

    assertResult((StatusCodes.OK, responseFromUpdate), "Update ACL shouldn't error") {
      aclUpdateResponse.response
    }

    //check result
    val (_, updatedACLs) = Await.result(services.workspaceService.getACL(testWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]].response

    assertResult(WorkspaceACL(Map(
      userInfo.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
      user.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
      group.groupEmail.value -> AccessEntry(WorkspaceAccessLevels.Write, false, false, true))), "Update ACL should actually do so") {
      updatedACLs
    }

    //remove ACL
    val aclRemove = Set(WorkspaceACLUpdate(group.groupEmail.value, WorkspaceAccessLevels.NoAccess, None))
    val aclRemoveResponse = Await.result(services.workspaceService.updateACL(testWorkspaceName, aclRemove, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, List[WorkspaceACLUpdateResponseList])]]
    val responseFromRemove = WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate(group.groupEmail.value, WorkspaceAccessLevels.NoAccess)), Set.empty, Set.empty)

    assertResult((StatusCodes.OK, responseFromRemove), "Remove ACL shouldn't error") {
      aclRemoveResponse.response
    }

    //check result
    val (_, removedACLs) = Await.result(services.workspaceService.getACL(testWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]].response

    assertResult(WorkspaceACL(Map(
      userInfo.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
      user.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true))), "Remove ACL should actually do so") {
      removedACLs
    }
  }

  it should "allow can share user to share when there are multiple project owners" in withDefaultTestDatabase { datasource: SlickDataSource =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
//    runAndWait(rawlsUserQuery.createUser(user))

    withServices(datasource, testData.userOwner) { services =>
      //add the owner as an owner on the billing project
      Await.result(services.userService.addUserToBillingProject(RawlsBillingProjectName(testData.workspace.namespace), ProjectAccessUpdate(testData.userOwner.userEmail.value, ProjectRoles.Owner)), Duration.Inf)

      val aCLUpdates = Set(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, Option(true)), WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Write, Option(false), Option(true)))
      val aCLUpdatesResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aCLUpdates, false), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]

      assertResult(StatusCodes.OK) {
        aCLUpdatesResponse.response._1
      }
    }
    withServices(datasource, testData.userReader) { services =>
      val addRead = Set(
        WorkspaceACLUpdate(testData.userProjectOwner.userEmail.value, WorkspaceAccessLevels.ProjectOwner, Option(true), Option(true)),
        WorkspaceACLUpdate(testData.userOwner.userEmail.value, WorkspaceAccessLevels.ProjectOwner, Option(true), Option(true)),
        WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Write, Option(false), Option(true)),
        WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, Option(true), Option(false)),
        WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.Read))

      val addReadResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, addRead, false), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
      assertResult(StatusCodes.OK) {
        addReadResponse.response._1
      }
      assertResult(Set(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.Read))) {
        addReadResponse.response._2.usersUpdated
      }
    }
  }

  it should "return non-existent users during patch ACLs" in withTestDataServices { services =>
    val testWorkspaceName = WorkspaceName("test-project", "test-workspace")
    val workspaceRequest = WorkspaceRequest(testWorkspaceName.namespace, testWorkspaceName.name, Map.empty, None)

    Await.result(services.userService.registerBillingProject(RawlsBillingProjectTransfer("test-project", "fc-bla", userInfo.userEmail.value, "invalid")), Duration.Inf)
    Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val aclUpdates = Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
    val vComplete = Await.result(services.workspaceService.updateACL(testWorkspaceName, aclUpdates, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate = WorkspaceACLUpdateResponseList(Set.empty, Set.empty, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)))

    assertResult((StatusCodes.OK, responseFromUpdate), "Add ACL shouldn't error") {
      vComplete.response
    }

    val aclUpdates2 = Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
    val vComplete2 = Await.result(services.workspaceService.updateACL(testWorkspaceName, aclUpdates2, true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate2 = WorkspaceACLUpdateResponseList(Set.empty, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)), Set.empty)

    assertResult((StatusCodes.OK, responseFromUpdate2), "Add ACL shouldn't error") {
      vComplete2.response
    }

    val aclUpdates3 = Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Read, None))
    val vComplete3 = Await.result(services.workspaceService.updateACL(testWorkspaceName, aclUpdates3, true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate3 = WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Read, None)), Set.empty, Set.empty)

    assertResult((StatusCodes.OK, responseFromUpdate3), "Add ACL shouldn't error") {
      vComplete3.response
    }

    assertResult(true, "Changing an invitees access level should return them in the usersUpdated group") {
      vComplete3.response._2.usersUpdated.size == 1
    }

    assertResult(true, "Changing an invitees access level should not return them in the invitesSent group") {
      vComplete3.response._2.invitesSent.size == 0
    }

    assertResult(true, "Changing an invitees access level shouldn't return them in the usersNotFound group") {
      vComplete3.response._2.usersNotFound.size == 0
    }
  }

  it should "invite a user to a workspace" in withTestDataServices { services =>
    val testWorkspaceName = WorkspaceName("test-project", "test-workspace")
    val workspaceRequest = WorkspaceRequest(testWorkspaceName.namespace, testWorkspaceName.name, Map.empty, None)

    Await.result(services.samDAO.registerUser(userInfo), Duration.Inf)
    Await.result(services.userService.registerBillingProject(RawlsBillingProjectTransfer("test-project", "fc-bla", userInfo.userEmail.value, "invalid")), Duration.Inf)
    Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val vComplete = Await.result(services.workspaceService.updateACL(testWorkspaceName, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)), true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]

    assertResult(StatusCodes.OK, "Invite user shouldn't error") {
      vComplete.response._1
    }

    assert(vComplete.response._2.invitesSent.contains(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)))

    val vComplete3 = Await.result(services.workspaceService.getACL(testWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    assert(vComplete3.response._2.acl.toSeq.contains(("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Owner, true, true, true)))) //TODO?: this is a subtle change in Phase 4: a pending Owner will have implicit canCompute and canShare set to true instead of false

  }

  //TODO: this test really just tests something that's in Sam so it doesn't really belong here anymore?
//  it should "be case insensitive when adding user/group to workspace" in withTestDataServices { services =>
//    val testWorkspaceName = WorkspaceName("test-project", "test-workspace")
//    val workspaceRequest = WorkspaceRequest(testWorkspaceName.namespace, testWorkspaceName.name, Map.empty, None)
//
//    Await.result(services.userService.registerBillingProject(RawlsBillingProjectTransfer("test-project", "fc-bla", userInfo.userEmail.value, "invalid")), Duration.Inf)
//    Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
//
//    val vComplete0 = Await.result(services.workspaceService.updateACL(testWorkspaceName, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)), true), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
//
//    val vComplete1 = Await.result(services.workspaceService.getACL(testWorkspaceName), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]
//
//    assert(vComplete1.response._2.acl.toSeq.contains("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Owner, false, true, true))) //TODO: before this was really just testing that we don't create duplicate invites...shouldn't be necessary anymore
//
//    val vComplete2 = Await.result(services.workspaceService.updateACL(testWorkspaceName, Set(WorkspaceACLUpdate("ObAmA@WhiteHouse.Gov", WorkspaceAccessLevels.Owner, None)), true), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
//
//    assertResult(true, "Entering already added user/group with different capitalization should not include them in the usersNotFound group") {
//      vComplete2.response._2.usersNotFound.isEmpty
//    }
//
//  }

  it should "update an existing workspace invitation to change access levels" in withTestDataServices { services =>
    val testWorkspaceName = WorkspaceName("test-project", "test-workspace")
    val workspaceRequest = WorkspaceRequest(testWorkspaceName.namespace, testWorkspaceName.name, Map.empty, None)

    Await.result(services.samDAO.registerUser(userInfo), Duration.Inf)
    Await.result(services.userService.registerBillingProject(RawlsBillingProjectTransfer("test-project", "fc-bla", userInfo.userEmail.value, "invalid")), Duration.Inf)
    Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val vComplete0 = Await.result(services.workspaceService.updateACL(testWorkspaceName, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)), true), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]

    println(vComplete0)

    val vComplete1 = Await.result(services.workspaceService.getACL(testWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    assert(vComplete1.response._2.acl.toSeq.contains(("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Owner, true, true, true))))

    Await.result(services.workspaceService.updateACL(testWorkspaceName, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Read, None)), true), Duration.Inf)

    val vComplete2 = Await.result(services.workspaceService.getACL(testWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    assert(vComplete2.response._2.acl.toSeq.contains(("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Read, true, false, false))))

  }

  it should "remove a user invite from a workspace" in withTestDataServices { services =>
    val testWorkspaceName = WorkspaceName("test-project", "test-workspace")
    val workspaceRequest = WorkspaceRequest(testWorkspaceName.namespace, testWorkspaceName.name, Map.empty, None)

    Await.result(services.samDAO.registerUser(userInfo), Duration.Inf)
    Await.result(services.userService.registerBillingProject(RawlsBillingProjectTransfer("test-project", "fc-bla", userInfo.userEmail.value, "invalid")), Duration.Inf)
    Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val vComplete = Await.result(services.workspaceService.updateACL(testWorkspaceName, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)), true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]

    assertResult(StatusCodes.OK, "Invite user shouldn't error") {
      vComplete.response._1
    }

    val vComplete2 = Await.result(services.workspaceService.getACL(testWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    assert(vComplete2.response._2.acl.toSeq.contains(("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Owner, true, true, true))))

    val vComplete3 = Await.result(services.workspaceService.updateACL(testWorkspaceName, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.NoAccess, None)), true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]

    assertResult(StatusCodes.OK, "Remove invite shouldn't error") {
      vComplete3.response._1
    }

    val vComplete4 = Await.result(services.workspaceService.getACL(testWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    assert(!vComplete4.response._2.acl.toSeq.contains(("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Owner, true, true, true))))
  }

  ignore should "send notification messages to all users on workspace" in withTestDataServices { services =>
    val vComplete = Await.result(services.workspaceService.sendChangeNotifications(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, String)]]

    assertResult(StatusCodes.OK, "Notification shouldn't error") {
      vComplete.response._1
    }

    assertResult("4", "Number of notifications sent should match number of users on workspace") {
      vComplete.response._2
    }

  }


  ignore should "retrieve catalog permission" in withTestDataServices { services =>
    val vComplete = Await.result(services.workspaceService.getCatalog(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Seq[WorkspaceCatalog])]]
    val (vStatus, vData) = vComplete.response
    assertResult((StatusCodes.OK, Vector.empty)) {
      (vStatus, vData.filter(wc => wc.catalog))
    }
  }

  ignore should "patch Catalog and return updated permissions" in withTestDataServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    val group = RawlsGroup(RawlsGroupName("test"), RawlsGroupEmail("group@whitehouse.gov"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
//    runAndWait(rawlsUserQuery.createUser(user))
//    runAndWait(rawlsGroupQuery.save(group))
//
//    services.gcsDAO.createGoogleGroup(group)

    //add catalog perm
    val catalogUpdateResponse = Await.result(services.workspaceService.updateCatalog(testData.workspace.toWorkspaceName,
      Seq(WorkspaceCatalog("obama@whitehouse.gov", true),WorkspaceCatalog("group@whitehouse.gov", true),WorkspaceCatalog("none@nowhere.gov", true))), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceCatalogUpdateResponseList)]]
    val expectedResponse = WorkspaceCatalogUpdateResponseList(Seq(
      WorkspaceCatalogResponse(user.userSubjectId.value, true),
      WorkspaceCatalogResponse(group.groupName.value,true)),Seq("none@nowhere.gov"))

    assertResult((StatusCodes.OK, expectedResponse)) {
      catalogUpdateResponse.response
    }

    //check result
    val (_, catalogUpdates) = Await.result(services.workspaceService.getCatalog(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Seq[WorkspaceCatalog])]].response

    assertSameElements(Vector(WorkspaceCatalog("obama@whitehouse.gov",true),WorkspaceCatalog("group@whitehouse.gov",true)), catalogUpdates)

    //remove catalog perm
    val catalogRemoveResponse = Await.result(services.workspaceService.updateCatalog(testData.workspace.toWorkspaceName,
      Seq(WorkspaceCatalog("obama@whitehouse.gov", false),WorkspaceCatalog("group@whitehouse.gov", false),WorkspaceCatalog("none@nowhere.gov", false))), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceCatalogUpdateResponseList)]]

    assertResult((StatusCodes.OK, Seq("none@nowhere.gov"))) {
      (catalogRemoveResponse.response._1, catalogRemoveResponse.response._2.emailsNotFound)
    }
    assertSameElements(Seq(WorkspaceCatalogResponse("obamaiscool", false),WorkspaceCatalogResponse("test", false)), catalogRemoveResponse.response._2.usersUpdated)

    //check result
    val (_, catalogRemovals) = Await.result(services.workspaceService.getCatalog(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Seq[WorkspaceCatalog])]].response

    assertResult(Vector.empty){
      catalogRemovals
    }
  }

  it should "lock a workspace with terminated submissions" in withTestDataServices { services =>
    //check workspace is not locked
    assert(!testData.workspaceTerminatedSubmissions.isLocked)

    val rqComplete = Await.result(services.workspaceService.lockWorkspace(testData.workspaceTerminatedSubmissions.toWorkspaceName), Duration.Inf)
        .asInstanceOf[RequestComplete[StatusCode]]

    assertResult(StatusCodes.NoContent) {
      rqComplete.response
    }

    //check workspace is locked
    assert {
      runAndWait(workspaceQuery.findByName(testData.workspaceTerminatedSubmissions.toWorkspaceName)).head.isLocked
    }
  }

  it should "fail to lock a workspace with active submissions" in withTestDataServices { services =>
    //check workspace is not locked
    assert(!testData.workspaceMixedSubmissions.isLocked)

   val except: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
     Await.result(services.workspaceService.lockWorkspace(new WorkspaceName(testData.workspaceMixedSubmissions.namespace, testData.workspaceMixedSubmissions.name)), Duration.Inf)
   }

    assertResult(StatusCodes.Conflict) {
      except.errorReport.statusCode.get
    }

    assert {
      !runAndWait(workspaceQuery.findByName(testData.workspaceMixedSubmissions.toWorkspaceName)).head.isLocked
    }
  }

  it should "delete a workspace with no submissions" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }


  }

  it should "delete a workspace with succeeded submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceSuccessfulSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName4))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig2",Some("Sample"),AgoraMethod("myNamespace","method-a",1),"dsde"),
      MethodConfigurationShort("testConfig1",Some("Sample"),AgoraMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.listActive(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionSuccessful1)) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSuccessfulSubmission.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName4), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName4))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if entities on workspace have been deleted
    assertResult(0) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSuccessfulSubmission.workspaceId)).length.result)
    }
  }

  it should "delete a workspace with failed submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceFailedSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName5))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1",Some("Sample"),AgoraMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.listActive(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionFailed)) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceFailedSubmission.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName5), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName5))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }


    //Check if entities on workspace exist
    assertResult(0) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceFailedSubmission.workspaceId)).length.result)
    }
  }

  it should "delete a workspace with submitted submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceSubmittedSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName6))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1",Some("Sample"),AgoraMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.listActive(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionSubmitted)) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSubmittedSubmission.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName6), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName6))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(0) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSubmittedSubmission.workspaceId)).length.result)
    }
  }

  it should "delete a workspace with mixed submissions" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceMixedSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName7))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1",Some("Sample"),AgoraMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.listActive(SlickWorkspaceContext(testData.workspaceMixedSubmissions)))
    }

    //Check if submissions on workspace exist
    assertResult(2) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions))).length
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceMixedSubmissions.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName7), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName7))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(SlickWorkspaceContext(testData.workspaceMixedSubmissions)))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions)))
    }

    //Check if entities on workspace exist
    assertResult(0) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceMixedSubmissions.workspaceId)).length.result)
    }

  }

  it should "return the correct tags from autocomplete" in withTestDataServices { services =>

    // when no tags, return empty set
    val res1 = Await.result(services.workspaceService.getTags(Some("notag")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector.empty[WorkspaceTag]) {
      res1.response._2
    }

    // add some tags
    Await.result(services.workspaceService.updateWorkspace(testData.wsName,
      Seq(AddListMember(AttributeName.withTagsNS, AttributeString("cancer")),
        AddListMember(AttributeName.withTagsNS, AttributeString("cantaloupe")))), Duration.Inf)

    Await.result(services.workspaceService.updateWorkspace(testData.wsName7,
      Seq(
        AddListMember(AttributeName.withTagsNS, AttributeString("cantaloupe")),
        AddListMember(AttributeName.withTagsNS, AttributeString("buffalo")))), Duration.Inf)

    // searching for tag that doesn't exist should return empty set
    val res2 = Await.result(services.workspaceService.getTags(Some("notag")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[String])]]
    assertResult(Vector.empty[String]) {
      res2.response._2
    }

    // searching for tag that does exist should return the tag (query string case doesn't matter)
    val res3 = Await.result(services.workspaceService.getTags(Some("bUf")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector(WorkspaceTag("buffalo", 1))) {
      res3.response._2
    }

    val res4 = Await.result(services.workspaceService.getTags(Some("aNc")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector(WorkspaceTag("cancer", 1))) {
      res4.response._2
    }

    // searching for multiple tag that does exist should return the tags (query string case doesn't matter)
    // should be sorted by counts of tags
    val res5 = Await.result(services.workspaceService.getTags(Some("cAn")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector(WorkspaceTag("cantaloupe", 2), WorkspaceTag("cancer", 1))) {
      res5.response._2
    }

    // searching for with no query should return all tags
    val res6 = Await.result(services.workspaceService.getTags(None), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector(WorkspaceTag("cantaloupe", 2), WorkspaceTag("buffalo", 1), WorkspaceTag("cancer", 1))) {
      res6.response._2
    }

    // remove tags
    Await.result(services.workspaceService.updateWorkspace(testData.wsName, Seq(RemoveAttribute(AttributeName.withTagsNS))), Duration.Inf)
    Await.result(services.workspaceService.updateWorkspace(testData.wsName7, Seq(RemoveAttribute(AttributeName.withTagsNS))), Duration.Inf)


    // make sure that tags no longer exists
    val res7 = Await.result(services.workspaceService.getTags(Some("aNc")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector.empty[WorkspaceTag]) {
      res7.response._2
    }

  }

  for ((accessLevel, callCount) <- Seq((Write, 1), (Read, 0))) {
    it should s"share billing compute $callCount times when workspace $accessLevel access granted" in withTestDataServices { services =>
      import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
      import WorkspaceACLJsonSupport._
      val email = s"${UUID.randomUUID}@bar.com"
      val results = RequestComplete(StatusCodes.OK, WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate(email, accessLevel)), Set.empty, Set.empty))
//      assertResult(results) {
//        Await.result(services.workspaceService.maybeShareProjectComputePolicy(Future.successful(results), testData.workspace.toWorkspaceName), Duration.Inf)
//      }

      import org.mockserver.model.HttpRequest.request
      mockServer.mockServer.verify(request().withMethod("PUT").withPath(s"/api/resource/${SamResourceTypeNames.billingProject.value}/${testData.workspace.namespace}/policies/${SamBillingProjectPolicyNames.canComputeUser}/memberEmails/$email"), VerificationTimes.exactly(callCount))
    }
  }
}
