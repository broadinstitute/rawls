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
import org.broadinstitute.dsde.rawls.mock.{CustomizableMockSamDAO, MockSamDAO, RemoteServicesMockServer}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.{Read, Write}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor
import org.broadinstitute.dsde.rawls.openam.{MockUserInfoDirectives, MockUserInfoDirectivesWithUser}
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsTestUtils, model}
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleBigQueryDAO
import org.mockserver.verify.VerificationTimes
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.rawls.config.MethodRepoConfig
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try


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
    Some("workflow-collection"),
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

  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit val executionContext: ExecutionContext) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with MockUserInfoDirectivesWithUser {
    private val userInfo1 = UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId)
    lazy val workspaceService: WorkspaceService = workspaceServiceConstructor(userInfo1)
    lazy val userService: UserService = userServiceConstructor(userInfo1)


    def actorRefFactory = system
    val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
    val samDAO = new MockSamDAO(dataSource)
    val gpsDAO = new MockGooglePubSubDAO

    val notificationTopic = "test-notification-topic"
    val notificationDAO = new PubSubNotificationDAO(gpsDAO, notificationTopic)

    val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName), slickDataSource)
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      executionServiceCluster,
      slickDataSource,
      samDAO,
      gcsDAO,
      gcsDAO.getBucketServiceAccountCredential,
      workbenchMetricBaseName = "test"
    ).withDispatcher("submission-monitor-dispatcher"))

    val userServiceConstructor = UserService.constructor(
      slickDataSource,
      gcsDAO,
      notificationDAO,
      samDAO,
      Seq("bigquery.jobUser"),
      "requesterPaysRole"
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
      trackDetailedSubmissionMetrics = true,
      workspaceBucketNamePrefix = "fc-"
    )_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  class TestApiServiceWithCustomSamDAO(dataSource: SlickDataSource, override val user: RawlsUser)(override implicit val executionContext: ExecutionContext) extends TestApiService(dataSource, user) {
    override val samDAO: CustomizableMockSamDAO = new CustomizableMockSamDAO(dataSource)
  }

  def withTestDataServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }
  }

  def withTestDataServicesCustomSam[T](testCode: TestApiServiceWithCustomSamDAO => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServicesCustomSam(dataSource, testData.userOwner)(testCode)
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

  private def withServicesCustomSam[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiServiceWithCustomSamDAO) => T) = {
    val apiService = new TestApiServiceWithCustomSamDAO(dataSource, user)

    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  "WorkspaceService" should "add attribute to entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("foo"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo")))).attributes.get(AttributeName.withDefaultNS("newAttribute"))
    }
  }

  it should "update attribute in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("biz"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("foo"), AttributeString("biz")))).attributes.get(AttributeName.withDefaultNS("foo"))
    }
  }

  it should "remove attribute from entity" in withTestDataServices { services =>
    assertResult(None) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveAttribute(AttributeName.withDefaultNS("foo")))).attributes.get(AttributeName.withDefaultNS("foo"))
    }
  }

  it should "add item to existing list in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(attributeList.list :+ AttributeString("new")))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("splat"), AttributeString("new")))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "add item to non-existing list in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("new"))))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("bob"), AttributeString("new")))).attributes.get(AttributeName.withDefaultNS("bob"))
    }
  }

  it should "throw AttributeUpdateOperationException when trying to create a new empty list by inserting AttributeNull" in withTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("nolisthere"), AttributeNull))).attributes.get(AttributeName.withDefaultNS("nolisthere"))
    }
  }

  it should "create empty AttributeEntityReferenceList" in withTestDataServices { services =>
    assertResult(Some(AttributeEntityReferenceEmptyList)) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("emptyRefList")))).attributes.get(AttributeName.withDefaultNS("emptyRefList"))
    }
  }

  it should "not wipe existing AttributeEntityReferenceList when calling CreateAttributeEntityReferenceList" in withTestDataServices { services =>
    assertResult(Some(s1.attributes(AttributeName.withDefaultNS("refs")))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("refs")))).attributes.get(AttributeName.withDefaultNS("refs"))
    }
  }

  it should "create empty AttributeValueList" in withTestDataServices { services =>
    assertResult(Some(AttributeValueEmptyList)) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(CreateAttributeValueList(AttributeName.withDefaultNS("emptyValList")))).attributes.get(AttributeName.withDefaultNS("emptyValList"))
    }
  }

  it should "not wipe existing AttributeValueList when calling CreateAttributeValueList" in withTestDataServices { services =>
    assertResult(Some(s1.attributes(AttributeName.withDefaultNS("splat")))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(CreateAttributeValueList(AttributeName.withDefaultNS("splat")))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "do nothing to existing lists when adding AttributeNull" in withTestDataServices { services =>
    assertResult(Some(attributeList)) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("splat"), AttributeNull))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "remove item from existing listing entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("b"), AttributeBoolean(true))))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember(AttributeName.withDefaultNS("splat"), AttributeString("a")))).attributes.get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "throw AttributeNotFoundException when removing from a list that does not exist" in withTestDataServices { services =>
    intercept[AttributeNotFoundException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember(AttributeName.withDefaultNS("bingo"), AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when remove from an attribute that is not a list" in withTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember(AttributeName.withDefaultNS("foo"), AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when adding to an attribute that is not a list" in withTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("foo"), AttributeString("a"))))
    }
  }

  it should "apply attribute updates in order to entity" in withTestDataServices { services =>
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

  it should "retrieve ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val vComplete = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]
    val (vStatus, vData) = vComplete.response

    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(WorkspaceACL(Map(
      testData.userOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
      testData.userWriter.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Write, false, false, true),
      testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false, false)))) {
      vData
    }
  }

  private def toUserInfo(user: RawlsUser) = UserInfo(user.userEmail, OAuth2BearerToken(""), 0, user.userSubjectId)
  private def populateWorkspacePolicies(services: TestApiService) = {
    val populateAcl = for {
      _ <- services.samDAO.registerUser(toUserInfo(testData.userOwner))
      _ <- services.samDAO.registerUser(toUserInfo(testData.userWriter))
      _ <- services.samDAO.registerUser(toUserInfo(testData.userReader))

      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.owner,
        SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set(SamWorkspaceActions.own, SamWorkspaceActions.write, SamWorkspaceActions.read), Set(SamWorkspaceRoles.owner)), userInfo)

      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.writer,
        SamPolicy(Set(WorkbenchEmail(testData.userWriter.userEmail.value)), Set(SamWorkspaceActions.write, SamWorkspaceActions.read), Set(SamWorkspaceRoles.writer)), userInfo)

      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.reader,
        SamPolicy(Set(WorkbenchEmail(testData.userReader.userEmail.value)), Set(SamWorkspaceActions.read), Set(SamWorkspaceRoles.reader)), userInfo)

      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCatalog,
        SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set(SamWorkspaceActions.catalog), Set.empty), userInfo)
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.shareReader,
        SamPolicy(Set.empty, Set.empty, Set.empty), userInfo)
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.shareWriter,
        SamPolicy(Set.empty, Set.empty, Set.empty), userInfo)
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCompute,
        SamPolicy(Set(WorkbenchEmail(testData.userWriter.userEmail.value)), Set.empty, Set.empty), userInfo)
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.projectOwner,
        SamPolicy(Set.empty, Set.empty, Set.empty), userInfo)
    } yield ()

    Await.result(populateAcl, Duration.Inf)
  }

  it should "add ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val user1 = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    val user2 = RawlsUser(RawlsUserSubjectId("obamaiscool2"), RawlsUserEmail("obama2@whitehouse.gov"))

    Await.result(for {
      _ <- services.samDAO.registerUser(toUserInfo(user1))
      _ <- services.samDAO.registerUser(toUserInfo(user2))
    } yield (), Duration.Inf)

    //add ACL
    val aclAdd = Set(WorkspaceACLUpdate(user1.userEmail.value, WorkspaceAccessLevels.Owner, None), WorkspaceACLUpdate(user2.userEmail.value, WorkspaceAccessLevels.Read, Option(true)))
    val aclAddResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclAdd, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromAdd = WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate(user1.userEmail.value, WorkspaceAccessLevels.Owner, Some(true), Some(true)), WorkspaceACLUpdate(user2.userEmail.value, WorkspaceAccessLevels.Read, Some(true), Some(false))), Set.empty, Set.empty)

    assertResult((StatusCodes.OK, responseFromAdd), aclAddResponse.response.toString()) {
      aclAddResponse.response
    }

    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.owner, user1.userEmail.value),
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.shareReader, user2.userEmail.value),
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.reader, user2.userEmail.value)
    )
    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq.empty
  }

  it should "update ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    //update ACL
    val aclUpdates = Set(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Write, None))
    val aclUpdateResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate = WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Write, Some(false), Some(true))), Set.empty, Set.empty)

    assertResult((StatusCodes.OK, responseFromUpdate), "Update ACL shouldn't error") {
      aclUpdateResponse.response
    }

    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.reader, testData.userReader.userEmail.value)
    )
    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.writer, testData.userReader.userEmail.value),
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCompute, testData.userReader.userEmail.value),
      (SamResourceTypeNames.billingProject, testData.workspace.namespace, SamBillingProjectPolicyNames.canComputeUser, testData.userReader.userEmail.value)
    )
  }

  it should "remove ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    //remove ACL
    val aclRemove = Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.NoAccess, None))
    val aclRemoveResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclRemove, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, List[WorkspaceACLUpdateResponseList])]]
    val responseFromRemove = WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.NoAccess, Some(false), Some(false))), Set.empty, Set.empty)

    assertResult((StatusCodes.OK, responseFromRemove), "Remove ACL shouldn't error") {
      aclRemoveResponse.response
    }

    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCompute, testData.userWriter.userEmail.value),
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.writer, testData.userWriter.userEmail.value)
    )
    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq.empty
  }

  it should "return non-existent users during patch ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val aclUpdates = Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
    val vComplete = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate = WorkspaceACLUpdateResponseList(Set.empty, Set.empty, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)))

    assertResult((StatusCodes.OK, responseFromUpdate), "Add ACL shouldn't error") {
      vComplete.response
    }
  }

  it should "invite a user to a workspace" in withTestDataServicesCustomSam { services =>
    val aclUpdates2 = Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
    val vComplete2 = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates2, true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate2 = WorkspaceACLUpdateResponseList(Set.empty, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, Some(true), Some(true))), Set.empty)

    assertResult((StatusCodes.OK, responseFromUpdate2), "Add ACL shouldn't error") {
      vComplete2.response
    }

    services.samDAO.invitedUsers.keySet should contain theSameElementsAs Set("obama@whitehouse.gov")
  }

  it should "retrieve catalog permission" in withTestDataServicesCustomSam { services =>
    val populateAcl = for {
      _ <- services.samDAO.registerUser(toUserInfo(testData.userOwner))

      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCatalog,
        SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set(SamWorkspaceActions.catalog), Set.empty), userInfo)
    } yield ()

    Await.result(populateAcl, Duration.Inf)

    val vComplete = Await.result(services.workspaceService.getCatalog(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Set[WorkspaceCatalog])]]
    val (vStatus, vData) = vComplete.response
    assertResult((StatusCodes.OK, Set.empty)) {
      (vStatus, vData.filter(wc => wc.catalog))
    }
  }

  it should "add catalog permissions" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val user1 = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))

    Await.result(for {
      _ <- services.samDAO.registerUser(toUserInfo(user1))
    } yield (), Duration.Inf)

    //add catalog perm
    val catalogUpdateResponse = Await.result(services.workspaceService.updateCatalog(testData.workspace.toWorkspaceName,
      Seq(WorkspaceCatalog("obama@whitehouse.gov", true))), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceCatalogUpdateResponseList)]]
    val expectedResponse = WorkspaceCatalogUpdateResponseList(Seq(WorkspaceCatalogResponse("obama@whitehouse.gov", true)), Seq.empty)

    assertResult((StatusCodes.OK, expectedResponse)) {
      catalogUpdateResponse.response
    }

    //check result
    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCatalog, user1.userEmail.value)
    )
  }

  it should "remove catalog permissions" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    //remove catalog perm
    val catalogRemoveResponse = Await.result(services.workspaceService.updateCatalog(testData.workspace.toWorkspaceName,
      Seq(WorkspaceCatalog(testData.userOwner.userEmail.value, false))), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceCatalogUpdateResponseList)]]

    val expectedResponse = WorkspaceCatalogUpdateResponseList(Seq(WorkspaceCatalogResponse(testData.userOwner.userEmail.value, false)), Seq.empty)

    assertResult((StatusCodes.OK, expectedResponse)) {
      catalogRemoveResponse.response
    }

    //check result
    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCatalog, testData.userOwner.userEmail.value)
    )
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

  for ((policyName, shouldShare) <- Seq((SamWorkspacePolicyNames.writer, false), (SamWorkspacePolicyNames.canCompute, true), (SamWorkspacePolicyNames.reader, false))) {
    it should s"${if (!shouldShare) "not " else ""}share billing compute when workspace $policyName access granted" in withTestDataServicesCustomSam { services =>
      val email = s"${UUID.randomUUID}@bar.com"
      val results = Set((policyName, email))
      Await.result(services.workspaceService.maybeShareProjectComputePolicy(results, testData.workspace.toWorkspaceName), Duration.Inf)

      val expectedPolicyEntry = (SamResourceTypeNames.billingProject, testData.workspace.namespace, SamBillingProjectPolicyNames.canComputeUser, email)
      if(shouldShare) {
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs(Set(expectedPolicyEntry))
      } else {
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs(Set.empty)
      }
    }
  }

  val aclTestUser = UserInfo(RawlsUserEmail("acl-test-user"), OAuth2BearerToken(""), 0, RawlsUserSubjectId("acl-test-user-subject-id"))

  def allWorkspaceAclUpdatePermutations(emailString: String): Seq[WorkspaceACLUpdate] = for {
    accessLevel <- WorkspaceAccessLevels.all
    canShare <- Set(Some(true), Some(false), None)
    canCompute <- Set(Some(true), Some(false), None)
  } yield WorkspaceACLUpdate(emailString, accessLevel, canShare, canCompute)

  def expectedPolicies(aclUpdate: WorkspaceACLUpdate): Either[StatusCode, Set[(SamResourceTypeName, SamResourcePolicyName)]] = {
    aclUpdate match {
      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.ProjectOwner, _, _) => Left(StatusCodes.BadRequest)
      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Owner, _, _) => Right(Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.owner))

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Write, canShare, canCompute) =>
        val canSharePolicy = canShare match {
          case None | Some(false) => Set.empty
          case Some(true) => Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.shareWriter)
        }
        val canComputePolicy = canCompute match {
          case None | Some(true) => Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.canCompute, SamResourceTypeNames.billingProject -> SamBillingProjectPolicyNames.canComputeUser)
          case Some(false) => Set.empty
        }
        Right(Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.writer) ++ canSharePolicy ++ canComputePolicy)

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Read, canShare, canCompute) =>
        if (canCompute.contains(true)) {
          Left(StatusCodes.BadRequest)
        } else {
          val canSharePolicy = canShare match {
            case None | Some(false) => Set.empty
            case Some(true) => Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.shareReader)
          }
          Right(Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.reader) ++ canSharePolicy)
        }

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.NoAccess, _, _) => Right(Set.empty)
    }
  }

  for (aclUpdate <- allWorkspaceAclUpdatePermutations(aclTestUser.userEmail.value)) {
    it should s"add correct policies for $aclUpdate" in withTestDataServicesCustomSam { services =>
      Await.result(services.samDAO.registerUser(aclTestUser), Duration.Inf)
      populateWorkspacePolicies(services)

      val result = Try {
        Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false), Duration.Inf)
      }

      (expectedPolicies(aclUpdate), result) match {
        case (Left(statusCode), util.Failure(exception: RawlsExceptionWithErrorReport)) => assertResult(Some(statusCode), result.toString) {
          exception.errorReport.statusCode
        }

        case (Right(policies), util.Success(_)) =>
          val expectedAdds = policies.map {
            case (SamResourceTypeNames.workspace, policyName) => (SamResourceTypeNames.workspace, testData.workspace.workspaceId, policyName, aclTestUser.userEmail.value)
            case (SamResourceTypeNames.billingProject, policyName) => (SamResourceTypeNames.billingProject, testData.workspace.namespace, policyName, aclTestUser.userEmail.value)
            case _ => throw new Exception("make the compiler happy")
          }

          withClue(result.toString) {
            services.samDAO.callsToAddToPolicy should contain theSameElementsAs expectedAdds
            services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
          }

        case (_, r) => fail(r.toString)
      }
    }
  }

  it should s"add correct policies for group" in withTestDataServicesCustomSam { services =>
    // setting the email to None is what a group looks like
    services.samDAO.userEmails.put(aclTestUser.userEmail.value, None)
    populateWorkspacePolicies(services)

    val aclUpdate = WorkspaceACLUpdate(aclTestUser.userEmail.value, WorkspaceAccessLevels.Write)

    val result = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false), Duration.Inf)

    withClue(result.toString) {
      services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set(
        (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.writer, aclTestUser.userEmail.value),
        (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCompute, aclTestUser.userEmail.value),
        (SamResourceTypeNames.billingProject, testData.workspace.namespace, SamBillingProjectPolicyNames.canComputeUser, aclTestUser.userEmail.value)
      )
      services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
    }
  }


  def addEmailToPolicy(services: TestApiServiceWithCustomSamDAO, policyName: SamResourcePolicyName, email: String) = {
    val policy = services.samDAO.policies((SamResourceTypeNames.workspace, testData.workspace.workspaceId))(policyName)
    val updateMembers = policy.policy.memberEmails + WorkbenchEmail(email)
    val updatedPolicy = policy.copy(policy = policy.policy.copy(memberEmails = updateMembers))
    services.samDAO.policies((SamResourceTypeNames.workspace, testData.workspace.workspaceId)).put(policyName, updatedPolicy)
  }

  val testPolicyNames = Set(SamWorkspacePolicyNames.canCompute, SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.reader, SamWorkspacePolicyNames.owner, SamWorkspacePolicyNames.projectOwner, SamWorkspacePolicyNames.shareReader, SamWorkspacePolicyNames.shareWriter)
  for(testPolicyName1 <- testPolicyNames; testPolicyName2 <- testPolicyNames if testPolicyName1 != testPolicyName2 && !(testPolicyName1 == SamWorkspacePolicyNames.shareReader && testPolicyName2 == SamWorkspacePolicyNames.shareWriter) && !(testPolicyName1 == SamWorkspacePolicyNames.shareWriter && testPolicyName2 == SamWorkspacePolicyNames.shareReader)) {
    it should s"remove $testPolicyName1 and $testPolicyName2" in withTestDataServicesCustomSam { services =>
      Await.result(services.samDAO.registerUser(aclTestUser), Duration.Inf)
      populateWorkspacePolicies(services)

      addEmailToPolicy(services, testPolicyName1, aclTestUser.userEmail.value)
      addEmailToPolicy(services, testPolicyName2, aclTestUser.userEmail.value)

      val result = Try {
        Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Set(WorkspaceACLUpdate(aclTestUser.userEmail.value, WorkspaceAccessLevels.NoAccess)), inviteUsersNotFound = false), Duration.Inf)
      }

      if (testPolicyName1 == SamWorkspacePolicyNames.projectOwner || testPolicyName2 == SamWorkspacePolicyNames.projectOwner) {
        val error = intercept[RawlsExceptionWithErrorReport] {
          result.get
        }
        assertResult(Some(StatusCodes.BadRequest), result.toString) {
          error.errorReport.statusCode
        }
      } else {
        assert(result.isSuccess, result.toString)
        services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set(
          (SamResourceTypeNames.workspace, testData.workspace.workspaceId, testPolicyName1, aclTestUser.userEmail.value),
          (SamResourceTypeNames.workspace, testData.workspace.workspaceId, testPolicyName2, aclTestUser.userEmail.value)
        )
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set.empty
      }

    }
  }

  for(testPolicyName <- Set(SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.reader, SamWorkspacePolicyNames.owner); aclUpdate <- Set(WorkspaceAccessLevels.Read ,WorkspaceAccessLevels.Write, WorkspaceAccessLevels.Owner).map(l => WorkspaceACLUpdate(aclTestUser.userEmail.value, l, canCompute = Some(false)))) {
    it should s"change $testPolicyName to $aclUpdate" in withTestDataServicesCustomSam { services =>
      Await.result(services.samDAO.registerUser(aclTestUser), Duration.Inf)
      populateWorkspacePolicies(services)

      addEmailToPolicy(services, testPolicyName, aclTestUser.userEmail.value)

      val result = Try {
        Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false), Duration.Inf)
      }

      assert(result.isSuccess, result.toString)

      if (aclUpdate.accessLevel.toPolicyName.contains(testPolicyName.value)) {
        services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set.empty
      } else {
        services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set((SamResourceTypeNames.workspace, testData.workspace.workspaceId, testPolicyName, aclTestUser.userEmail.value))
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set((SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamResourcePolicyName(aclUpdate.accessLevel.toPolicyName.get), aclTestUser.userEmail.value))
      }
    }
  }

}
