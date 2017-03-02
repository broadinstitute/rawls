package org.broadinstitute.dsde.rawls.workspace

import java.util.concurrent.TimeUnit
import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.slick._
import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsTestUtils}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.{GoogleGroupSyncMonitorSupervisor, BucketDeletionMonitor}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import org.broadinstitute.dsde.rawls.webservice._
import AttributeUpdateOperations._
import org.scalatest.{FlatSpec, Matchers}
import spray.http.{StatusCode, StatusCodes}
import spray.testkit.ScalatestRouteTest

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.duration._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent


class WorkspaceServiceSpec extends FlatSpec with ScalatestRouteTest with Matchers with TestDriverComponent with RawlsTestUtils {
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
    None,
    "aWorkspaceId",
    "aBucket",
    currentTime(),
    currentTime(),
    "test",
    Map.empty,
    Map.empty,
    Map.empty
  )

  case class TestApiService(dataSource: SlickDataSource)(implicit val executionContext: ExecutionContext) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with MockUserInfoDirectives {
    lazy val workspaceService: WorkspaceService = TestActorRef(WorkspaceService.props(workspaceServiceConstructor, userInfo)).underlyingActor
    lazy val userService: UserService = TestActorRef(UserService.props(userServiceConstructor, userInfo)).underlyingActor
    val mockServer = RemoteServicesMockServer()


    def actorRefFactory = system
    val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
    val gpsDAO = new MockGooglePubSubDAO

    val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, mockServer.defaultWorkflowSubmissionTimeout), slickDataSource)
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      executionServiceCluster,
      slickDataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-ws-submission-supervisor")
    val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(slickDataSource, gcsDAO))

    val directoryDAO = new MockUserDirectoryDAO

    val userServiceConstructor = UserService.constructor(
      slickDataSource,
      gcsDAO,
      directoryDAO,
      gpsDAO,
      "test-topic-name"
    )_

    val googleGroupSyncMonitorSupervisor = system.actorOf(GoogleGroupSyncMonitorSupervisor.props(500 milliseconds, 0 seconds, gpsDAO, "test-topic-name", "test-sub-name", 1, userServiceConstructor))

    val execServiceBatchSize = 3
    val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      new HttpMethodRepoDAO(mockServer.mockServerBaseUrl),
      executionServiceCluster,
      execServiceBatchSize,
      gcsDAO,
      submissionSupervisor,
      bucketDeletionMonitor,
      userServiceConstructor
    )_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
      bucketDeletionMonitor ! PoisonPill
      googleGroupSyncMonitorSupervisor ! PoisonPill
    }
  }

  def withTestDataServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      val apiService = new TestApiService(dataSource)
      try {
        testData.createWorkspaceGoogleGroups(apiService.gcsDAO)
        testCode(apiService)
      } finally {
        apiService.cleanupSupervisor
      }
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

  it should "return conflicts during an entity copy" in {
    val s1 = Entity("s1", "samples", Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"), AttributeName.withDefaultNS("bar") -> AttributeNumber(3)))
    val s2 = Entity("s3", "child", Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"), AttributeName.withDefaultNS("bar") -> AttributeNumber(3)))
    //println("hello " + workspaceService.getCopyConflicts(wsns, wsname, Seq(s1, s2)).size)
    //still needs to be implemented fully
    assertResult(true) {
      true
    }
  }


  it should "pull entity records for a single entity given no expression" in withTestDataServices { services =>
    withWorkspaceContext(testData.workspace) { ctx =>
      val subRq = SubmissionRequest(testData.methodConfig.namespace, testData.methodConfig.name, "Sample", "sample1", None)

      //Lookup succeeds
      runAndWait(
        services.workspaceService.withSubmissionEntityRecs(subRq, ctx, "Sample", this) { entityRecs =>
          assertResult(1) {
            entityRecs.size
          }
          assertResult("sample1") {
            entityRecs.head.name
          }
          DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
        })

      //Lookup fails because it's not there
      val notFoundExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(
          services.workspaceService.withSubmissionEntityRecs(subRq.copy(entityName = "sampel1"), ctx, "Sample", this) { entityRecs =>
            DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
          })
      }
      assertResult(StatusCodes.NotFound) {
        notFoundExc.errorReport.statusCode.get
      }

      //Lookup fails because it doesn't match the method type
      val noMatchyMethodTypeExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(
          services.workspaceService.withSubmissionEntityRecs(subRq, ctx, "Pair", this) { entityRecs =>
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
      val subRq = SubmissionRequest(testData.methodConfig.namespace, testData.methodConfig.name, "SampleSet", "sset1", Some("this.samples"))

      //Lookup succeeds
      runAndWait(
        services.workspaceService.withSubmissionEntityRecs(subRq, ctx, "Sample", this) { entityRecs =>
          assertResult(Set("sample1", "sample2", "sample3")) {
            entityRecs.map(_.name).toSet
          }
          DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
        })

      //Lookup fails due to parse failure
      val badExpressionExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(
          services.workspaceService.withSubmissionEntityRecs(subRq.copy(expression = Some("nonsense!")), ctx, "Sample", this) { entityRecs =>
            DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
          })
      }
      assertResult(StatusCodes.BadRequest) {
        badExpressionExc.errorReport.statusCode.get
      }

      //Lookup fails due to no results
      val noResultExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(
          services.workspaceService.withSubmissionEntityRecs(subRq.copy(expression = Some("this.bonk")), ctx, "Sample", this) { entityRecs =>
            DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
          })
      }
      assertResult(StatusCodes.BadRequest) {
        noResultExc.errorReport.statusCode.get
      }

      //Lookup fails because it doesn't match the method type
      val noMatchyMethodTypeExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(
          services.workspaceService.withSubmissionEntityRecs(subRq, ctx, "Pair", this) { entityRecs =>
            DBIO.successful(RequestComplete(StatusCodes.Created)) //has to be here because inner function needs to return a RqComplete
          })
      }
      assertResult(StatusCodes.BadRequest) {
        noMatchyMethodTypeExc.errorReport.statusCode.get
      }
    }
  }

  it should "validate method config expressions" in withTestDataServices { services =>
    val shouldBeValid = services.workspaceService.validateMCExpressions(testData.methodConfigValidExprs, this)
    assertResult(6) { shouldBeValid.validInputs.size }
    assertResult(4) { shouldBeValid.validOutputs.size }
    assertResult(0) { shouldBeValid.invalidInputs.size }
    assertResult(0) { shouldBeValid.invalidOutputs.size }

    val shouldBeInvalid = services.workspaceService.validateMCExpressions(testData.methodConfigInvalidExprs, this)
    assertResult(1) { shouldBeInvalid.validInputs.size }
    assertResult(0) { shouldBeInvalid.validOutputs.size }
    assertResult(1) { shouldBeInvalid.invalidInputs.size }
    assertResult(4) { shouldBeInvalid.invalidOutputs.size }
  }

  it should "retrieve ACLs" in withTestDataServices { services =>
    //Really annoying setup. I'm trying to avoid using the patch function to test get, so I have to poke
    //ACLs into the workspace manually.
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    val group = RawlsGroup(RawlsGroupName("test"), RawlsGroupEmail("group@whitehouse.gov"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])

    runAndWait(rawlsUserQuery.save(user))
    runAndWait(rawlsGroupQuery.save(group))

    val ownerGroupRef = testData.workspace.accessLevels(WorkspaceAccessLevels.Owner)
    val theOwnerGroup = runAndWait(rawlsGroupQuery.load(ownerGroupRef)).get
    val replacementOwnerGroup = theOwnerGroup.copy(users = theOwnerGroup.users + user, subGroups = theOwnerGroup.subGroups + group)
    runAndWait(rawlsGroupQuery.save(replacementOwnerGroup))

    val vComplete = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]
    val (vStatus, vData) = vComplete.response

    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(WorkspaceACL(Map(
      testData.userProjectOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true),
      testData.userOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true),
      "obama@whitehouse.gov" -> AccessEntry(WorkspaceAccessLevels.Owner, false, true),
      "group@whitehouse.gov" -> AccessEntry(WorkspaceAccessLevels.Owner, false, true),
      testData.userWriter.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Write, false, false),
      testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false)))) {
      vData
    }
  }

  it should "patch ACLs and return updated acls" in withTestDataServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    val group = RawlsGroup(RawlsGroupName("test"), RawlsGroupEmail("group@whitehouse.gov"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    runAndWait(rawlsUserQuery.save(user))
    runAndWait(rawlsGroupQuery.save(group))

    services.gcsDAO.createGoogleGroup(group)

    //add ACL
    val aclAdd = Seq(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.Owner, None), WorkspaceACLUpdate(group.groupEmail.value, WorkspaceAccessLevels.Read, None))
    val aclAddResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclAdd, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromAdd = WorkspaceACLUpdateResponseList(Seq(WorkspaceACLUpdateResponse(user.userSubjectId.value, WorkspaceAccessLevels.Owner), WorkspaceACLUpdateResponse(group.groupName.value, WorkspaceAccessLevels.Read)), Seq.empty, Seq.empty, Seq.empty)

    assertResult((StatusCodes.OK, responseFromAdd), "Add ACL shouldn't error") {
      aclAddResponse.response
    }

    //check result
    val (_, addedACLs) = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
    .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]].response

    assertResult(WorkspaceACL(Map(
      testData.userProjectOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true),
      testData.userOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true),
      user.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true),
      testData.userWriter.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Write, false, false),
      testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false),
      group.groupEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false))), "Add ACL should actually do so") {
      addedACLs
    }

    //update ACL
    val aclUpdates = Seq(WorkspaceACLUpdate(group.groupEmail.value, WorkspaceAccessLevels.Write, None))
    val aclUpdateResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate = WorkspaceACLUpdateResponseList(Seq(WorkspaceACLUpdateResponse(group.groupName.value, WorkspaceAccessLevels.Write)), Seq.empty, Seq.empty, Seq.empty)

    assertResult((StatusCodes.OK, responseFromUpdate), "Update ACL shouldn't error") {
      aclUpdateResponse.response
    }

    //check result
    val (_, updatedACLs) = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]].response

    assertResult(WorkspaceACL(Map(
      testData.userProjectOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true),
      testData.userOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true),
      user.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true),
      testData.userWriter.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Write, false, false),
      testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false),
      group.groupEmail.value -> AccessEntry(WorkspaceAccessLevels.Write, false, false))), "Update ACL should actually do so") {
      updatedACLs
    }

    //remove ACL
    val aclRemove = Seq(WorkspaceACLUpdate(group.groupEmail.value, WorkspaceAccessLevels.NoAccess, None))
    val aclRemoveResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclRemove, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, List[WorkspaceACLUpdateResponseList])]]
    val responseFromRemove = WorkspaceACLUpdateResponseList(Seq(WorkspaceACLUpdateResponse(group.groupName.value, WorkspaceAccessLevels.NoAccess)), Seq.empty, Seq.empty, Seq.empty)

    assertResult((StatusCodes.OK, responseFromRemove), "Remove ACL shouldn't error") {
      aclRemoveResponse.response
    }

    //check result
    val (_, removedACLs) = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]].response

    assertResult(WorkspaceACL(Map(
      testData.userProjectOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true),
      testData.userOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true),
      user.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true),
      testData.userWriter.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Write, false, false),
      testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false))), "Remove ACL should actually do so") {
      removedACLs
    }
  }

  it should "patch realm ACLs when the owner is also a project owner" in withTestDataServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    runAndWait(rawlsUserQuery.save(user))

    //add the owner as an owner on the billing project
    Await.result(services.userService.addUserToBillingProject(RawlsBillingProjectName(testData.controlledWorkspace.namespace), ProjectAccessUpdate("owner-access", ProjectRoles.Owner)), Duration.Inf)

    //add dbGapAuthorizedUsers group to ACL
    val aclAdd = Seq(WorkspaceACLUpdate(testData.dbGapAuthorizedUsersGroup.groupEmail.value, WorkspaceAccessLevels.Read, None))
    val aclAddResponse = Await.result(services.workspaceService.updateACL(testData.controlledWorkspace.toWorkspaceName, aclAdd, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromAdd = Seq(WorkspaceACLUpdateResponse(testData.dbGapAuthorizedUsersGroup.groupName.value, WorkspaceAccessLevels.Read))

    assertResult(responseFromAdd, "Add ACL shouldn't error") {
      aclAddResponse.response._2.usersUpdated
    }

    //add a member of dbGapAuthorizedUsers as a writer on the workspace
    val aclAdd2 = Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Write, None))
    val aclAddResponse2 = Await.result(services.workspaceService.updateACL(testData.controlledWorkspace.toWorkspaceName, aclAdd2, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromAdd2 = Seq(WorkspaceACLUpdateResponse(testData.userReader.userSubjectId.value, WorkspaceAccessLevels.Write))

    assertResult(responseFromAdd2, "Add ACL shouldn't error") {
      aclAddResponse2.response._2.usersUpdated
    }

    val getACLResponse = Await.result(services.workspaceService.getACL(testData.controlledWorkspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    //realm ACLs should be maintained and this should return successfully
    assertResult(StatusCodes.OK) {
      getACLResponse.response._1
    }
  }

  it should "return non-existent users during patch ACLs" in withTestDataServices { services =>
    val aclUpdates = Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
    val vComplete = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate = WorkspaceACLUpdateResponseList(Seq.empty, Seq.empty, Seq.empty, Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)))

    assertResult((StatusCodes.OK, responseFromUpdate), "Add ACL shouldn't error") {
      vComplete.response
    }

    val aclUpdates2 = Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
    val vComplete2 = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates2, true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate2 = WorkspaceACLUpdateResponseList(Seq.empty, Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)), Seq.empty, Seq.empty)

    assertResult((StatusCodes.OK, responseFromUpdate2), "Add ACL shouldn't error") {
      vComplete2.response
    }

    val aclUpdates3 = Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Read, None))
    val vComplete3 = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates3, true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate3 = WorkspaceACLUpdateResponseList(Seq.empty, Seq.empty, Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Read, None)), Seq.empty)

    assertResult((StatusCodes.OK, responseFromUpdate3), "Add ACL shouldn't error") {
      vComplete3.response
    }

    assertResult(true, "Changing an invitees access level should return them in the invitesUpdated group") {
      vComplete3.response._2.invitesUpdated.size == 1
    }

    assertResult(true, "Changing an invitees access level shouldn't return them in the usersNotFound group") {
      vComplete3.response._2.usersNotFound.size == 0
    }
  }

  it should "invite a user to a workspace" in withTestDataServices { services =>
    val vComplete = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)), true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]

    assertResult(StatusCodes.OK, "Invite user shouldn't error") {
      vComplete.response._1
    }

    assert(vComplete.response._2.invitesSent.contains(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)))

    val vComplete3 = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    assert(vComplete3.response._2.acl.toSeq.contains(("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Owner, true, false))))

  }

  it should "update an existing workspace invitation to change access levels" in withTestDataServices { services =>
    val vComplete0 = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)), true), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]

    val vComplete1 = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    assert(vComplete1.response._2.acl.toSeq.contains(("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Owner, true, false))))

    Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Read, None)), true), Duration.Inf)

    val vComplete2 = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    assert(vComplete2.response._2.acl.toSeq.contains(("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Read, true, false))))

  }

  it should "remove a user invite from a workspace" in withTestDataServices { services =>
    val vComplete = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)), true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]

    assertResult(StatusCodes.OK, "Invite user shouldn't error") {
      vComplete.response._1
    }

    val vComplete2 = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    assert(vComplete2.response._2.acl.toSeq.contains(("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Owner, true, false))))

    val vComplete3 = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.NoAccess, None)), true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]

    assertResult(StatusCodes.OK, "Remove invite shouldn't error") {
      vComplete3.response._1
    }

    val vComplete4 = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]

    assert(!vComplete4.response._2.acl.toSeq.contains(("obama@whitehouse.gov", AccessEntry(WorkspaceAccessLevels.Owner, true, false))))
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
    assertResult(Vector(MethodConfigurationShort("testConfig2","Sample",MethodRepoMethod("myNamespace","method-a",1),"dsde"), MethodConfigurationShort("testConfig1","Sample",MethodRepoMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if access levels on workspace exist
    assertResult(Set((testData.userOwner.userEmail.value, WorkspaceAccessLevels.Owner, false), (testData.userOwner.userEmail.value, WorkspaceAccessLevels.ProjectOwner, false), ("project-owner-access", WorkspaceAccessLevels.ProjectOwner, false), (testData.userReader.userEmail.value,WorkspaceAccessLevels.Read, false), (testData.userWriter.userEmail.value,WorkspaceAccessLevels.Write, false))) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission))).toSet
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionSuccessful1)) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findEntityByWorkspace(UUID.fromString(testData.workspaceSuccessfulSubmission.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName4), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName4))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if access levels have been deleted
    assertResult(Vector()) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if entities on workspace have been deleted
    assertResult(0) {
      runAndWait(entityQuery.findEntityByWorkspace(UUID.fromString(testData.workspaceSuccessfulSubmission.workspaceId)).length.result)
    }
  }

  it should "delete a workspace with failed submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceFailedSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName5))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1","Sample",MethodRepoMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }

    //Check if access levels on workspace exist
    assertResult(Set((testData.userOwner.userEmail.value, WorkspaceAccessLevels.Owner, false), (testData.userOwner.userEmail.value, WorkspaceAccessLevels.ProjectOwner, false), ("project-owner-access", WorkspaceAccessLevels.ProjectOwner, false), (testData.userReader.userEmail.value,WorkspaceAccessLevels.Read, false), (testData.userWriter.userEmail.value,WorkspaceAccessLevels.Write, false))) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceFailedSubmission))).toSet
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionFailed)) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findEntityByWorkspace(UUID.fromString(testData.workspaceFailedSubmission.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName5), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName5))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }

    //Check if access levels have been deleted
    assertResult(Vector()) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }


    //Check if entities on workspace exist
    assertResult(0) {
      runAndWait(entityQuery.findEntityByWorkspace(UUID.fromString(testData.workspaceFailedSubmission.workspaceId)).length.result)
    }
  }

  it should "delete a workspace with submitted submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceSubmittedSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName6))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1","Sample",MethodRepoMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if access levels on workspace exist
    assertResult(Set((testData.userOwner.userEmail.value, WorkspaceAccessLevels.Owner, false), (testData.userOwner.userEmail.value, WorkspaceAccessLevels.ProjectOwner, false), ("project-owner-access", WorkspaceAccessLevels.ProjectOwner, false), (testData.userReader.userEmail.value,WorkspaceAccessLevels.Read, false), (testData.userWriter.userEmail.value,WorkspaceAccessLevels.Write, false))) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceSubmittedSubmission))).toSet
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionSubmitted)) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findEntityByWorkspace(UUID.fromString(testData.workspaceSubmittedSubmission.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName6), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName6))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if access levels have been deleted
    assertResult(Vector()) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(0) {
      runAndWait(entityQuery.findEntityByWorkspace(UUID.fromString(testData.workspaceSubmittedSubmission.workspaceId)).length.result)
    }
  }

  it should "delete a workspace with mixed submissions" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceMixedSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName7))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1","Sample",MethodRepoMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions)))
    }

    //Check if access levels on workspace exist
    assertResult(Set((testData.userOwner.userEmail.value, WorkspaceAccessLevels.Owner, false), (testData.userOwner.userEmail.value, WorkspaceAccessLevels.ProjectOwner, false), ("project-owner-access", WorkspaceAccessLevels.ProjectOwner, false), (testData.userReader.userEmail.value,WorkspaceAccessLevels.Read, false), (testData.userWriter.userEmail.value,WorkspaceAccessLevels.Write, false))) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceMixedSubmissions))).toSet
    }

    //Check if submissions on workspace exist
    assertResult(2) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions))).length
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findEntityByWorkspace(UUID.fromString(testData.workspaceMixedSubmissions.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName7), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName7))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions)))
    }

    //Check if access levels have been deleted
    assertResult(Vector()) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceMixedSubmissions)))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions)))
    }

    //Check if entities on workspace exist
    assertResult(0) {
      runAndWait(entityQuery.findEntityByWorkspace(UUID.fromString(testData.workspaceMixedSubmissions.workspaceId)).length.result)
    }

  }
}
