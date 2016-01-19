package org.broadinstitute.dsde.rawls.workspace

import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.broadinstitute.dsde.rawls.webservice._
import AttributeUpdateOperations._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}
import spray.http.{StatusCodes, StatusCode, OAuth2BearerToken}
import spray.testkit.ScalatestRouteTest

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

class WorkspaceServiceSpec extends FlatSpec with ScalatestRouteTest with Matchers with OrientDbTestFixture {
  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val s1 = Entity("s1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList))
  val workspace = Workspace(
    testData.wsName.namespace,
    testData.wsName.name,
    "aWorkspaceId",
    "aBucket",
    DateTime.now().withMillis(0),
    DateTime.now().withMillis(0),
    "test",
    Map.empty,
    Map.empty
  )

  case class TestApiService(dataSource: DataSource)(implicit val executionContext: ExecutionContext) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with MockUserInfoDirectives {
    def actorRefFactory = system
    lazy val workspaceService: WorkspaceService = TestActorRef(WorkspaceService.props(workspaceServiceConstructor, userInfo)).underlyingActor
    val mockServer = RemoteServicesMockServer()

    val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-ws-submission-supervisor")
    val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(dataSource, containerDAO, gcsDAO))

    val directoryDAO = new MockUserDirectoryDAO

    val userServiceConstructor = UserService.constructor(
      dataSource,
      gcsDAO,
      containerDAO,
      directoryDAO
    )_

    val workspaceServiceConstructor = WorkspaceService.constructor(
      dataSource,
      containerDAO,
      new HttpMethodRepoDAO(mockServer.mockServerBaseUrl),
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      gcsDAO,
      submissionSupervisor,
      bucketDeletionMonitor,
      userServiceConstructor
    )_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  def withTestDataServices(testCode: TestApiService => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      val apiService = new TestApiService(dataSource)
      try {
        testCode(apiService)
      } finally {
        apiService.cleanupSupervisor
      }
    }
  }


  "WorkspaceService" should "add attribute to entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("foo"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute("newAttribute", AttributeString("foo")))).attributes.get("newAttribute")
    }
  }

  it should "update attribute in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("biz"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddUpdateAttribute("foo", AttributeString("biz")))).attributes.get("foo")
    }
  }

  it should "remove attribute from entity" in withTestDataServices { services =>
    assertResult(None) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveAttribute("foo"))).attributes.get("foo")
    }
  }

  it should "add item to existing list in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(attributeList.list :+ AttributeString("new")))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("splat", AttributeString("new")))).attributes.get("splat")
    }
  }

  it should "add item to non-existing list in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("new"))))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("bob", AttributeString("new")))).attributes.get("bob")
    }
  }

  it should "create an empty list when inserting null via AddListMember" in withTestDataServices { services =>
    assertResult(Some(AttributeEmptyList)) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("nolisthere", AttributeNull))).attributes.get("nolisthere")
    }
  }

  it should "do nothing to existing lists when adding AttributeNull" in withTestDataServices { services =>
    assertResult(Some(attributeList)) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("splat", AttributeNull))).attributes.get("splat")
    }
  }

  it should "remove item from existing listing entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("b"), AttributeBoolean(true))))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember("splat", AttributeString("a")))).attributes.get("splat")
    }
  }

  it should "throw AttributeNotFoundException when removing from a list that does not exist" in withTestDataServices { services =>
    intercept[AttributeNotFoundException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember("bingo", AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when remove from an attribute that is not a list" in withTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(RemoveListMember("foo", AttributeString("a"))))
    }
  }

  it should "throw AttributeUpdateOperationException when adding to an attribute that is not a list" in withTestDataServices { services =>
    intercept[AttributeUpdateOperationException] {
      services.workspaceService.applyOperationsToEntity(s1, Seq(AddListMember("foo", AttributeString("a"))))
    }
  }

  it should "apply attribute updates in order to entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("splat"))) {
      services.workspaceService.applyOperationsToEntity(s1, Seq(
        AddUpdateAttribute("newAttribute", AttributeString("foo")),
        AddUpdateAttribute("newAttribute", AttributeString("bar")),
        AddUpdateAttribute("newAttribute", AttributeString("splat"))
      )).attributes.get("newAttribute")
    }
  }

  it should "return conflicts during an entity copy" in {
    val s1 = Entity("s1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3)))
    val s2 = Entity("s3", "child", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3)))
    //println("hello " + workspaceService.getCopyConflicts(wsns, wsname, Seq(s1, s2)).size)
    //still needs to be implemented fully
    assertResult(true) {
      true
    }
  }

  it should "validate method config expressions" in withTestDataServices { services =>
    val shouldBeValid = services.workspaceService.validateMCExpressions(testData.methodConfigValidExprs)
    assertResult(2) { shouldBeValid.validInputs.size }
    assertResult(2) { shouldBeValid.validOutputs.size }
    assertResult(0) { shouldBeValid.invalidInputs.size }
    assertResult(0) { shouldBeValid.invalidOutputs.size }

    val shouldBeInvalid = services.workspaceService.validateMCExpressions(testData.methodConfigInvalidExprs)
    assertResult(1) { shouldBeInvalid.validInputs.size }
    assertResult(0) { shouldBeInvalid.validOutputs.size }
    assertResult(1) { shouldBeInvalid.invalidInputs.size }
    assertResult(2) { shouldBeInvalid.invalidOutputs.size }
  }

  it should "retrieve ACLs" in withTestDataServices { services =>
    services.dataSource.inTransaction() { txn =>
      //Really annoying setup. I'm trying to avoid using the patch function to test get, so I have to poke
      //ACLs into the workspace manually.
      val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
      val group = RawlsGroup(RawlsGroupName("test"), RawlsGroupEmail("group@whitehouse.gov"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])

      containerDAO.authDAO.saveUser(user, txn)
      containerDAO.authDAO.saveGroup(group, txn)

      val ownerGroupRef = testData.workspace.accessLevels(WorkspaceAccessLevels.Owner)
      val theOwnerGroup = containerDAO.authDAO.loadGroup(ownerGroupRef, txn).get
      val replacementOwnerGroup = theOwnerGroup.copy(users = theOwnerGroup.users + user, subGroups = theOwnerGroup.subGroups + group)
      containerDAO.authDAO.saveGroup(replacementOwnerGroup, txn)
    }

    val vComplete = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Map[String, WorkspaceAccessLevel])]]
    val (vStatus, vData) = vComplete.response

    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(Map(
      "owner-access" -> WorkspaceAccessLevels.Owner,
      "obama@whitehouse.gov" -> WorkspaceAccessLevels.Owner,
      "group@whitehouse.gov" -> WorkspaceAccessLevels.Owner,
      "writer-access" -> WorkspaceAccessLevels.Write,
      "reader-access" -> WorkspaceAccessLevels.Read)) {
      vData
    }
  }

  it should "patch ACLs" in withTestDataServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    val group = RawlsGroup(RawlsGroupName("test"), RawlsGroupEmail("group@whitehouse.gov"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    services.dataSource.inTransaction() { txn =>
      containerDAO.authDAO.saveUser(user, txn)
      containerDAO.authDAO.saveGroup(group, txn)
    }

    services.gcsDAO.createGoogleGroup(group)
    testData.workspace.accessLevels.foreach { case (_, groupRef) => Await.result(services.gcsDAO.createGoogleGroup(groupRef), Duration.Inf) }


    //add ACL
    val aclAdd = Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner), WorkspaceACLUpdate("group@whitehouse.gov", WorkspaceAccessLevels.Read))
    val aclAddResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclAdd), Duration.Inf)
      .asInstanceOf[RequestComplete[StatusCode]]

    assertResult(StatusCodes.OK, "Add ACL shouldn't error") {
      aclAddResponse.response
    }

    //check result
    val (_, addedACLs) = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
    .asInstanceOf[RequestComplete[(StatusCode, Map[String, WorkspaceAccessLevel])]].response

    assertResult(Map(
      "owner-access" -> WorkspaceAccessLevels.Owner,
      "obama@whitehouse.gov" -> WorkspaceAccessLevels.Owner,
      "writer-access" -> WorkspaceAccessLevels.Write,
      "reader-access" -> WorkspaceAccessLevels.Read,
      "group@whitehouse.gov" -> WorkspaceAccessLevels.Read), "Add ACL should actually do so") {
      addedACLs
    }

    //update ACL
    val aclUpdates = Seq(WorkspaceACLUpdate("group@whitehouse.gov", WorkspaceAccessLevels.Write))
    val aclUpdateResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates), Duration.Inf)
      .asInstanceOf[RequestComplete[StatusCode]]

    assertResult(StatusCodes.OK, "Update ACL shouldn't error") {
      aclUpdateResponse.response
    }

    //check result
    val (_, updatedACLs) = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Map[String, WorkspaceAccessLevel])]].response

    assertResult(Map(
      "owner-access" -> WorkspaceAccessLevels.Owner,
      "obama@whitehouse.gov" -> WorkspaceAccessLevels.Owner,
      "writer-access" -> WorkspaceAccessLevels.Write,
      "reader-access" -> WorkspaceAccessLevels.Read,
      "group@whitehouse.gov" -> WorkspaceAccessLevels.Write), "Update ACL should actually do so") {
      updatedACLs
    }

    //remove ACL
    val aclRemove = Seq(WorkspaceACLUpdate("group@whitehouse.gov", WorkspaceAccessLevels.NoAccess))
    val aclRemoveResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclRemove), Duration.Inf)
      .asInstanceOf[RequestComplete[StatusCode]]

    assertResult(StatusCodes.OK, "Remove ACL shouldn't error") {
      aclRemoveResponse.response
    }

    //check result
    val (_, removedACLs) = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Map[String, WorkspaceAccessLevel])]].response

    assertResult(Map(
      "owner-access" -> WorkspaceAccessLevels.Owner,
      "obama@whitehouse.gov" -> WorkspaceAccessLevels.Owner,
      "writer-access" -> WorkspaceAccessLevels.Write,
      "reader-access" -> WorkspaceAccessLevels.Read), "Remove ACL should actually do so") {
      removedACLs
    }
  }

  it should "fail to patch ACLs if a user doesn't exist" in withTestDataServices { services =>
    val group = RawlsGroup(RawlsGroupName("test"), RawlsGroupEmail("group@whitehouse.gov"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    services.dataSource.inTransaction() { txn =>
      containerDAO.authDAO.saveGroup(group, txn)
    }
    testData.workspace.accessLevels.foreach { case (_, groupRef) => Await.result(services.gcsDAO.createGoogleGroup(groupRef), Duration.Inf) }

    val aclUpdates = Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner), WorkspaceACLUpdate("group@whitehouse.gov", WorkspaceAccessLevels.Read))
    val vComplete = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates), Duration.Inf)
      .asInstanceOf[RequestComplete[ErrorReport]]

    val vErrorReport = vComplete.response
    assertResult(StatusCodes.NotFound) {
      vErrorReport.statusCode.get
    }
  }
}
