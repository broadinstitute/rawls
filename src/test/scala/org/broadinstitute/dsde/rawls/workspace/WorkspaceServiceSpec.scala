package org.broadinstitute.dsde.rawls.workspace

import java.util.concurrent.TimeUnit

import _root_.slick.dbio.DBIO
import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.broadinstitute.dsde.rawls.webservice._
import AttributeUpdateOperations._
import org.scalatest.{FlatSpec, Matchers}
import spray.http.{StatusCodes, StatusCode}
import spray.testkit.ScalatestRouteTest
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.{FiniteDuration, Duration}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent


class WorkspaceServiceSpec extends FlatSpec with ScalatestRouteTest with Matchers with TestDriverComponent {
  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val s1 = Entity("s1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList))
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
    def actorRefFactory = system
    val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)
    lazy val workspaceService: WorkspaceService = TestActorRef(WorkspaceService.props(workspaceServiceConstructor, userInfo)).underlyingActor
    val mockServer = RemoteServicesMockServer()

    val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
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
      ProjectTemplate(Map.empty, Seq.empty)
    )_

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
    }
  }

  def withTestDataServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
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
    assertResult(2) { shouldBeValid.validInputs.size }
    assertResult(2) { shouldBeValid.validOutputs.size }
    assertResult(0) { shouldBeValid.invalidInputs.size }
    assertResult(0) { shouldBeValid.invalidOutputs.size }

    val shouldBeInvalid = services.workspaceService.validateMCExpressions(testData.methodConfigInvalidExprs, this)
    assertResult(1) { shouldBeInvalid.validInputs.size }
    assertResult(0) { shouldBeInvalid.validOutputs.size }
    assertResult(1) { shouldBeInvalid.invalidInputs.size }
    assertResult(2) { shouldBeInvalid.invalidOutputs.size }
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
    runAndWait(rawlsUserQuery.save(user))
    runAndWait(rawlsGroupQuery.save(group))

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
    runAndWait(rawlsGroupQuery.save(group))
    testData.workspace.accessLevels.foreach { case (_, groupRef) => Await.result(services.gcsDAO.createGoogleGroup(groupRef), Duration.Inf) }

    val aclUpdates = Seq(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner), WorkspaceACLUpdate("group@whitehouse.gov", WorkspaceAccessLevels.Read))
    val vComplete = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates), Duration.Inf)
      .asInstanceOf[RequestComplete[ErrorReport]]

    val vErrorReport = vComplete.response
    assertResult(StatusCodes.NotFound) {
      vErrorReport.statusCode.get
    }
  }

  it should "delete a workspace with no submissions" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    //check if method configs have been deleted

  }

  it should "delete a workspace with succeeded submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertResult(Option(testData.workspaceSuccessfulSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName4))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig2","Sample",MethodRepoMethod("myNamespace","method-a",1),"dsde"), MethodConfigurationShort("testConfig1","Sample",MethodRepoMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if access levels on workspace exist
    assertResult(Vector(("owner-access", WorkspaceAccessLevels.Owner), ("reader-access",WorkspaceAccessLevels.Read), ("writer-access",WorkspaceAccessLevels.Write))) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionSuccessful)) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(Vector(testData.indiv1, testData.indiv2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "Individual"))
    }
    assertResult(Vector(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7, testData.sample8)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "Sample"))
    }
    assertResult(Vector(testData.aliquot1, testData.aliquot2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "Aliquot"))
    }
    assertResult(Vector(testData.pair1, testData.pair2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "Pair"))
    }
    assertResult(Vector(testData.ps1)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "PairSet"))
    }
    assertResult(Vector(testData.sset1, testData.sset2, testData.sset3, testData.sset4, testData.sset_empty)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "SampleSet"))
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
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "Individual"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "Sample"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "Aliquot"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "Pair"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSuccessfulSubmission), "SampleSet"))
    }

  }

  it should "delete a workspace with failed submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertResult(Option(testData.workspaceFailedSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName5))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1","Sample",MethodRepoMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }

    //Check if access levels on workspace exist
    assertResult(Vector(("owner-access", WorkspaceAccessLevels.Owner), ("reader-access",WorkspaceAccessLevels.Read), ("writer-access",WorkspaceAccessLevels.Write))) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionFailed)) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(Vector(testData.indiv1, testData.indiv2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "Individual"))
    }
    assertResult(Vector(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7, testData.sample8)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "Sample"))
    }
    assertResult(Vector(testData.aliquot1, testData.aliquot2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "Aliquot"))
    }
    assertResult(Vector(testData.pair1, testData.pair2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "Pair"))
    }
    assertResult(Vector(testData.ps1)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "PairSet"))
    }
    assertResult(Vector(testData.sset1, testData.sset2, testData.sset3, testData.sset4, testData.sset_empty)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "SampleSet"))
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

    //Check if entities on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "Individual"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "Sample"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "Aliquot"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "Pair"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceFailedSubmission), "SampleSet"))
    }
  }

  it should "delete a workspace with submitted submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertResult(Option(testData.workspaceSubmittedSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName6))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1","Sample",MethodRepoMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if access levels on workspace exist
    assertResult(Vector(("owner-access", WorkspaceAccessLevels.Owner), ("reader-access",WorkspaceAccessLevels.Read), ("writer-access",WorkspaceAccessLevels.Write))) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionSubmitted)) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission)))
    }

    //Check if entities on workspace exist
    assertResult(Vector(testData.indiv1, testData.indiv2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "Individual"))
    }
    assertResult(Vector(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7, testData.sample8)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "Sample"))
    }
    assertResult(Vector(testData.aliquot1, testData.aliquot2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "Aliquot"))
    }
    assertResult(Vector(testData.pair1, testData.pair2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "Pair"))
    }
    assertResult(Vector(testData.ps1)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "PairSet"))
    }
    assertResult(Vector(testData.sset1, testData.sset2, testData.sset3, testData.sset4, testData.sset_empty)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "SampleSet"))
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

    //Check if entities on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "Individual"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "Sample"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "Aliquot"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "Pair"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "SampleSet"))
    }
  }

  it should "delete a workspace with mixed submissions" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertResult(Option(testData.workspaceMixedSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName7))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1","Sample",MethodRepoMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions)))
    }

    //Check if access levels on workspace exist
    assertResult(Vector(("owner-access", WorkspaceAccessLevels.Owner), ("reader-access",WorkspaceAccessLevels.Read), ("writer-access",WorkspaceAccessLevels.Write))) {
      runAndWait(workspaceQuery.listEmailsAndAccessLevel(SlickWorkspaceContext(testData.workspaceMixedSubmissions)))
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionMixed)) {
      runAndWait(submissionQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions)))
    }

    //Check if entities on workspace exist
    assertResult(Vector(testData.indiv1, testData.indiv2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceSubmittedSubmission), "Individual"))
    }
    assertResult(Vector(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7, testData.sample8)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions), "Sample"))
    }
    assertResult(Vector(testData.aliquot1, testData.aliquot2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions), "Aliquot"))
    }
    assertResult(Vector(testData.pair1, testData.pair2)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions), "Pair"))
    }
    assertResult(Vector(testData.ps1)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions), "PairSet"))
    }
    assertResult(Vector(testData.sset1, testData.sset2, testData.sset3, testData.sset4, testData.sset_empty)) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions), "SampleSet"))
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

    //Check if entities on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions), "Individual"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions), "Sample"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions), "Aliquot"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions), "Pair"))
    }
    assertResult(Vector()) {
      runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspaceMixedSubmissions), "SampleSet"))
    }

  }
}
