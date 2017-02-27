package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.RawlsRealmRefFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.ProjectOwner
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.http._
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.routing._

import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, TestData}

/**
 * Created by dvoet on 4/24/15.
 */
class WorkspaceApiServiceSpec extends ApiServiceSpec {
  import driver.api._

  trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
    val user: String
    def requireUserInfo(magnet: ImplicitMagnet[ExecutionContext]): Directive1[UserInfo] = {
      // just return the cookie text as the common name
      user match {
        case testData.userProjectOwner.userEmail.value => provide(UserInfo(user, OAuth2BearerToken("token"), 123, testData.userProjectOwner.userSubjectId.value))
        case testData.userOwner.userEmail.value => provide(UserInfo(user, OAuth2BearerToken("token"), 123, testData.userOwner.userSubjectId.value))
        case testData.userWriter.userEmail.value => provide(UserInfo(user, OAuth2BearerToken("token"), 123, testData.userWriter.userSubjectId.value))
        case testData.userReader.userEmail.value => provide(UserInfo(user, OAuth2BearerToken("token"), 123, testData.userReader.userSubjectId.value))
        case "no-access" => provide(UserInfo(user, OAuth2BearerToken("token"), 123, "123456789876543212348"))
        case _ => provide(UserInfo(user, OAuth2BearerToken("token"), 123, "123456789876543212349"))
      }
    }
  }

  case class TestApiService(dataSource: SlickDataSource, user: String, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectivesWithUser

  def withApiServices[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def withTestDataApiServicesAndUser[T](user: String)(testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource, user) { services =>
        testData.createWorkspaceGoogleGroups(services.gcsDAO)
        testCode(services)
      }
    }
  }

  def withEmptyWorkspaceApiServices[T](user: String)(testCode: TestApiService => T): T = {
    withCustomTestDatabase(new EmptyWorkspace) { dataSource: SlickDataSource =>
      withApiServices(dataSource, user)(testCode)
    }
  }

  def withLockedWorkspaceApiServices[T](user: String)(testCode: TestApiService => T): T = {
    withCustomTestDatabase(new LockedWorkspace) { dataSource: SlickDataSource =>
      withApiServices(dataSource, user)(testCode)
    }
  }

  class TestWorkspaces() extends TestData {
    val userProjectOwner = RawlsUser(UserInfo("project-owner-access", OAuth2BearerToken("token"), 123, "123456789876543210101"))
    val userOwner = RawlsUser(UserInfo(testData.userOwner.userEmail.value, OAuth2BearerToken("token"), 123, "123456789876543212345"))
    val userWriter = RawlsUser(UserInfo(testData.userWriter.userEmail.value, OAuth2BearerToken("token"), 123, "123456789876543212346"))
    val userReader = RawlsUser(UserInfo(testData.userReader.userEmail.value, OAuth2BearerToken("token"), 123, "123456789876543212347"))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName("ns"), generateBillingGroups(RawlsBillingProjectName("ns"), Map(ProjectRoles.Owner -> Set(userProjectOwner), ProjectRoles.User -> Set.empty), Map.empty), "testBucketUrl", CreationStatuses.Ready, None, None)

    val workspaceName = WorkspaceName(billingProject.projectName.value, "testworkspace")

    val workspace2Name = WorkspaceName(billingProject.projectName.value, "testworkspace2")

    val workspace3Name = WorkspaceName(billingProject.projectName.value, "testworkspace3")

    val defaultRealmGroup = makeRawlsGroup(s"Default Realm", Set.empty)

    val workspace1Id = UUID.randomUUID().toString
    val makeWorkspace1 = makeWorkspaceWithUsers(Map(
      WorkspaceAccessLevels.Owner -> Set(userOwner),
      WorkspaceAccessLevels.Write -> Set(userWriter),
      WorkspaceAccessLevels.Read -> Set(userReader)
    ))_
    val (workspace, workspaceGroups) = makeWorkspace1(billingProject, workspaceName.name, None, workspace1Id, "bucket1", testDate, testDate, "testUser", Map(AttributeName.withDefaultNS("a") -> AttributeString("x")), false)

    val makeWorkspace2 = makeWorkspaceWithUsers(Map(
      WorkspaceAccessLevels.Owner -> Set.empty,
      WorkspaceAccessLevels.Write -> Set(userOwner),
      WorkspaceAccessLevels.Read -> Set.empty
    ))_
    val workspace2Id = UUID.randomUUID().toString
    val (workspace2, workspace2Groups) = makeWorkspace2(billingProject, workspace2Name.name, None, workspace2Id, "bucket2", testDate, testDate, "testUser", Map(AttributeName.withDefaultNS("b") -> AttributeString("y")), false)

    val makeWorkspace3 = makeWorkspaceWithUsers(Map(
      WorkspaceAccessLevels.Owner -> Set.empty,
      WorkspaceAccessLevels.Write -> Set(userOwner),
      WorkspaceAccessLevels.Read -> Set.empty
    ))_
    val workspace3Id = UUID.randomUUID().toString
    val (workspace3, workspace3Groups) = makeWorkspace3(billingProject, workspace3Name.name, Some(RawlsRealmRef(defaultRealmGroup.groupName)), workspace3Id, "bucket3", testDate, testDate, "testUser", Map(AttributeName.withDefaultNS("c") -> AttributeString("z")), false)

    val sample1 = Entity("sample1", "sample", Map.empty)
    val sample2 = Entity("sample2", "sample", Map.empty)
    val sample3 = Entity("sample3", "sample", Map.empty)
    val sample4 = Entity("sample4", "sample", Map.empty)
    val sample5 = Entity("sample5", "sample", Map.empty)
    val sample6 = Entity("sample6", "sample", Map.empty)
    val sampleSet = Entity("sampleset", "sample_set", Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      sample1.toReference,
      sample2.toReference,
      sample3.toReference
    ))))

    val methodConfig = MethodConfiguration("dsde", "testConfig", "Sample", Map("ready"-> AttributeString("true")), Map("param1"-> AttributeString("foo")), Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")), MethodRepoMethod(workspaceName.namespace, "method-a", 1))
    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, workspaceName)
    val submissionTemplate = createTestSubmission(workspace, methodConfig, sampleSet, userOwner,
      Seq(sample1, sample2, sample3), Map(sample1 -> testData.inputResolutions, sample2 -> testData.inputResolutions, sample3 -> testData.inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> testData.inputResolutions2, sample5 -> testData.inputResolutions2, sample6 -> testData.inputResolutions2))
    val submissionSuccess = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Done,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Succeeded))
    )
    val submissionFail = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Done,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Failed))
    )
    val submissionRunning1 = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Submitted,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Running))
    )
    val submissionRunning2 = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Submitted,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Running))
    )

    override def save() = {
      DBIO.seq(
        rawlsUserQuery.save(userProjectOwner),
        rawlsUserQuery.save(userOwner),
        rawlsUserQuery.save(userWriter),
        rawlsUserQuery.save(userReader),
        DBIO.sequence(billingProject.groups.values.map(rawlsGroupQuery.save).toSeq),
        rawlsBillingProjectQuery.create(billingProject),
        DBIO.sequence(workspaceGroups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(workspace2Groups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(workspace3Groups.map(rawlsGroupQuery.save).toSeq),
        rawlsGroupQuery.save(defaultRealmGroup),
        rawlsGroupQuery.setGroupAsRealm(RawlsRealmRef(defaultRealmGroup.groupName)),

        workspaceQuery.save(workspace),
        workspaceQuery.save(workspace2),
        workspaceQuery.save(workspace3),
  
        withWorkspaceContext(workspace) { ctx =>
          DBIO.seq(
            entityQuery.save(ctx, sample1),
            entityQuery.save(ctx, sample2),
            entityQuery.save(ctx, sample3),
            entityQuery.save(ctx, sample4),
            entityQuery.save(ctx, sample5),
            entityQuery.save(ctx, sample6),
            entityQuery.save(ctx, sampleSet),
    
            methodConfigurationQuery.save(ctx, methodConfig),
    
            submissionQuery.create(ctx, submissionSuccess),
            submissionQuery.create(ctx, submissionFail),
            submissionQuery.create(ctx, submissionRunning1),
            submissionQuery.create(ctx, submissionRunning2)
          )
        }
      )
    }
  }

  val testWorkspaces = new TestWorkspaces

  def withTestWorkspacesApiServices[T](testCode: TestApiService => T): T = {
    withCustomTestDatabase(testWorkspaces) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def withTestWorkspacesApiServicesAndUser[T](user: String)(testCode: TestApiService => T): T = {
    withCustomTestDatabase(testWorkspaces) { dataSource: SlickDataSource =>
      withApiServices(dataSource, user)(testCode)
    }
  }

  "WorkspaceApi" should "return 201 for post to workspaces" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      None,
      Map.empty
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        assertResult(newWorkspace) {
          val ws = runAndWait(workspaceQuery.findByName(newWorkspace.toWorkspaceName)).get
          WorkspaceRequest(ws.namespace, ws.name, ws.realm, ws.attributes)
        }
        assertResult(newWorkspace) {
          val ws = responseAs[Workspace]
          WorkspaceRequest(ws.namespace, ws.name, ws.realm, ws.attributes)
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${newWorkspace.namespace}/${newWorkspace.name}"))))) {
          header("Location")
        }
      }
  }

  it should "return 400 for post to workspaces with not ready project" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = testData.billingProject.projectName.value,
      name = "newWorkspace",
      None,
      Map.empty
    )

    Seq(CreationStatuses.Creating, CreationStatuses.Error).foreach { projectStatus =>
      runAndWait(rawlsBillingProjectQuery.updateBillingProjects(Seq(testData.billingProject.copy(status = projectStatus))))

      Post(s"/workspaces", httpJson(newWorkspace)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest, response.entity.asString) {
            status
          }
        }
    }
  }

  it should "create a workspace with a Realm" in withTestDataApiServices { services =>
    val realmGroup = RawlsGroup(RawlsGroupName("realm-for-testing"), RawlsGroupEmail("king@realm.example.com"), Set(testData.userOwner), Set.empty)
    val workspaceWithRealm = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      realm = Option(RawlsRealmRef(realmGroup.groupName)),
      Map.empty
    )

    runAndWait(rawlsGroupQuery.save(realmGroup))
    runAndWait(rawlsGroupQuery.setGroupAsRealm(RawlsRealmRef(realmGroup.groupName)))

    Post(s"/workspaces", httpJson(workspaceWithRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        assertResult(workspaceWithRealm) {
          val ws = runAndWait(workspaceQuery.findByName(workspaceWithRealm.toWorkspaceName)).get
          WorkspaceRequest(ws.namespace, ws.name, ws.realm, ws.attributes)
        }
        assertResult(workspaceWithRealm) {
          val ws = responseAs[Workspace]
          WorkspaceRequest(ws.namespace, ws.name, ws.realm, ws.attributes)
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${workspaceWithRealm.namespace}/${workspaceWithRealm.name}"))))) {
          header("Location")
        }
      }
  }

  it should "return 403 on create workspace with invalid-namespace attributes" in withTestDataApiServices { services =>
    val invalidAttrNamespace = "invalid"

    val newWorkspace = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      None,
      Map(AttributeName(invalidAttrNamespace, "attribute") -> AttributeString("foo"))
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(invalidAttrNamespace))
      }
  }

  it should "return 201 on create workspace with library-namespace attributes as curator" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      None,
      Map(AttributeName(AttributeName.libraryNamespace, "attribute") -> AttributeString("foo"))
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        assertResult(newWorkspace) {
          val ws = runAndWait(workspaceQuery.findByName(newWorkspace.toWorkspaceName)).get
          WorkspaceRequest(ws.namespace, ws.name, ws.realm, ws.attributes)
        }
        assertResult(newWorkspace) {
          val ws = responseAs[Workspace]
          WorkspaceRequest(ws.namespace, ws.name, ws.realm, ws.attributes)
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${newWorkspace.namespace}/${newWorkspace.name}"))))) {
          header("Location")
        }
      }
  }

  it should "return 403 on create workspace with library-namespace attributes as non-curator" in withTestDataApiServices { services =>
    revokeCuratorRole(services)

    val newWorkspace = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      None,
      Map(AttributeName(AttributeName.libraryNamespace, "attribute") -> AttributeString("foo"))
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(AttributeName.libraryNamespace))
      }
  }

  it should "concurrently create workspaces" in withTestDataApiServices { services =>
    def generator(i: Int): ReadAction[Option[Workspace]] = {
      val newWorkspace = WorkspaceRequest(
        namespace = testData.wsName.namespace,
        name = s"newWorkspace$i",
        None,
        Map.empty
      )

      Post(s"/workspaces", httpJson(newWorkspace)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created, response.entity.asString) {
            status
          }
          workspaceQuery.findByName(newWorkspace.toWorkspaceName)
        }
    }

    runMultipleAndWait(100)(generator)
  }

  it should "get a workspace" in withTestWorkspacesApiServices { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val dateTime = currentTime()
        assertResult(
          WorkspaceListResponse(WorkspaceAccessLevels.Owner, testWorkspaces.workspace.copy(lastModified = dateTime), WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2), Seq(testData.userOwner.userEmail.value))
        ){
          val response = responseAs[WorkspaceListResponse]
          WorkspaceListResponse(response.accessLevel, response.workspace.copy(lastModified = dateTime), response.workspaceSubmissionStats, response.owners)
        }
      }
  }

  it should "return 404 getting a non-existent workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "delete a workspace" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted, response.entity.asString) {
          status
        }
      }
      assertResult(None) {
        runAndWait(workspaceQuery.findByName(testData.workspace.toWorkspaceName))
      }
  }

  it should "delete all entities when deleting a workspace" in withTestDataApiServices { services =>
    // check that length of result is > 0:
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(entityQuery.findEntityByWorkspace(workspaceContext.workspaceId).length.result) > 0
      }
    }
    // delete the workspace
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted, response.entity.asString) {
          status
        }
      }
    // now you should have no entities listed
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(entityQuery.findEntityByWorkspace(workspaceContext.workspaceId).length.result) == 0
      }
    }
  }

  it should "delete all method configs when deleting a workspace" in withTestDataApiServices { services =>
    // check that length of result is > 0:
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(methodConfigurationQuery.findByName(workspaceContext.workspaceId, testData.methodConfig.namespace,
          testData.methodConfig.name).length.result) > 0
      }
    }
    // delete the workspace
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted, response.entity.asString) {
          status
        }
      }
    // now you should have no method configs listed
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(methodConfigurationQuery.findByName(workspaceContext.workspaceId, testData.methodConfig.namespace,
          testData.methodConfig.name).length.result) == 0
      }
    }
  }

  it should "delete all submissions when deleting a workspace" in withTestDataApiServices { services =>
    // check that length of result is > 0:
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(submissionQuery.findByWorkspaceId(workspaceContext.workspaceId).length.result) > 0
      }
    }
    // delete the workspace
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted, response.entity.asString) {
          status
        }
      }
    // now you should have no submissions listed
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(submissionQuery.findByWorkspaceId(workspaceContext.workspaceId).length.result) == 0
      }
    }
  }

  it should "delete workspace groups when deleting a workspace" in withTestDataApiServices { services =>
    val workspaceGroupRefs = (testData.workspace.accessLevels.values.toSet ++ testData.workspace.realmACLs.values) - testData.workspace.accessLevels(ProjectOwner)
    workspaceGroupRefs foreach { case groupRef =>
      assertResult(Option(groupRef)) {
        runAndWait(rawlsGroupQuery.load(groupRef)) map RawlsGroup.toRef
      }
    }

    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted) {
          status
        }
      }

      workspaceGroupRefs foreach { case groupRef =>
        assertResult(None) {
          runAndWait(rawlsGroupQuery.load(groupRef))
        }
      }

  }

  it should "list workspaces" in withTestWorkspacesApiServices { services =>
    Get("/workspaces") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }

        val dateTime = currentTime()
        assertResult(Set(
          WorkspaceListResponse(WorkspaceAccessLevels.Owner, testWorkspaces.workspace.copy(lastModified = dateTime), WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2), Seq(testData.userOwner.userEmail.value)),
          WorkspaceListResponse(WorkspaceAccessLevels.Write, testWorkspaces.workspace2.copy(lastModified = dateTime), WorkspaceSubmissionStats(None, None, 0), Seq.empty),
          WorkspaceListResponse(WorkspaceAccessLevels.NoAccess, testWorkspaces.workspace3.copy(lastModified = dateTime), WorkspaceSubmissionStats(None, None, 0), Seq.empty)
        )) {
          responseAs[Array[WorkspaceListResponse]].toSet[WorkspaceListResponse].map(wslr => wslr.copy(workspace = wslr.workspace.copy(lastModified = dateTime)))
        }
      }
  }

  it should "return 404 Not Found on clone if the source workspace cannot be found" in withTestDataApiServices { services =>
    val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_nonexistent", None, Map.empty)
    Post(s"/workspaces/${testData.workspace.namespace}/nonexistent/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    //this endpoint is only temporarily returning MethodNotAllowed. When GAWB-422 is complete, it should return NotFound
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.MethodNotAllowed) {
          status
        }
      }
  }

  it should "return 200 on update workspace attributes" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          runAndWait(workspaceQuery.findByName(testData.wsName)).get.attributes.get(AttributeName.withDefaultNS("boo"))
        }
      }

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(None) {
          runAndWait(workspaceQuery.findByName(testData.wsName)).get.attributes.get(AttributeName.withDefaultNS("boo"))
        }
      }
  }

  it should "return 403 on update workspace with invalid-namespace attributes" in withTestDataApiServices { services =>
    val name = AttributeName("invalid", "misc")
    val attr = AttributeString("foo")

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(name.namespace))
      }
  }

  it should "return 200 on update with workspace library-namespace attributes as curator" in withTestDataApiServices { services =>
    val name = AttributeName(AttributeName.libraryNamespace, "whatever")
    val attr: Attribute = AttributeString("something")

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(attr)) {
          runAndWait(workspaceQuery.findByName(testData.wsName)).get.attributes.get(name)
        }
      }

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute(name): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(None) {
          runAndWait(workspaceQuery.findByName(testData.wsName)).get.attributes.get(name)
        }
      }
  }

  it should "return 403 on update (add) with workspace library-namespace attributes as non-curator" in withTestDataApiServices { services =>
    revokeCuratorRole(services)

    val name = AttributeName(AttributeName.libraryNamespace, "whatever")
    val attr: Attribute = AttributeString("something")

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(name.namespace))
      }
  }

  it should "return 403 on update (remove) with workspace library-namespace attributes as non-curator" in withTestDataApiServices { services =>
    val name = AttributeName(AttributeName.libraryNamespace, "whatever")
    val attr: Attribute = AttributeString("something")

    // first add as curator

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(attr)) {
          runAndWait(workspaceQuery.findByName(testData.wsName)).get.attributes.get(name)
        }
      }

    // then remove as non-curator

    revokeCuratorRole(services)

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute(name): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(name.namespace))
      }
  }

  it should "return 400 on update with workspace attributes that specify list as value" in withTestDataApiServices { services =>
    // we can't make this non-sensical json from our object hierarchy; we have to craft it by hand
    val testPayload =
      """
        |[{
        |    "op" : "AddUpdateAttribute",
        |    "attributeName" : "something",
        |    "addUpdateAttribute" : {
        |      "itemsType" : "SomeValueNotExpected",
        |      "items" : ["foo", "bar", "baz"]
        |    }
        |}]
      """.stripMargin

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, testPayload)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "concurrently update workspace attributes" in withTestDataApiServices { services =>
    def generator(i: Int): ReadAction[Option[Workspace]] = {
      Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString(s"bang$i")): AttributeUpdateOperation))) ~>
        sealRoute(services.workspaceRoutes) ~> check {
          assertResult(StatusCodes.OK, responseAs[String]) {
            status
          }
          workspaceQuery.findByName(testData.wsName)
        }
    }

    runMultipleAndWait(100)(generator)
  }

  it should "clone a workspace if the source exists" in withTestDataApiServices { services =>
    val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy", None, Map.empty)
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }

        withWorkspaceContext(testData.workspace) { sourceWorkspaceContext =>
          val copiedWorkspace = runAndWait(workspaceQuery.findByName(workspaceCopy.toWorkspaceName)).get
          assert(copiedWorkspace.attributes == testData.workspace.attributes)

          withWorkspaceContext(copiedWorkspace) { copiedWorkspaceContext =>
            //Name, namespace, creation date, and owner might change, so this is all that remains.
            assertResult(runAndWait(entityQuery.listEntitiesAllTypes(sourceWorkspaceContext)).toSet) {
              runAndWait(entityQuery.listEntitiesAllTypes(copiedWorkspaceContext)).toSet
            }
            assertResult(runAndWait(methodConfigurationQuery.list(sourceWorkspaceContext)).toSet) {
              runAndWait(methodConfigurationQuery.list(copiedWorkspaceContext)).toSet
            }
          }
        }

        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${workspaceCopy.namespace}/${workspaceCopy.name}"))))) {
          header("Location")
        }
      }
  }

  it should "return 403 on clone workspace when adding invalid-namespace attributes" in withTestDataApiServices { services =>
    val invalidAttrNamespace = "invalid"

    val workspaceCopy = WorkspaceRequest(
      namespace = testData.workspace.namespace,
      name = "test_copy",
      None, Map(AttributeName(invalidAttrNamespace, "attribute") -> AttributeString("foo"))
    )
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(invalidAttrNamespace))
      }
  }

  it should "return 201 on clone workspace when adding library-namespace attributes as curator" in withTestDataApiServices { services =>
    val newAttr = AttributeName(AttributeName.libraryNamespace, "attribute") -> AttributeString("foo")

    val workspaceCopy = WorkspaceRequest(
      namespace = testData.workspace.namespace,
      name = "test_copy",
      None, Map(newAttr)
    )
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }

        withWorkspaceContext(testData.workspace) { sourceWorkspaceContext =>
          val copiedWorkspace = runAndWait(workspaceQuery.findByName(workspaceCopy.toWorkspaceName)).get
          assert(copiedWorkspace.attributes == testData.workspace.attributes + newAttr)

          withWorkspaceContext(copiedWorkspace) { copiedWorkspaceContext =>
            //Name, namespace, creation date, and owner might change, so this is all that remains.
            assertResult(runAndWait(entityQuery.listEntitiesAllTypes(sourceWorkspaceContext)).toSet) {
              runAndWait(entityQuery.listEntitiesAllTypes(copiedWorkspaceContext)).toSet
            }
            assertResult(runAndWait(methodConfigurationQuery.list(sourceWorkspaceContext)).toSet) {
              runAndWait(methodConfigurationQuery.list(copiedWorkspaceContext)).toSet
            }
          }
        }

        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${workspaceCopy.namespace}/${workspaceCopy.name}"))))) {
          header("Location")
        }
      }
  }

  it should "return 403 on clone workspace when adding library-namespace attributes as non-curator" in withTestDataApiServices { services =>
    revokeCuratorRole(services)

    val workspaceCopy = WorkspaceRequest(
      namespace = testData.workspace.namespace,
      name = "test_copy",
      None, Map(AttributeName(AttributeName.libraryNamespace, "attribute") -> AttributeString("foo"))
    )
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(AttributeName.libraryNamespace))
      }
  }

  it should "return 201 on clone workspace with existing library-namespace attributes as non-curator" in withTestDataApiServices { services =>

    // first update Workspace as curator

    val updatedWorkspace = testData.workspace.copy(attributes = testData.workspace.attributes + (AttributeName(AttributeName.libraryNamespace, "attribute") -> AttributeString("foo")))
    runAndWait(workspaceQuery.save(updatedWorkspace))

    revokeCuratorRole(services)

    // then clone as non-curator

    val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy", None, Map.empty)
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }

        withWorkspaceContext(testData.workspace) { sourceWorkspaceContext =>
          val copiedWorkspace = runAndWait(workspaceQuery.findByName(workspaceCopy.toWorkspaceName)).get
          assert(copiedWorkspace.attributes == updatedWorkspace.attributes)

          withWorkspaceContext(copiedWorkspace) { copiedWorkspaceContext =>
            //Name, namespace, creation date, and owner might change, so this is all that remains.
            assertResult(runAndWait(entityQuery.listEntitiesAllTypes(sourceWorkspaceContext)).toSet) {
              runAndWait(entityQuery.listEntitiesAllTypes(copiedWorkspaceContext)).toSet
            }
            assertResult(runAndWait(methodConfigurationQuery.list(sourceWorkspaceContext)).toSet) {
              runAndWait(methodConfigurationQuery.list(copiedWorkspaceContext)).toSet
            }
          }
        }

        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${workspaceCopy.namespace}/${workspaceCopy.name}"))))) {
          header("Location")
        }
      }
  }

  it should "clone a workspace's Realm if it exists" in withTestDataApiServices { services =>
    val realmGroup = RawlsGroup(RawlsGroupName("realm-for-testing"), RawlsGroupEmail("king@realm.example.com"), Set(testData.userOwner), Set.empty)

    runAndWait(rawlsGroupQuery.save(realmGroup))
    runAndWait(rawlsGroupQuery.setGroupAsRealm(RawlsRealmRef(realmGroup.groupName)))

    val workspaceWithRealm = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      realm = Option(RawlsRealmRef(realmGroup.groupName)),
      Map.empty
    )

    Post(s"/workspaces", httpJson(workspaceWithRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    val workspaceCopy = WorkspaceRequest(namespace = workspaceWithRealm.namespace, name = "test_copy", workspaceWithRealm.realm, Map.empty)
    Post(s"/workspaces/${workspaceWithRealm.namespace}/${workspaceWithRealm.name}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(workspaceWithRealm.realm) {
          responseAs[Workspace].realm
        }
      }
  }

  it should "not allow changing a workspace's Realm if it exists" in withTestDataApiServices { services =>
    val name1 = "Guilder"
    val name2 = "Florin"
    val realmGroup1 = RawlsGroup(RawlsGroupName(name1), RawlsGroupEmail("king@guilder.eu"), Set(testData.userOwner), Set.empty)
    val realmGroup2 = RawlsGroup(RawlsGroupName(name2), RawlsGroupEmail("king@florin.eu"), Set(testData.userOwner), Set.empty)

    runAndWait(rawlsGroupQuery.save(realmGroup1))
    runAndWait(rawlsGroupQuery.setGroupAsRealm(RawlsRealmRef(realmGroup1.groupName)))

    val workspaceWithRealm = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      realm = Option(RawlsRealmRef(realmGroup1.groupName)),
      Map.empty
    )

    Post(s"/workspaces", httpJson(workspaceWithRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    val workspaceCopy = WorkspaceRequest(namespace = workspaceWithRealm.namespace, name = "test_copy", Option(RawlsRealmRef(realmGroup2.groupName)), Map.empty)
    Post(s"/workspaces/${workspaceWithRealm.namespace}/${workspaceWithRealm.name}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(workspaceWithRealm.namespace))
        assert(errorText.contains(workspaceWithRealm.name))
        assert(errorText.contains(name1))
        assert(errorText.contains(name2))
      }
  }

  it should "set the Realm when cloning a workspace with no Realm" in withTestDataApiServices { services =>
    val workspaceCopyNoRealm = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy", None, Map.empty)
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceCopyNoRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        assertResult(None) {
          responseAs[Workspace].realm
        }
      }

    val realmGroup = RawlsGroup(RawlsGroupName("realm-for-testing"), RawlsGroupEmail("king@realm.example.com"), Set(testData.userOwner), Set.empty)
    val realmGroupRef: RawlsRealmRef = RawlsRealmRef(realmGroup.groupName)

    runAndWait(rawlsGroupQuery.save(realmGroup))
    runAndWait(rawlsGroupQuery.setGroupAsRealm(realmGroupRef))

    val workspaceCopyRealm = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy2", Option(realmGroupRef), Map.empty)
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceCopyRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        assertResult(Some(realmGroupRef)) {
          responseAs[Workspace].realm
        }
      }
  }

  it should "return 403 when creating a workspace in a realm that you don't have access to" in withTestDataApiServices { services =>
    import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._

    val realmGroup = RawlsRealmRef(RawlsGroupName("realm-for-testing"))

    services.gcsDAO.adminList += testData.userOwner.userEmail.value

    Post(s"/admin/realms", realmGroup) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    val workspaceWithRealm = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      realm = Option(realmGroup),
      Map.empty
    )

    Post(s"/workspaces", httpJson(workspaceWithRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }

  }

  it should "return 403 when creating a workspace and pointing the realmRef to a regular group" in withTestDataApiServices { services =>
    val realmGroup = RawlsGroup(RawlsGroupName("realm-for-testing"), RawlsGroupEmail("king@realm.example.com"), Set.empty, Set.empty)

    services.gcsDAO.adminList += testData.userOwner.userEmail.value

    Post(s"/admin/groups", realmGroup) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"/admin/groups/${realmGroup.groupName.value}/members", RawlsGroupMemberList(userEmails = Some(Seq("owner-access")))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    val workspaceWithRealm = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      realm = Option(RawlsRealmRef(realmGroup.groupName)),
      Map.empty
    )

    Post(s"/workspaces", httpJson(workspaceWithRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }

  }

  it should "update the intersection groups for related workspaces when group membership changes" in withTestDataApiServices { services =>
    import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.RawlsRealmRefFormat
    val realmGroup = RawlsRealmRef(RawlsGroupName("realm-for-testing"))

    services.gcsDAO.adminList += testData.userOwner.userEmail.value

    Post(s"/admin/realms", realmGroup) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
      }

    val ownerAdd = RawlsGroupMemberList(None, None, Some(Seq(testData.userOwner.userSubjectId.value)), None)
    Post(s"/admin/groups/${realmGroup.realmName.value}/members", httpJson(ownerAdd)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    val workspaceWithRealm = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      realm = Option(realmGroup),
      Map.empty
    )

    Post(s"/workspaces", httpJson(workspaceWithRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    //add userWriter to writer ACLs + add userOwner to owner ACLs
    Patch(s"/workspaces/${workspaceWithRealm.namespace}/${workspaceWithRealm.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Write, None), WorkspaceACLUpdate(testData.userOwner.userEmail.value, WorkspaceAccessLevels.Owner, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
      }

    //assert userWriter is not a part of realm writer ACLs
    val ws1 = runAndWait(workspaceQuery.findByName(WorkspaceName(workspaceWithRealm.namespace, workspaceWithRealm.name))).get

    assertResult(false){
      runAndWait(rawlsGroupQuery.load(ws1.realmACLs(WorkspaceAccessLevels.Write))).get.users.contains(RawlsUserRef(testData.userWriter.userSubjectId))
    }

    //add userWriter to realm
    val groupAdd = RawlsGroupMemberList(None, None, Some(Seq(testData.userWriter.userSubjectId.value)), None)
    Post(s"/admin/groups/${realmGroup.realmName.value}/members", httpJson(groupAdd)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    //assert userWriter is a part of realm writer ACLs and userOwner is a part of realm owner ACLs
    val ws2 = runAndWait(workspaceQuery.findByName(WorkspaceName(workspaceWithRealm.namespace, workspaceWithRealm.name))).get

    assertResult(true){
      runAndWait(rawlsGroupQuery.load(ws2.realmACLs(WorkspaceAccessLevels.Write))).get.users.contains(RawlsUserRef(testData.userWriter.userSubjectId))
    }
    assertResult(true){
      runAndWait(rawlsGroupQuery.load(ws2.realmACLs(WorkspaceAccessLevels.Owner))).get.users.contains(RawlsUserRef(testData.userOwner.userSubjectId))
    }

    //remove userWriter from realm
    val groupRemove = RawlsGroupMemberList(None, None, Some(Seq(testData.userWriter.userSubjectId.value)), None)
    Delete(s"/admin/groups/${realmGroup.realmName.value}/members", httpJson(groupRemove)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    //assert userWriter is not a part of realm writer ACLs
    val ws3 = runAndWait(workspaceQuery.findByName(WorkspaceName(workspaceWithRealm.namespace, workspaceWithRealm.name))).get

    assertResult(false){
      runAndWait(rawlsGroupQuery.load(ws3.realmACLs(WorkspaceAccessLevels.Write))).get.users.contains(RawlsUserRef(testData.userWriter.userSubjectId))
    }
  }

  it should "update the intersection groups for related workspaces when updating subgroup membership" in withTestDataApiServices { services =>
    import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.RawlsRealmRefFormat

    val realmGroup = RawlsRealmRef(RawlsGroupName("realm-for-testing"))
    val groupA = RawlsGroup(RawlsGroupName("GroupA"), RawlsGroupEmail("groupA@firecloud.org"), Set.empty, Set.empty)
    val groupB = RawlsGroup(RawlsGroupName("GroupB"), RawlsGroupEmail("groupB@firecloud.org"), Set.empty, Set(groupA))

    services.gcsDAO.adminList += testData.userOwner.userEmail.value

    //create the realm group
    Post(s"/admin/realms", realmGroup) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    //add the owner to the realm
    val ownerAdd = RawlsGroupMemberList(None, None, Some(Seq(testData.userOwner.userSubjectId.value)), None)
    Post(s"/admin/groups/${realmGroup.realmName.value}/members", httpJson(ownerAdd)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    val workspaceWithRealm = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      realm = Option(realmGroup),
      Map.empty
    )

    //create the workspace with the realm
    Post(s"/workspaces", httpJson(workspaceWithRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    //create group A
    Post(s"/admin/groups", groupA) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    //create group B
    Post(s"/admin/groups", groupB) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    //add userWriter to group A
    val addWriterToA = RawlsGroupMemberList(None, None, Some(Seq(testData.userWriter.userSubjectId.value)), None)
    Post(s"/admin/groups/${groupA.groupName.value}/members", httpJson(addWriterToA)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    //add group A to group B
    val addAtoB = RawlsGroupMemberList(None, None, None, Some(Seq(groupA.groupName.value)))
    Post(s"/admin/groups/${groupB.groupName.value}/members", httpJson(addAtoB)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    val writerAdd = RawlsGroupMemberList(None, None, Some(Seq(testData.userWriter.userSubjectId.value)), None)
    Post(s"/admin/groups/${realmGroup.realmName.value}/members", httpJson(writerAdd)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    //add group B to the writer ACL + add owner to owner ACL
    val groupBEmail = RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(groupB.groupName)).value
    Patch(s"/workspaces/${workspaceWithRealm.namespace}/${workspaceWithRealm.name}/acl", httpJson(Seq(WorkspaceACLUpdate(groupBEmail, WorkspaceAccessLevels.Write, None), WorkspaceACLUpdate(testData.userOwner.userEmail.value, WorkspaceAccessLevels.Owner, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    //assert userWriter is a part of realm writer ACLs
    val ws1 = runAndWait(workspaceQuery.findByName(WorkspaceName(workspaceWithRealm.namespace, workspaceWithRealm.name))).get

    assertResult(true) {
      runAndWait(rawlsGroupQuery.load(ws1.realmACLs(WorkspaceAccessLevels.Write))).get.users.contains(RawlsUserRef(testData.userWriter.userSubjectId))
    }

    //remove userWriter from group A
    val removeWriterFromA = RawlsGroupMemberList(None, None, Some(Seq(testData.userWriter.userSubjectId.value)), None)
    Delete(s"/admin/groups/${realmGroup.realmName.value}/members", httpJson(removeWriterFromA)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    //assert userWriter is not a part of realm writer ACLs
    val ws2 = runAndWait(workspaceQuery.findByName(WorkspaceName(workspaceWithRealm.namespace, workspaceWithRealm.name))).get

    assertResult(false) {
      runAndWait(rawlsGroupQuery.load(ws2.realmACLs(WorkspaceAccessLevels.Write))).get.users.contains(RawlsUserRef(testData.userWriter.userSubjectId))
    }
  }

  it should "update the intersection groups for related workspaces when updating realm subgroup membership" in withTestDataApiServices { services =>
    import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.RawlsRealmRefFormat

    val realmGroup = RawlsRealmRef(RawlsGroupName("realm-for-testing"))
    val groupC = RawlsGroup(RawlsGroupName("GroupC"), RawlsGroupEmail("groupC@firecloud.org"), Set.empty, Set.empty)
    val groupD = RawlsGroup(RawlsGroupName("GroupD"), RawlsGroupEmail("groupD@firecloud.org"), Set.empty, Set(groupC))

    services.gcsDAO.adminList += testData.userOwner.userEmail.value

    //create the realm group
    Post(s"/admin/realms", realmGroup) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    //add the owner to the realm
    val ownerAdd = RawlsGroupMemberList(None, None, Some(Seq(testData.userOwner.userSubjectId.value)), None)
    Post(s"/admin/groups/${realmGroup.realmName.value}/members", httpJson(ownerAdd)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    val workspaceWithRealm = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      realm = Option(realmGroup),
      Map.empty
    )

    //create the workspace with the realm
    Post(s"/workspaces", httpJson(workspaceWithRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    //create group C
    Post(s"/admin/groups", groupC) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    //create group D
    Post(s"/admin/groups", groupD) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    //add userWriter to group C
    val addWriterToC = RawlsGroupMemberList(None, None, Some(Seq(testData.userWriter.userSubjectId.value)), None)
    Post(s"/admin/groups/${groupC.groupName.value}/members", httpJson(addWriterToC)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    //add group C to group D
    val addCtoD = RawlsGroupMemberList(None, None, None, Some(Seq(groupC.groupName.value)))
    Post(s"/admin/groups/${groupD.groupName.value}/members", httpJson(addCtoD)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    //add group D to realm
    val addDtoRealm= RawlsGroupMemberList(None, None, None, Some(Seq(groupD.groupName.value)))
    Post(s"/admin/groups/${realmGroup.realmName.value}/members", httpJson(addDtoRealm)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    //add userWriter to the writer ACL + add owner to owner ACL
    Patch(s"/workspaces/${workspaceWithRealm.namespace}/${workspaceWithRealm.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Write, None), WorkspaceACLUpdate(testData.userOwner.userEmail.value, WorkspaceAccessLevels.Owner, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }

    //assert userWriter is a part of realm writer ACLs
    val ws1 = runAndWait(workspaceQuery.findByName(WorkspaceName(workspaceWithRealm.namespace, workspaceWithRealm.name))).get

    assertResult(true) {
      runAndWait(rawlsGroupQuery.load(ws1.realmACLs(WorkspaceAccessLevels.Write))).get.users.contains(RawlsUserRef(testData.userWriter.userSubjectId))
    }

    //remove userWriter from group C
    val removeWriterFromC = RawlsGroupMemberList(None, None, Some(Seq(testData.userWriter.userSubjectId.value)), None)
    Delete(s"/admin/groups/${groupC.groupName.value}/members", httpJson(removeWriterFromC)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    //assert userWriter is not a part of realm writer ACLs
    val ws2 = runAndWait(workspaceQuery.findByName(WorkspaceName(workspaceWithRealm.namespace, workspaceWithRealm.name))).get

    assertResult(false) {
      runAndWait(rawlsGroupQuery.load(ws2.realmACLs(WorkspaceAccessLevels.Write))).get.users.contains(RawlsUserRef(testData.userWriter.userSubjectId))
    }
  }

  it should "add attributes when cloning a workspace" in withTestDataApiServices { services =>
    val workspaceNoAttrs = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy", None, Map.empty)
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceNoAttrs)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        assertResult(testData.workspace.attributes) {
          responseAs[Workspace].attributes
        }
      }

    val newAtts = Map(
      AttributeName.withDefaultNS("number") -> AttributeNumber(11),    // replaces an existing attribute
      AttributeName.withDefaultNS("another") -> AttributeNumber(12)    // adds a new attribute
    )

    val workspaceCopyRealm = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy2", None, newAtts)
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceCopyRealm)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(testData.workspace.attributes ++ newAtts) {
          responseAs[Workspace].attributes
        }
      }
  }

  it should "return 409 Conflict on clone if the destination already exists" in withTestDataApiServices { services =>
    val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace, name = testData.workspaceNoGroups.name, None, Map.empty)
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 200 when requesting an ACL from an existing workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when requesting an ACL from a non-existent workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/xyzzy/plugh/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 when replacing an ACL for an existing workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty[WorkspaceACLUpdate].toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "modifying a workspace acl should modify the workspace last modified date" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty[WorkspaceACLUpdate].toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }
  }

  it should "return 404 when replacing an ACL on a non-existent workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/xyzzy/plugh/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty[WorkspaceACLUpdate].toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  // Begin tests where routes are restricted by ACLs

  // Get Workspace requires READ access.  Accept if OWNER, WRITE, READ; Reject if NO ACCESS

  it should "allow an project-owner-access user to get a workspace" in withTestWorkspacesApiServicesAndUser(testData.userProjectOwner.userEmail.value) { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow an owner-access user to get a workspace" in withTestWorkspacesApiServicesAndUser(testData.userOwner.userEmail.value) { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a write-access user to get a workspace" in withTestWorkspacesApiServicesAndUser(testData.userWriter.userEmail.value) { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a read-access user to get a workspace" in withTestWorkspacesApiServicesAndUser(testData.userReader.userEmail.value) { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "not allow a no-access user to get a workspace" in withTestWorkspacesApiServicesAndUser("no-access") { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  // Update Workspace requires WRITE access.  Accept if OWNER or WRITE; Reject if READ or NO ACCESS

  it should "allow an project-owner-access user to update a workspace" in withTestDataApiServicesAndUser(testData.userProjectOwner.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "check that an update to a workspace modifies the last modified date" in withTestDataApiServicesAndUser(testData.userProjectOwner.userEmail.value) { services =>
    var mutableWorkspace: Workspace = testData.workspace.copy()
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        mutableWorkspace = responseAs[WorkspaceListResponse].workspace
      }

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        val updatedWorkspace: Workspace = responseAs[WorkspaceListResponse].workspace
        assertWorkspaceModifiedDate(status, updatedWorkspace)
        assert {
          updatedWorkspace.lastModified.isAfter(mutableWorkspace.lastModified)
        }
      }
  }

  it should "allow an owner-access user to update a workspace" in withTestDataApiServicesAndUser(testData.userOwner.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a write-access user to update a workspace" in withTestDataApiServicesAndUser(testData.userWriter.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "not allow a read-access user to update a workspace" in withTestDataApiServicesAndUser(testData.userReader.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "not allow a no-access user to update a workspace" in withTestDataApiServicesAndUser("no-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  // Put ACL requires OWNER access.  Accept if OWNER; Reject if WRITE, READ, NO ACCESS

  it should "allow a project-owner-access user to update an ACL" in withTestDataApiServicesAndUser(testData.userProjectOwner.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userProjectOwner.userEmail.value, WorkspaceAccessLevels.ProjectOwner, None), WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Read, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString ) { status }
      }

    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString ) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userWriter.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false))
      }
  }

  it should "allow an owner-access user to update an ACL" in withTestDataApiServicesAndUser(testData.userOwner.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userProjectOwner.userEmail.value, WorkspaceAccessLevels.ProjectOwner, None), WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Read, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString ) { status }
      }

    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString ) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userWriter.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false))
      }
  }

  it should "not allow ACL updates with a member specified twice" in withTestDataApiServicesAndUser(testData.userOwner.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userProjectOwner.userEmail.value, WorkspaceAccessLevels.ProjectOwner, None), WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Read, None), WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Owner, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest, response.entity.asString ) { status }
      }
  }

  it should "not allow an project-owner-access user to update an ACL with all users group" in withTestDataApiServicesAndUser(testData.userProjectOwner.userEmail.value) { services =>
    val allUsersEmail = RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(UserService.allUsersGroupRef.groupName))
    runAndWait(rawlsGroupQuery.save(RawlsGroup(UserService.allUsersGroupRef.groupName, allUsersEmail, Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])))
    WorkspaceAccessLevels.all.foreach { accessLevel =>
      Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(allUsersEmail.value, accessLevel, None)))) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) { status }
        }
    }
  }

  it should "not allow an owner-access user to update an ACL with all users group" in withTestDataApiServicesAndUser(testData.userOwner.userEmail.value) { services =>
    val allUsersEmail = RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(UserService.allUsersGroupRef.groupName))
    runAndWait(rawlsGroupQuery.save(RawlsGroup(UserService.allUsersGroupRef.groupName, allUsersEmail, Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])))
    WorkspaceAccessLevels.all.foreach { accessLevel =>
      Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(allUsersEmail.value, accessLevel, None)))) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) { status }
        }
    }
  }

  it should "not allow an owner-access user to downgrade project owner ACL" in withTestDataApiServicesAndUser(testData.userOwner.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userProjectOwner.userEmail.value, WorkspaceAccessLevels.Read, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest, response.entity.asString ) { status }
      }

    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString ) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userProjectOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true))

      }
  }

  it should "not allow an owner-access user to add project owner ACL" in withTestDataApiServicesAndUser(testData.userOwner.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.ProjectOwner, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) { status }
      }
  }

  it should "not allow a write-access user to update an ACL" in withTestDataApiServicesAndUser(testData.userWriter.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty[WorkspaceACLUpdate].toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  it should "not allow a read-access user to update an ACL" in withTestDataApiServicesAndUser(testData.userReader.userEmail.value) { services =>
    import WorkspaceACLJsonSupport._
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Read, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, response.entity.asString) { status }
      }
  }

  it should "not allow a no-access user to update an ACL" in withTestDataApiServicesAndUser("no-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty[WorkspaceACLUpdate].toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "allow an owner to grant share permissions to a non-owner" in withTestDataApiServicesAndUser("owner-access") { services =>
    import WorkspaceACLJsonSupport._
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, Option(true))))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, true))
      }
  }

  it should "allow an owner to revoke share permissions to a non-owner" in withTestDataApiServicesAndUser("owner-access") { services =>
    import WorkspaceACLJsonSupport._
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, Option(false))))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false))
      }
  }

  it should "allow a writer with share permissions to share equal to and below their access level" in withTestDataApiServicesAndUser("writer-access") { services =>
    import WorkspaceACLJsonSupport._
    Patch(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
      }
    Get(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false))
      }
  }

  it should "not allow a writer with share permissions to give permission above their own access level" in withTestDataApiServicesAndUser("writer-access") { services =>
    import WorkspaceACLJsonSupport._
    Patch(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Owner, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) { status }
      }
    Get(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[WorkspaceACL].acl should not contain (testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, false))
      }
  }

  it should "not allow a writer with share permissions to alter the permissions of users above their access level" in withTestDataApiServicesAndUser("writer-access") { services =>
    import WorkspaceACLJsonSupport._
    Patch(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userOwner.userEmail.value, WorkspaceAccessLevels.Read, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) { status }
      }
    Get(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.ProjectOwner, false, true))
      }
  }

  it should "allow a user in a group with share permissions to share equal to and below their access level" in withTestDataApiServicesAndUser("reader-access-via-group") { services =>
    import WorkspaceACLJsonSupport._
    Patch(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Get(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false))
      }
  }

  it should "not allow a non-owner to grant share permissions to anyone" in withTestDataApiServicesAndUser("writer-access") { services =>
    import WorkspaceACLJsonSupport._
    Patch(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, Option(true))))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) { status }
      }
    Get(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[WorkspaceACL].acl should not contain (testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, true))
      }
  }

  it should "granting and revoking share permissions should update accordingly" in withTestDataApiServicesAndUser("owner-access") { services =>
    import WorkspaceACLJsonSupport._

    Patch(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, Option(true))))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }

    Get(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, true))
      }

    Patch(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, Option(false))))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }

    Get(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false))
      }

    Patch(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, Option(true))))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }

    Get(s"/workspaces/${testData.workspaceToTestGrant.namespace}/${testData.workspaceToTestGrant.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[WorkspaceACL].acl should contain (testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, true))
      }
  }

  // Note that user writer-access has share permissions from another workspace- testData.workspaceToTestGrant
  // This is set up directly in the test data in TestDriverComponent
  it should "share permissions should not bleed across workspaces" in withTestDataApiServicesAndUser("writer-access") { services =>
    import WorkspaceACLJsonSupport._

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  // End ACL-restriction Tests

  // Workspace Locking
  it should "allow an owner to lock (and re-lock) the workspace" in withEmptyWorkspaceApiServices(testData.userOwner.userEmail.value) { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
  }

  it should "locking (and unlocking) a workspace should modify the workspace last modified date" in withEmptyWorkspaceApiServices(testData.userOwner.userEmail.value) { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }

    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/unlock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }

    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }

  }

  it should "not allow anyone to write to a workspace when locked"  in withLockedWorkspaceApiServices(testData.userWriter.userEmail.value) { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq.empty[AttributeUpdateOperation].toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  it should "allow a reader to read a workspace, even when locked"  in withLockedWorkspaceApiServices(testData.userReader.userEmail.value) { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "allow an owner to retrieve and adjust an the ACL, even when locked"  in withLockedWorkspaceApiServices(testData.userOwner.userEmail.value) { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty[WorkspaceACLUpdate].toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "not allow an owner to lock a workspace with incomplete submissions" in withTestDataApiServicesAndUser(testData.userOwner.userEmail.value) { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) { status }
      }
  }

  it should "allow an owner to unlock the workspace (repeatedly)" in withEmptyWorkspaceApiServices(testData.userOwner.userEmail.value) { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/unlock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/unlock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
  }

  it should "not allow a non-owner to lock or unlock the workspace" in withEmptyWorkspaceApiServices(testData.userWriter.userEmail.value) { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/unlock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  it should "not allow a no-access user to infer the existence of the workspace by locking or unlocking" in withLockedWorkspaceApiServices("no-access") { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/unlock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 403 creating workspace in billing project that does not exist" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = "foobar",
      name = "newWorkspace",
      None,
      Map.empty
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, response.entity.asString) {
          status
        }
      }
  }

  it should "return 403 creating workspace in billing project with no access" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = "project",
      name = "newWorkspace",
      None,
      Map.empty
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, response.entity.asString) {
          status
        }
      }
  }

  it should "use access groups as realmACLs when creating a workspace if there is no realm" in withTestDataApiServices { services =>
    val request = WorkspaceRequest(
      namespace = testData.billingProject.projectName.value,
      name = "newWorkspace",
      None,
      Map.empty
    )

    def expectedAccessGroups(workspaceId: String) = Map(
      WorkspaceAccessLevels.ProjectOwner -> RawlsGroup.toRef(testData.billingProject.groups(ProjectRoles.Owner)),
      WorkspaceAccessLevels.Owner -> RawlsGroupRef(RawlsGroupName(s"fc-$workspaceId-OWNER")),
      WorkspaceAccessLevels.Write -> RawlsGroupRef(RawlsGroupName(s"fc-$workspaceId-WRITER")),
      WorkspaceAccessLevels.Read -> RawlsGroupRef(RawlsGroupName(s"fc-$workspaceId-READER"))
    )

    Post(s"/workspaces", httpJson(request)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }

        val ws = responseAs[Workspace]
        val expected = expectedAccessGroups(ws.workspaceId)

        assertResult(expected) {
          ws.accessLevels
        }

        assertResult(expected) {
          ws.realmACLs
        }
      }
  }

  it should "create realmACLs when creating a workspace if there is a realm" in withTestDataApiServices { services =>
    val realmName = "testRealm"
    val realm = makeRawlsGroup(realmName, Set(testData.userOwner))

    val request = WorkspaceRequest(
      namespace = testData.billingProject.projectName.value,
      name = "newWorkspace",
      Option(RawlsRealmRef(realm.groupName)),
      Map.empty
    )

    def expectedAccessGroups(workspaceId: String) = Map(
      WorkspaceAccessLevels.ProjectOwner -> RawlsGroup.toRef(testData.billingProject.groups(ProjectRoles.Owner)),
      WorkspaceAccessLevels.Owner -> RawlsGroupRef(RawlsGroupName(s"fc-$workspaceId-OWNER")),
      WorkspaceAccessLevels.Write -> RawlsGroupRef(RawlsGroupName(s"fc-$workspaceId-WRITER")),
      WorkspaceAccessLevels.Read -> RawlsGroupRef(RawlsGroupName(s"fc-$workspaceId-READER"))
    )

    def expectedIntersectionGroups(workspaceId: String) = Map(
      WorkspaceAccessLevels.ProjectOwner -> RawlsGroupRef(RawlsGroupName(s"fc-$realmName-$workspaceId-PROJECT_OWNER")),
      WorkspaceAccessLevels.Owner -> RawlsGroupRef(RawlsGroupName(s"fc-$realmName-$workspaceId-OWNER")),
      WorkspaceAccessLevels.Write -> RawlsGroupRef(RawlsGroupName(s"fc-$realmName-$workspaceId-WRITER")),
      WorkspaceAccessLevels.Read -> RawlsGroupRef(RawlsGroupName(s"fc-$realmName-$workspaceId-READER"))
    )

    runAndWait(rawlsGroupQuery.save(realm))
    runAndWait(rawlsGroupQuery.setGroupAsRealm(RawlsRealmRef(realm.groupName)))

    Post(s"/workspaces", httpJson(request)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }

        val ws = responseAs[Workspace]

        assertResult(expectedAccessGroups(ws.workspaceId)) {
          ws.accessLevels
        }

        assertResult(expectedIntersectionGroups(ws.workspaceId)) {
          ws.realmACLs
        }
      }
  }

  it should "prevent users not in a realm from accessing workspace" in {
    import WorkspaceACLJsonSupport._

    val realmName = "testRealm"
    val realm = makeRawlsGroup(realmName, Set(testData.userOwner))

    val request = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      Option(RawlsRealmRef(realm.groupName)),
      Map.empty
    )
    val newSample = Entity("sampleNew", "sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))

    // called both where success and failure are expected to ensure that there are not just typos on the URLs
    def checkWorkspaceAccess(services: TestApiService, expectSuccess: Boolean): Unit = {
      Get(s"/workspaces/${request.namespace}/${request.name}/entities/${newSample.entityType}/${newSample.name}") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(if (expectSuccess) StatusCodes.OK else StatusCodes.NotFound) {
            status
          }
        }
      Get(s"/workspaces/${request.namespace}/${request.name}/entities") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(if (expectSuccess) StatusCodes.OK else StatusCodes.NotFound) {
            status
          }
        }
      Get(s"/workspaces/${request.namespace}/${request.name}") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(if (expectSuccess) StatusCodes.OK else StatusCodes.NotFound) {
            status
          }
        }
    }

    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource) { services =>
        runAndWait(rawlsGroupQuery.save(realm))
        runAndWait(rawlsGroupQuery.setGroupAsRealm(RawlsRealmRef(realm.groupName)))
        Post(s"/workspaces", httpJson(request)) ~>
          sealRoute(services.workspaceRoutes) ~>
          check {
            assertResult(StatusCodes.Created, response.entity.asString) {
              status
            }
          }
        Patch(s"/workspaces/${request.namespace}/${request.name}/acl", httpJson(Seq(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Write, None)))) ~>
          sealRoute(services.workspaceRoutes) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
          }

        Post(s"/workspaces/${request.namespace}/${request.name}/entities", httpJson(newSample)) ~>
          sealRoute(services.entityRoutes) ~>
          check {
            assertResult(StatusCodes.Created) {
              status
            }
          }
        checkWorkspaceAccess(services, true)
      }

      withApiServices(dataSource, testData.userWriter.userEmail.value) { services =>
        Get("/workspaces") ~>
          sealRoute(services.workspaceRoutes) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
            assertResult(Some(WorkspaceAccessLevels.NoAccess)) {
              responseAs[Array[WorkspaceListResponse]].find(r => r.workspace.toWorkspaceName == request.toWorkspaceName).map(_.accessLevel)
            }
          }
        checkWorkspaceAccess(services, false)
      }
    }
  }

  it should "return 200 when a user can read a workspace bucket" in withEmptyWorkspaceApiServices(testData.userReader.userEmail.value) { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/checkBucketReadAccess") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when a user can't read the bucket because they dont have workspace access" in withEmptyWorkspaceApiServices("no-access") { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/checkBucketReadAccess") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 404 when requesting bucket size for a non-existent workspace" in withTestWorkspacesApiServicesAndUser("reader-access") { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}x/bucketUsage") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 404 when a no-access user requests bucket usage" in withTestWorkspacesApiServicesAndUser("no-access") { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}/bucketUsage") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "not allow a reader-access user to request bucket usage" in withTestWorkspacesApiServicesAndUser("reader-access") { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}/bucketUsage") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  for (access <- Seq("owner-access", "writer-access")) {
    it should s"return 200 when workspace with $access requests bucket usage for an existing workspace" in withTestWorkspacesApiServicesAndUser(access) { services =>
      Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}/bucketUsage") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(BucketUsageResponse(BigInt(42))) {
            responseAs[BucketUsageResponse]
          }
        }
    }
  }
}

