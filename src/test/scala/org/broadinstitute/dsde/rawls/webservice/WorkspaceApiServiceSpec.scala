package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.http._
import spray.json._
import spray.routing._
import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.rawls.db.TestData

/**
 * Created by dvoet on 4/24/15.
 */
class WorkspaceApiServiceSpec extends ApiServiceSpec {
  trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
    val user: String
    def requireUserInfo(magnet: ImplicitMagnet[ExecutionContext]): Directive1[UserInfo] = {
      // just return the cookie text as the common name
      user match {
        case "owner-access" => provide(UserInfo(user, OAuth2BearerToken("token"), 123, "123456789876543212345"))
        case "write-access" => provide(UserInfo(user, OAuth2BearerToken("token"), 123, "123456789876543212346"))
        case "read-access" => provide(UserInfo(user, OAuth2BearerToken("token"), 123, "123456789876543212347"))
        case "no-access" => provide(UserInfo(user, OAuth2BearerToken("token"), 123, "123456789876543212348"))
        case _ => provide(UserInfo(user, OAuth2BearerToken("token"), 123, "123456789876543212349"))
      }
    }
  }

  case class TestApiService(dataSource: DataSource, user: String, gcsDAO: MockGoogleServicesDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectivesWithUser

  def withApiServices(dataSource: DataSource, user: String = "owner-access")(testCode: TestApiService => Any): Unit = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"))
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices(testCode: TestApiService => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def withTestDataApiServicesAndUser(user: String)(testCode: TestApiService => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      withApiServices(dataSource, user)(testCode)
    }
  }

  def withEmptyWorkspaceApiServices(user: String)(testCode: TestApiService => Any): Unit = {
    withCustomTestDatabase(new EmptyWorkspace) { dataSource =>
      withApiServices(dataSource, user)(testCode)
    }
  }

  def withLockedWorkspaceApiServices(user: String)(testCode: TestApiService => Any): Unit = {
    withCustomTestDatabase(new LockedWorkspace) { dataSource =>
      withApiServices(dataSource, user)(testCode)
    }
  }

  class TestWorkspaces() extends TestData {
    val userOwner = RawlsUser(UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345"))
    val userWriter = RawlsUser(UserInfo("writer-access", OAuth2BearerToken("token"), 123, "123456789876543212346"))
    val userReader = RawlsUser(UserInfo("reader-access", OAuth2BearerToken("token"), 123, "123456789876543212347"))

    val workspaceName = WorkspaceName("ns", "testworkspace")
    val workspaceOwnerGroup = makeRawlsGroup(s"${workspaceName} OWNER", Set(userOwner), Set.empty)
    val workspaceWriterGroup = makeRawlsGroup(s"${workspaceName} WRITER", Set(userWriter), Set.empty)
    val workspaceReaderGroup = makeRawlsGroup(s"${workspaceName} READER", Set(userReader), Set.empty)

    val workspace2Name = WorkspaceName("ns", "testworkspace2")
    val workspace2OwnerGroup = makeRawlsGroup(s"${workspace2Name} OWNER", Set.empty, Set.empty)
    val workspace2WriterGroup = makeRawlsGroup(s"${workspace2Name} WRITER", Set(userOwner), Set.empty)
    val workspace2ReaderGroup = makeRawlsGroup(s"${workspace2Name} READER", Set.empty, Set.empty)

    val workspace = Workspace(workspaceName.namespace, workspaceName.name, "workspaceId1", "bucket1", testDate, testDate, "testUser", Map("a" -> AttributeString("x")),
      Map(WorkspaceAccessLevels.Owner -> workspaceOwnerGroup,
        WorkspaceAccessLevels.Write -> workspaceWriterGroup,
        WorkspaceAccessLevels.Read -> workspaceReaderGroup))
    val workspace2 = Workspace(workspace2Name.namespace, workspace2Name.name, "workspaceId2", "bucket2", testDate, testDate, "testUser", Map("b" -> AttributeString("y")),
      Map(WorkspaceAccessLevels.Owner -> workspace2OwnerGroup,
        WorkspaceAccessLevels.Write -> workspace2WriterGroup,
        WorkspaceAccessLevels.Read -> workspace2ReaderGroup))

    val sample1 = Entity("sample1", "sample", Map.empty)
    val sample2 = Entity("sample2", "sample", Map.empty)
    val sample3 = Entity("sample3", "sample", Map.empty)
    val sampleSet = Entity("sampleset", "sample_set", Map("samples" -> AttributeEntityReferenceList(Seq(
      AttributeEntityReference(sample1.entityType, sample1.name),
      AttributeEntityReference(sample2.entityType, sample2.name),
      AttributeEntityReference(sample3.entityType, sample3.name)
    ))))

    val methodConfig = MethodConfiguration("dsde", "testConfig", "Sample", Map("ready"-> AttributeString("true")), Map("param1"-> AttributeString("foo")), Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")), MethodRepoMethod(workspaceName.namespace, "method-a", 1))
    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, workspaceName)
    val submissionTemplate = createTestSubmission(workspace, methodConfig, sampleSet, userOwner, Seq(sample1, sample2, sample3), Map(sample1 -> testData.inputResolutions, sample2 -> testData.inputResolutions, sample3 -> testData.inputResolutions))
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

    override def save(txn: RawlsTransaction): Unit = {
      authDAO.saveUser(userOwner, txn)
      authDAO.saveUser(userWriter, txn)
      authDAO.saveUser(userReader, txn)
      authDAO.saveGroup(workspaceOwnerGroup, txn)
      authDAO.saveGroup(workspaceWriterGroup, txn)
      authDAO.saveGroup(workspaceReaderGroup, txn)
      authDAO.saveGroup(workspace2OwnerGroup, txn)
      authDAO.saveGroup(workspace2WriterGroup, txn)
      authDAO.saveGroup(workspace2ReaderGroup, txn)

      workspaceDAO.save(workspace, txn)
      workspaceDAO.save(workspace2, txn)

      withWorkspaceContext(workspace, txn, bSkipLockCheck=true) { ctx =>
        entityDAO.save(ctx, sample1, txn)
        entityDAO.save(ctx, sample2, txn)
        entityDAO.save(ctx, sample3, txn)
        entityDAO.save(ctx, sampleSet, txn)

        methodConfigDAO.save(ctx, methodConfig, txn)

        submissionDAO.save(ctx, submissionSuccess, txn)
        submissionDAO.save(ctx, submissionFail, txn)
        submissionDAO.save(ctx, submissionRunning1, txn)
        submissionDAO.save(ctx, submissionRunning2, txn)
      }
    }
  }

  val testWorkspaces = new TestWorkspaces

  def withTestWorkspacesApiServices(testCode: TestApiService => Any): Unit = {
    withCustomTestDatabase(testWorkspaces) { dataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def withTestWorkspacesApiServicesAndUser(user: String)(testCode: TestApiService => Any): Unit = {
    withCustomTestDatabase(testWorkspaces) { dataSource =>
      withApiServices(dataSource, user)(testCode)
    }
  }

  "WorkspaceApi" should "return 201 for post to workspaces" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      Map.empty
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        services.dataSource.inTransaction() { txn =>
          assertResult(newWorkspace) {
            val ws = workspaceDAO.loadContext(newWorkspace.toWorkspaceName, txn).get.workspace
            WorkspaceRequest(ws.namespace, ws.name, ws.attributes)
          }
        }
        assertResult(newWorkspace) {
          val ws = responseAs[Workspace]
          WorkspaceRequest(ws.namespace, ws.name, ws.attributes)
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${newWorkspace.namespace}/${newWorkspace.name}"))))) {
          header("Location")
        }
      }
  }

  it should "get a workspace" in withTestWorkspacesApiServices { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val dateTime = org.joda.time.DateTime.now
        services.dataSource.inTransaction() { txn =>
          assertResult(
            WorkspaceListResponse(WorkspaceAccessLevels.Owner, testWorkspaces.workspace.copy(lastModified = dateTime), WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2), Seq("owner-access"))
          ){
            val response = responseAs[WorkspaceListResponse]
            WorkspaceListResponse(response.accessLevel, response.workspace.copy(lastModified = dateTime), response.workspaceSubmissionStats, response.owners)
          }
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
        assertResult(StatusCodes.Accepted) {
          status
        }
      }
      services.dataSource.inTransaction() { txn =>
        assertResult(None) {
          workspaceDAO.loadContext(testData.workspace.toWorkspaceName, txn)
        }
      }
  }

  it should "list workspaces" in withTestWorkspacesApiServices { services =>
    Get("/workspaces") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val dateTime = org.joda.time.DateTime.now
        services.dataSource.inTransaction() { txn =>
          assertResult(Set(
            WorkspaceListResponse(WorkspaceAccessLevels.Owner, testWorkspaces.workspace.copy(lastModified = dateTime), WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2), Seq("owner-access")),
            WorkspaceListResponse(WorkspaceAccessLevels.Write, testWorkspaces.workspace2.copy(lastModified = dateTime), WorkspaceSubmissionStats(None, None, 0), Seq.empty)
          )) {
            responseAs[Array[WorkspaceListResponse]].toSet[WorkspaceListResponse].map(wslr => wslr.copy(workspace = wslr.workspace.copy(lastModified = dateTime)))
          }
        }
      }
  }

  it should "return 404 Not Found on copy if the source workspace cannot be found" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/nonexistent/clone", httpJson(testData.workspace)) ~>
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
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on update workspace attributes" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          services.dataSource.inTransaction() { txn =>
            workspaceDAO.loadContext(testData.wsName, txn).get.workspace.attributes.get("boo")
          }
        }
      }

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute("boo"): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(None) {
          services.dataSource.inTransaction() { txn =>
            workspaceDAO.loadContext(testData.wsName, txn).get.workspace.attributes.get("boo")
          }
        }
      }
  }

  it should "copy a workspace if the source exists" in withTestDataApiServices { services =>
    val workspaceCopy = WorkspaceName(namespace = testData.workspace.namespace, name = "test_copy")
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName), writeLocks=Set(workspaceCopy)) { txn =>
          withWorkspaceContext(testData.workspace, txn) { sourceWorkspaceContext =>
            val copiedWorkspace = workspaceDAO.loadContext(workspaceCopy, txn).get.workspace
            assert(copiedWorkspace.attributes == testData.workspace.attributes)

            withWorkspaceContext(copiedWorkspace, txn) { copiedWorkspaceContext =>
              //Name, namespace, creation date, and owner might change, so this is all that remains.
              assertResult(entityDAO.listEntitiesAllTypes(sourceWorkspaceContext, txn).toSet) {
                entityDAO.listEntitiesAllTypes(copiedWorkspaceContext, txn).toSet
              }
              assertResult(methodConfigDAO.list(sourceWorkspaceContext, txn).toSet) {
                methodConfigDAO.list(copiedWorkspaceContext, txn).toSet
              }
            }
          }
        }

        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${workspaceCopy.namespace}/${workspaceCopy.name}"))))) {
          header("Location")
        }
      }
  }

  it should "return 409 Conflict on copy if the destination already exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(testData.workspace)) ~>
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
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when replacing an ACL on a non-existent workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/xyzzy/plugh/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  // Begin tests where routes are restricted by ACLs

  // Get Workspace requires READ access.  Accept if OWNER, WRITE, READ; Reject if NO ACCESS

  it should "allow an owner-access user to get a workspace" in withTestWorkspacesApiServicesAndUser("owner-access") { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a write-access user to get a workspace" in withTestWorkspacesApiServicesAndUser("write-access") { services =>
    Get(s"/workspaces/${testWorkspaces.workspace.namespace}/${testWorkspaces.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a read-access user to get a workspace" in withTestWorkspacesApiServicesAndUser("read-access") { services =>
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

  it should "allow an owner-access user to update a workspace" in withTestDataApiServicesAndUser("owner-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute("boo"): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a write-access user to update a workspace" in withTestDataApiServicesAndUser("write-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute("boo"): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "not allow a read-access user to update a workspace" in withTestDataApiServicesAndUser("read-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute("boo"): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "not allow a no-access user to update a workspace" in withTestDataApiServicesAndUser("no-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", httpJson(Seq(RemoveAttribute("boo"): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  // Put ACL requires OWNER access.  Accept if OWNER; Reject if WRITE, READ, NO ACCESS

  it should "allow an owner-access user to update an ACL" in withTestDataApiServicesAndUser("owner-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "not allow an owner-access user to update an ACL with all users group" in withTestDataApiServicesAndUser("owner-access") { services =>
    val allUsersEmail = RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(UserService.allUsersGroupRef.groupName))
    services.dataSource.inTransaction() { txn =>
      containerDAO.authDAO.saveGroup(RawlsGroup(UserService.allUsersGroupRef.groupName, allUsersEmail, Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef]), txn)
    }
    import WorkspaceACLJsonSupport._
    WorkspaceAccessLevels.all.foreach { accessLevel =>
      Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq(WorkspaceACLUpdate(allUsersEmail.value, accessLevel)).toJson.toString)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) { status }
        }
    }
  }

  it should "not allow a write-access user to update an ACL" in withTestDataApiServicesAndUser("write-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  it should "not allow a read-access user to update an ACL" in withTestDataApiServicesAndUser("read-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  it should "not allow a no-access user to update an ACL" in withTestDataApiServicesAndUser("no-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  // End ACL-restriction Tests

  // Workspace Locking
  it should "allow an owner to lock (and re-lock) the workspace" in withEmptyWorkspaceApiServices("owner-access") { services =>
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

  it should "not allow anyone to write to a workspace when locked"  in withLockedWorkspaceApiServices("write-access") { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  it should "allow a reader to read a workspace, even when locked"  in withLockedWorkspaceApiServices("read-access") { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "allow an owner to retrieve and adjust an the ACL, even when locked"  in withLockedWorkspaceApiServices("owner-access") { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "not allow an owner to lock a workspace with incomplete submissions" in withTestDataApiServicesAndUser("owner-access") { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) { status }
      }
  }

  it should "allow an owner to unlock the workspace (repeatedly)" in withEmptyWorkspaceApiServices("owner-access") { services =>
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

  it should "not allow a non-owner to lock or unlock the workspace" in withEmptyWorkspaceApiServices("write-access") { services =>
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

  it should "return 400 creating workspace in billing project that does not exist" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = "foobar",
      name = "newWorkspace",
      Map.empty
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest, response.entity.asString) {
          status
        }
      }
  }

  it should "return 403 creating workspace in billing project with no access" in withTestDataApiServices { services =>
    services.dataSource.inTransaction() { txn =>
      billingDAO.saveProject(RawlsBillingProject(RawlsBillingProjectName("foobar"), Set.empty, "mockBucketUrl"), txn)
    }
    val newWorkspace = WorkspaceRequest(
      namespace = "foobar",
      name = "newWorkspace",
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
}
