package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, GraphEntityDAO, GraphMethodConfigurationDAO, GraphWorkspaceDAO, HttpExecutionServiceDAO, HttpMethodRepoDAO, _}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockOpenAmDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{FlatSpec, Matchers}
import spray.http.HttpHeaders.Cookie
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest
import scala.collection.immutable.HashMap
import scala.concurrent.duration._

/**
 * Created by dvoet on 4/24/15.
 */
class WorkspaceApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with OrientDbTestFixture {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  // these tokens won't work for login to remote services: that requires a password and is therefore limited to the integration test
  def addOpenAmCookie(token: String) = addHeader(Cookie(HttpCookie("iPlanetDirectoryPro", token)))
  def addMockOpenAmCookie = addOpenAmCookie("test_token")

  def actorRefFactory = system

  val mockServer = RemoteServicesMockServer()

  override def beforeAll() = {
    super.beforeAll
    mockServer.startServer
  }

  override def afterAll() = {
    super.afterAll
    mockServer.stopServer
  }

  case class TestApiService(dataSource: DataSource) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with GoogleAuthApiService with MockOpenAmDirectives {
    def actorRefFactory = system

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, containerDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), MockGoogleCloudStorageDAO, submissionSupervisor)_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  def withApiServices(dataSource: DataSource)(testCode: TestApiService => Any): Unit = {
    val apiService = new TestApiService(dataSource)
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

  class TestWorkspaces() extends TestData {
    val writerWorkspaceName = WorkspaceName("ns", "writer")
    val workspaceOwner = Workspace("ns", "owner", "aBucket", testDate, "testUser", Map("a" -> AttributeString("x")) )
    val workspaceWriter = Workspace(writerWorkspaceName.namespace, writerWorkspaceName.name, "aBucket", testDate, "testUser", Map("b" -> AttributeString("y")) )
    val workspaceReader = Workspace("ns", "reader", "aBucket", testDate, "testUser", Map("c" -> AttributeString("z")) )
    val workspaceNoAccess = Workspace("ns", "noaccess", "aBucket", testDate, "testUser", Map("d" -> AttributeString("afterz")) )

    val sample1 = Entity("sample1", "sample", Map.empty)
    val sample2 = Entity("sample2", "sample", Map.empty)
    val sample3 = Entity("sample3", "sample", Map.empty)
    val sampleSet = Entity("sampleset", "sample_set", Map("samples" -> AttributeEntityReferenceList(Seq(
      AttributeEntityReference(sample1.entityType, sample1.name),
      AttributeEntityReference(sample2.entityType, sample2.name),
      AttributeEntityReference(sample3.entityType, sample3.name)
    ))))

    val methodConfig = MethodConfiguration("dsde", "testConfig", "Sample", Map("ready"-> AttributeString("true")), Map("param1"-> AttributeString("foo")), Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")), MethodRepoConfiguration(writerWorkspaceName.namespace+"-config", "method-a-config", "1"), MethodRepoMethod(writerWorkspaceName.namespace, "method-a", "1"))
    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, writerWorkspaceName)
    val submissionTemplate = createTestSubmission(workspaceWriter, methodConfig, sampleSet, Seq(sample1, sample2, sample3))
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
      workspaceDAO.save(workspaceOwner, txn)
      workspaceDAO.save(workspaceWriter, txn)
      workspaceDAO.save(workspaceReader, txn)
      workspaceDAO.save(workspaceNoAccess, txn)

      withWorkspaceContext(workspaceWriter, txn) { ctx =>
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

  "WorkspaceApi" should "return 201 for post to workspaces" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = "newNamespace",
      name = "newWorkspace",
      Map.empty
    )

    Post(s"/workspaces", HttpEntity(ContentTypes.`application/json`, newWorkspace.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(newWorkspace) {
            val ws = workspaceDAO.load(newWorkspace.toWorkspaceName, txn).get
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

  it should "get a workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(testData.workspace) {
            workspaceDAO.load(testData.wsName, txn).get
          }
        }
        assertResult(testData.workspace) {
          responseAs[Workspace]
        }
      }
  }

  it should "return 404 getting a non-existent workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "delete a workspace" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted) {
          status
        }
      }
      services.dataSource.inTransaction { txn =>
        assertResult(None) {
          workspaceDAO.load(testData.workspace.toWorkspaceName, txn)
        }
      }
  }

  it should "list workspaces" in withTestWorkspacesApiServices { services =>     Get("/workspaces") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(Set(
            WorkspaceListResponse(WorkspaceAccessLevel.Owner, testWorkspaces.workspaceOwner, WorkspaceSubmissionStats(None, None, 0), MockGoogleCloudStorageDAO.getOwners(testWorkspaces.workspaceOwner.toWorkspaceName)),
            WorkspaceListResponse(WorkspaceAccessLevel.Write, testWorkspaces.workspaceWriter, WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2), MockGoogleCloudStorageDAO.getOwners(testWorkspaces.workspaceOwner.toWorkspaceName)),
            WorkspaceListResponse(WorkspaceAccessLevel.Read, testWorkspaces.workspaceReader, WorkspaceSubmissionStats(None, None, 0), MockGoogleCloudStorageDAO.getOwners(testWorkspaces.workspaceOwner.toWorkspaceName))
          )) {
            responseAs[Array[WorkspaceListResponse]].toSet
          }
        }
      }
  }

  it should "return 404 Not Found on copy if the source workspace cannot be found" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/nonexistent/clone", HttpEntity(ContentTypes.`application/json`, testData.workspace.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on update workspace attributes" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          services.dataSource.inTransaction { txn =>
            workspaceDAO.load(testData.wsName, txn).get.attributes.get("boo")
          }
        }
      }

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(None) {
          services.dataSource.inTransaction { txn =>
            workspaceDAO.load(testData.wsName, txn).get.attributes.get("boo")
          }
        }
      }
  }

  it should "copy a workspace if the source exists" in withTestDataApiServices { services =>
    val workspaceCopy = WorkspaceName(namespace = testData.workspace.namespace, name = "test_copy")
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { sourceWorkspaceContext =>
            val copiedWorkspace = workspaceDAO.load(workspaceCopy, txn).get
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
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", HttpEntity(ContentTypes.`application/json`, testData.workspace.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 200 when requesting an ACL from an existing workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when requesting an ACL from a non-existent workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/xyzzy/plugh/acl") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 when replacing an ACL for an existing workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when replacing an ACL on a non-existent workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/xyzzy/plugh/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  // Begin tests where routes are restricted by ACLs

  // Get Workspace requires READ access.  Accept if OWNER, WRITE, READ; Reject if NO ACCESS

  it should "allow an owner-access user to get a workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addOpenAmCookie("owner-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a write-access user to get a workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addOpenAmCookie("write-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a read-access user to get a workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addOpenAmCookie("read-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "not allow a no-access user to get a workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addOpenAmCookie("no-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  // Update Workspace requires WRITE access.  Accept if OWNER or WRITE; Reject if READ or NO ACCESS

  it should "allow an owner-access user to update a workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addOpenAmCookie("owner-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a write-access user to update a workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addOpenAmCookie("write-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "not allow a read-access user to update a workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addOpenAmCookie("read-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "not allow a no-access user to update a workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addOpenAmCookie("no-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  // Put ACL requires OWNER access.  Accept if OWNER; Reject if WRITE, READ, NO ACCESS

  it should "allow an owner-access user to update an ACL" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      addOpenAmCookie("owner-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "not allow a write-access user to update an ACL" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      addOpenAmCookie("write-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  it should "not allow a read-access user to update an ACL" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      addOpenAmCookie("read-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  it should "not allow a no-access user to update an ACL" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`application/json`, Seq.empty.toJson.toString)) ~>
      addOpenAmCookie("no-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  // End ACL-restriction Tests

}
