package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID
import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockOpenAmDirectives
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.dataaccess.{GraphMethodConfigurationDAO, GraphEntityDAO, GraphWorkspaceDAO, HttpMethodRepoDAO, HttpExecutionServiceDAO, DataSource}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperations._
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.http.HttpHeaders.Cookie
import spray.http._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest
import spray.json._
import spray.httpx.SprayJsonSupport
import SprayJsonSupport._
import WorkspaceJsonSupport._
import ExecutionJsonSupport.SubmissionRequestFormat
import ExecutionJsonSupport.SubmissionFormat
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
      new GraphSubmissionDAO(new GraphWorkflowDAO()),
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      new GraphWorkflowDAO(),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, workspaceDAO, entityDAO, methodConfigDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), MockGoogleCloudStorageDAO, submissionSupervisor, submissionDAO)

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

  "WorkspaceApi" should "return 201 for post to workspaces" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = "newNamespace",
      name = "newWorkspace",
      Map.empty
    )

    Post(s"/workspaces", HttpEntity(ContentTypes.`application/json`, newWorkspace.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.postWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(newWorkspace) {
            val ws = workspaceDAO.load(newWorkspace.namespace, newWorkspace.name, txn).get
            WorkspaceRequest(ws.namespace,ws.name,ws.attributes)
          }
        }
        assertResult(newWorkspace) {
          val ws = responseAs[Workspace]
          WorkspaceRequest(ws.namespace,ws.name,ws.attributes)
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${newWorkspace.namespace}/${newWorkspace.name}"))))) {
          header("Location")
        }
      }
  }

  it should "get a workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.getWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        services.dataSource.inTransaction { txn =>
          assertResult(testData.workspace) {
            workspaceDAO.load(testData.workspace.namespace, testData.workspace.name, txn).get
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
      sealRoute(services.getWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "list workspaces" in withTestDataApiServices { services =>
    Get("/workspaces") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.listWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(workspaceDAO.list(txn).toSet) {
            responseAs[Array[Workspace]].toSet
          }
        }
      }
  }

  it should "return 404 Not Found on copy if the source workspace cannot be found" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/nonexistent/clone", HttpEntity(ContentTypes.`application/json`, testData.workspace.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.getEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.deleteEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 on Entity CRUD when workspace does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities", HttpEntity(ContentTypes.`application/json`, testData.sample2.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.createEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 on create entity" in withTestDataApiServices { services =>
    val newSample = Entity("sampleNew", "sample", Map("type" -> AttributeString("tumor")), testData.wsName)

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", HttpEntity(ContentTypes.`application/json`, newSample.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.createEntityRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        services.dataSource.inTransaction { txn =>
          assertResult(newSample) {
            entityDAO.get(testData.workspace.namespace, testData.workspace.name, newSample.entityType, newSample.name, txn).get
          }
        }
        assertResult(newSample) {
          responseAs[Entity]
        }

        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/${newSample.path}"))))) {
          header("Location")
        }
      }
  }

  it should "return 409 conflict on create entity when entity exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", HttpEntity(ContentTypes.`application/json`, testData.sample2.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.createEntityRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 200 on get entity" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.getEntityRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        services.dataSource.inTransaction { txn =>
          assertResult(testData.sample2) {
            entityDAO.get(testData.workspace.namespace, testData.workspace.name, testData.sample2.entityType, testData.sample2.name, txn).get
          }
        }
        assertResult(testData.sample2) {
          responseAs[Entity]
        }
      }
  }

  it should "return 200 on list entity types" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.listEntityTypesRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        services.dataSource.inTransaction { txn =>
          val entityTypes = entityDAO.getEntityTypes(testData.workspace.namespace, testData.workspace.name, txn)

          assertResult(entityTypes) {
            responseAs[Array[String]]
          }
        }
      }
  }

  it should "return 200 on list all samples" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.listEntitiesPerTypeRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        services.dataSource.inTransaction { txn =>
          val samples = entityDAO.list(testData.workspace.namespace, testData.workspace.name, testData.sample2.entityType, txn).toSet

          assertResult(samples) {
            responseAs[Array[Entity]].toSet
          }
        }
      }
  }

  it should "return 404 on non-existing entity" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}x") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.getEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on update workspace attributes" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          services.dataSource.inTransaction { txn =>
            workspaceDAO.load(testData.workspace.namespace, testData.workspace.name, txn).get.attributes.get("boo")
          }
        }
      }

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(None) {
          services.dataSource.inTransaction { txn =>
            workspaceDAO.load(testData.workspace.namespace, testData.workspace.name, txn).get.attributes.get("boo")
          }
        }
      }
  }

  it should "return 200 on update entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          services.dataSource.inTransaction { txn =>
            entityDAO.get(testData.workspace.namespace, testData.workspace.name, testData.sample2.entityType, testData.sample2.name, txn).get.attributes.get("boo")
          }
        }
      }
  }

  it should "return 200 on remove attribute from entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("bar"): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(None) {
          services.dataSource.inTransaction { txn =>
            entityDAO.get(testData.workspace.namespace, testData.workspace.name, testData.sample2.entityType, testData.sample2.name, txn).get.attributes.get("bar")
          }
        }
      }
  }

  it should "return 404 on update to non-existing entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}x", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 400 on remove from an attribute that is not a list" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveListMember("foo", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on remove from list attribute that does not exist" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveListMember("grip", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on add to list attribute that is not a list" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample1.entityType}/${testData.sample1.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddListMember("somefoo", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 409 on entity rename when rename already exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}/rename", HttpEntity(ContentTypes.`application/json`, EntityName("sample1").toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.renameEntityRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on entity rename" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}/rename", HttpEntity(ContentTypes.`application/json`, EntityName("s2_changed").toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.renameEntityRoute) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(true) {
            entityDAO.get(testData.workspace.namespace, testData.workspace.name, testData.sample2.entityType, "s2_changed", txn).isDefined
          }
        }
      }
  }

  it should "return 404 on entity rename, entity does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/foox/rename", HttpEntity(ContentTypes.`application/json`, EntityName("s2_changed").toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.renameEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(None) {
            entityDAO.get(testData.workspace.namespace, testData.workspace.name, testData.sample2.entityType, "s2_changed", txn)
          }
        }
      }
  }

  it should "return 204 entity delete" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.deleteEntityRoute) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(None) {
            entityDAO.get(testData.workspace.namespace, testData.workspace.name, testData.sample2.entityType, testData.sample2.name, txn)
          }
        }
      }
  }
  it should "return 404 entity delete, entity does not exist" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/s2_changed") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.deleteEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on successfully parsing an expression" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/SampleSet/sset1/evaluate", HttpEntity(ContentTypes.`application/json`, "this.samples.type")) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.evaluateExpressionRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Array("normal", "tumor", "tumor")) {
          responseAs[Array[String]]
        }
      }
  }

  it should "return 400 on failing to parse an expression" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/SampleSet/sset1/evaluate", HttpEntity(ContentTypes.`application/json`, "nonexistent.anything")) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.evaluateExpressionRoute) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 201 on create method configuration" in withTestDataApiServices { services =>
    val newMethodConfig = MethodConfiguration("dsde", "testConfig2", "samples", Map("ready" -> "true"), Map("param1" -> "foo"), Map("out" -> "bar"),
      testData.wsName, MethodStoreConfiguration(testData.wsName.namespace+"_config", "method-a", "1"), MethodStoreMethod(testData.wsName.namespace, "method-a", "1"))

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, newMethodConfig.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.createMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(newMethodConfig) {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, newMethodConfig.namespace, newMethodConfig.name, txn).get
          }
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/${newMethodConfig.path}"))))) {
          header("Location")
        }
      }
  }

  it should "return 409 on method configuration rename when rename already exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName(testData.methodConfig.name, testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.renameMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on method configuration rename" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName("testConfig2_changed", testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.renameMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(true) {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, testData.methodConfig.namespace, "testConfig2_changed", txn).isDefined
          }
          assertResult(None) {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, testData.methodConfig.namespace, testData.methodConfig.name, txn)
          }
        }
      }
  }

  it should "return 404 on method configuration rename, method configuration does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/foox/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName(testData.methodConfig.name, testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.renameMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(true) {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, testData.methodConfig.namespace, testData.methodConfig.name, txn).isDefined
          }
          assertResult(None) {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, testData.methodConfig.namespace, "foox", txn)
          }
        }
      }
  }

  it should "return 204 method configuration delete" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.deleteMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(None) {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, testData.methodConfig.namespace, testData.methodConfig.name, txn)
          }
        }
      }
  }
  it should "return 404 method configuration delete, method configuration does not exist" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}x") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.deleteMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }

        services.dataSource.inTransaction { txn =>
          assertResult(true) {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, testData.methodConfig.namespace, testData.methodConfig.name, txn).isDefined
          }
          assertResult(None) {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, testData.methodConfig.namespace, "foox", txn)
          }
        }
      }
  }

  it should "return 200 on update method configuration" in withTestDataApiServices { services =>
    val modifiedMethodConfig = testData.methodConfig.copy(inputs = testData.methodConfig.inputs + ("param2" -> "foo2"))
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}", HttpEntity(ContentTypes.`application/json`, modifiedMethodConfig.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(Option("foo2")) {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.inputs.get("param2")
          }
        }
      }
  }

  it should "return 404 on update method configuration" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/update}", HttpEntity(ContentTypes.`application/json`, testData.methodConfig.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.updateMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 on copy method configuration" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairCreated.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult("testConfig1") {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.name
          }
        }
      }
  }

  it should "return 409 on copy method configuration to existing name" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairConflict.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairNotFound.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  val copyFromMethodRepo = "/methodconfigs/copyFromMethodRepo"

  it should "return 201 on copy method configuration from method repo" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoGood.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult("testConfig1") {
            methodConfigDAO.get(testData.workspace.namespace, testData.workspace.name, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.name
          }
        }
      }
  }

  it should "return 409 on copy method configuration from method repo to existing name" in withTestDataApiServices { services =>
    val existingMethodConfigCopy = MethodRepoConfigurationQuery("workspace_test", "rawls_test_good", "1", testData.methodConfigName)
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, existingMethodConfigCopy.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source in method repo" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoMissing.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is missing" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoEmptyPayload.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is unparseable" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoBadPayload.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 200 on get method configuration" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.getMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "list method Configuration" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.listMethodConfigurationsRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        services.dataSource.inTransaction { txn =>
          val configs = methodConfigDAO.list(testData.workspace.namespace, testData.workspace.name, txn).toSet
          assertResult(configs) {
            responseAs[Array[MethodConfigurationShort]].toSet
          }
        }
      }
  }


  it should "copy a workspace if the source exists" in withTestDataApiServices { services =>
    val workspaceCopy = WorkspaceName(namespace = testData.workspace.namespace, name = "test_copy")
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          val copiedWorkspace = workspaceDAO.load(workspaceCopy.namespace, workspaceCopy.name, txn).get

          //Name, namespace, creation date, and owner might change, so this is all that remains.
          assert(copiedWorkspace.attributes == testData.workspace.attributes)
          assertResult(entityDAO.listEntitiesAllTypes(testData.workspace.namespace, testData.workspace.name, txn).toSet) {
            entityDAO.listEntitiesAllTypes(workspaceCopy.namespace, workspaceCopy.name, txn) map {
              _.copy(workspaceName = WorkspaceName(testData.workspace.namespace, testData.workspace.name))
            } toSet
          }
          assertResult(methodConfigDAO.list(testData.workspace.namespace, testData.workspace.name, txn).toSet) {
            methodConfigDAO.list(workspaceCopy.namespace, workspaceCopy.name, txn) map {
              _.copy(workspaceName = WorkspaceName(testData.workspace.namespace, testData.workspace.name))
            } toSet
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
      sealRoute(services.copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 Not Found when creating a submission using a MethodConfiguration that doesn't exist in the workspace" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", HttpEntity(ContentTypes.`application/json`,SubmissionRequest("dsde","not there","Pattern","pattern1",None).toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.submissionRoute) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 404 Not Found when creating a submission using an Entity that doesn't exist in the workspace" in withTestDataApiServices { services =>
    val mcName = MethodConfigurationName("three_step_1","dsde",testData.wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name,"Pattern",Map.empty,Map("pattern"->"String"),Map.empty,mcName.workspaceName, MethodStoreConfiguration("dsde_config","three_step","1"), MethodStoreMethod("dsde","three_step","1"))
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`,methodConf.toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.createMethodConfigurationRoute) ~>
      check { assertResult(StatusCodes.Created) {status} }
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", HttpEntity(ContentTypes.`application/json`,SubmissionRequest(mcName.namespace,mcName.name,"Pattern","pattern1",None).toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.submissionRoute) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 201 Created when creating a submission" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("three_step","dsde",wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name,"Pattern",Map.empty,Map.empty,Map.empty, wsName, MethodStoreConfiguration("dsde_config","three_step","1"), MethodStoreMethod("dsde","three_step","1"))
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`,methodConf.toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.createMethodConfigurationRoute) ~>
      check { assertResult(StatusCodes.Created) {status} }
    val entity = Entity("pattern1","Pattern",Map("pattern"->AttributeString("hello")),wsName)
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/entities", HttpEntity(ContentTypes.`application/json`,entity.toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.createEntityRoute) ~>
      check { assertResult(StatusCodes.Created) {status} }
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", HttpEntity(ContentTypes.`application/json`,SubmissionRequest(mcName.namespace,mcName.name,"Pattern","pattern1",None).toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.submissionRoute) ~>
      check { assertResult(StatusCodes.Created) {status} }
  }

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val z1 = Entity("z1", "Sample", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList), testData.wsName)
  val workspace2Name = new WorkspaceName(testData.wsName.namespace + "2", testData.wsName.name + "2")
  val workspace2 = WorkspaceRequest(
    workspace2Name.namespace,
    workspace2Name.name,
    Map.empty
  )

  it should "return 201 for copying entities into a workspace with no conflicts" in withTestDataApiServices { services =>
    Post("/workspaces", HttpEntity(ContentTypes.`application/json`, workspace2.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.postWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(workspace2) {
            val ws = workspaceDAO.load(workspace2.namespace, workspace2.name, txn).get
            WorkspaceRequest(ws.namespace,ws.name,ws.attributes)
          }
        }


        Post(s"/workspaces/${workspace2.namespace}/${workspace2.name}/entities", HttpEntity(ContentTypes.`application/json`, z1.toJson.toString())) ~>
          addMockOpenAmCookie ~>
          sealRoute(services.createEntityRoute) ~>
          check {
            assertResult(StatusCodes.Created) {
              status
            }
            services.dataSource.inTransaction { txn =>
              assertResult(z1.copy(workspaceName = workspace2Name)) {
                entityDAO.get(workspace2.namespace, workspace2.name, z1.entityType, z1.name, txn).get
              }
            }

            val sourceWorkspace = WorkspaceName(workspace2.namespace, workspace2.name)
            val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("z1"))
            Post("/entities/copy", HttpEntity(ContentTypes.`application/json`, entityCopyDefinition.toJson.toString())) ~>
              addMockOpenAmCookie ~>
              sealRoute(services.copyEntitiesRoute) ~>
              check {
                assertResult(StatusCodes.Created) {
                  status
                }
                services.dataSource.inTransaction { txn =>
                  assertResult(z1.copy(workspaceName = testData.wsName)) {
                    entityDAO.get(testData.workspace.namespace, testData.workspace.name, z1.entityType, z1.name, txn).get.copy(workspaceName = testData.wsName)
                  }
                }
              }
          }
      }
  }

  it should "return 409 for copying entities into a workspace with conflicts" in withTestDataApiServices { services =>
    val sourceWorkspace = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("sample1"))
    Post("/entities/copy", HttpEntity(ContentTypes.`application/json`, entityCopyDefinition.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.copyEntitiesRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 200 on getting a submission" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/${testData.submission1.id}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.getStatusRoute) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(testData.submission1) {responseAs[Submission]}
      }
  }

  it should "return 404 on getting a nonexistent submission" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/unrealSubmission42") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.getStatusRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {status}
      }
  }

  it should "return 200 when requesting an ACL from an existing workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.getACLRoute) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when requesting an ACL from a non-existent workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/xyzzy/plugh/acl") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.getACLRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 when replacing an ACL for an existing workspace" in withTestDataApiServices { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl",HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.putACLRoute) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when replacing an ACL on a non-existent workspace" in withTestDataApiServices { services =>
    Put(s"/workspaces/xyzzy/plugh/acl",HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.putACLRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  // Begin tests where routes are restricted by ACLs

  // Get Workspace requires READ access.  Accept if OWNER, WRITE, READ; Reject if NO ACCESS

  it should "allow an owner-access user to get a workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addOpenAmCookie("owner-access") ~>
      sealRoute(services.getWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a write-access user to get a workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addOpenAmCookie("write-access") ~>
      sealRoute(services.getWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a read-access user to get a workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addOpenAmCookie("read-access") ~>
      sealRoute(services.getWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "not allow a no-access user to get a workspace" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      addOpenAmCookie("no-access") ~>
      sealRoute(services.getWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  // Update Workspace requires WRITE access.  Accept if OWNER or WRITE; Reject if READ or NO ACCESS

  it should "allow an owner-access user to update a workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addOpenAmCookie("owner-access") ~>
      sealRoute(services.updateWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a write-access user to update a workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addOpenAmCookie("write-access") ~>
      sealRoute(services.updateWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "not allow a read-access user to update a workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addOpenAmCookie("read-access") ~>
      sealRoute(services.updateWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "not allow a no-access user to update a workspace" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addOpenAmCookie("no-access") ~>
      sealRoute(services.updateWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  // Put ACL requires OWNER access.  Accept if OWNER; Reject if WRITE, READ, NO ACCESS

  it should "allow an owner-access user to update an ACL" in withTestDataApiServices { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl",HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addOpenAmCookie("owner-access") ~>
      sealRoute(services.putACLRoute) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "not allow a write-access user to update an ACL" in withTestDataApiServices { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl",HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addOpenAmCookie("write-access") ~>
      sealRoute(services.putACLRoute) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  it should "not allow a read-access user to update an ACL" in withTestDataApiServices { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl",HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addOpenAmCookie("read-access") ~>
      sealRoute(services.putACLRoute) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  it should "not allow a no-access user to update an ACL" in withTestDataApiServices { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl",HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addOpenAmCookie("no-access") ~>
      sealRoute(services.putACLRoute) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
  }

  // End ACL-restriction Tests


}
