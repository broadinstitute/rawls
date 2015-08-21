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
import org.broadinstitute.dsde.rawls.dataaccess.{GraphMethodConfigurationDAO, GraphEntityDAO, GraphWorkspaceDAO, HttpMethodRepoDAO, HttpExecutionServiceDAO, DataSource}
import AttributeUpdateOperations._
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
      new GraphWorkspaceDAO(),
      new GraphSubmissionDAO(new GraphWorkflowDAO()),
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      new GraphWorkflowDAO(),
      new GraphEntityDAO(),
      new GraphMethodConfigurationDAO(),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, workspaceDAO, entityDAO, methodConfigDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), MockGoogleCloudStorageDAO, submissionSupervisor, submissionDAO)_

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

  it should "list workspaces" in withTestDataApiServices { services =>
    Get("/workspaces") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
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

  it should "return 404 on Entity CRUD when workspace does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities", HttpEntity(ContentTypes.`application/json`, testData.sample2.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 on create entity" in withTestDataApiServices { services =>
    val wsName = WorkspaceName(testData.workspace.namespace,testData.workspace.name)
    val newSample = Entity("sampleNew", "sample", Map("type" -> AttributeString("tumor")))

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", HttpEntity(ContentTypes.`application/json`, newSample.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(newSample) {
              entityDAO.get(workspaceContext, newSample.entityType, newSample.name, txn).get
            }
          }
        }
        assertResult(newSample) {
          responseAs[Entity]
        }

        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(newSample.path(wsName)))))) {
          header("Location")
        }
      }
  }

  it should "return 409 conflict on create entity when entity exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", HttpEntity(ContentTypes.`application/json`, testData.sample2.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 400 when batch upserting an entity with invalid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(RemoveListMember("bingo", AttributeString("a"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", HttpEntity(ContentTypes.`application/json`, Seq(update1).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        assertResult(1) {
          responseAs[Array[String]].length
        }
      }
  }

  it should "return 204 when batch upserting an entity that does not yet exist" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition("newSample", "Sample", Seq(AddUpdateAttribute("newAttribute", AttributeString("foo"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", HttpEntity(ContentTypes.`application/json`, Seq(update1).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(Some(Entity("newSample", "Sample", Map("newAttribute" -> AttributeString("foo"))))) {
              entityDAO.get(workspaceContext, "Sample", "newSample", txn)
            }
          }
        }
      }
  }

  it should "return 204 when batch upserting an entity with valid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute("newAttribute", AttributeString("bar"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq.empty)
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", HttpEntity(ContentTypes.`application/json`, Seq(update1, update2).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + ("newAttribute" -> AttributeString("bar"))))) {
              entityDAO.get(workspaceContext, testData.sample1.entityType, testData.sample1.name, txn)
            }
          }
        }
      }
  }

  it should "return 400 when batch updating an entity with invalid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(RemoveListMember("bingo", AttributeString("a"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", HttpEntity(ContentTypes.`application/json`, Seq(update1).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        assertResult(1) {
          responseAs[Array[String]].length
        }
      }
  }

  it should "return 400 when batch updating an entity that does not yet exist" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition("superDuperNewSample", "Samples", Seq(AddUpdateAttribute("newAttribute", AttributeString("foo"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", HttpEntity(ContentTypes.`application/json`, Seq(update1).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        assertResult(1) {
          responseAs[Array[String]].length
        }
      }
  }

  it should "return 204 when batch updating an entity with valid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute("newAttribute", AttributeString("bar"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", HttpEntity(ContentTypes.`application/json`, Seq(update1).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + ("newAttribute" -> AttributeString("bar"))))) {
              entityDAO.get(workspaceContext, testData.sample1.entityType, testData.sample1.name, txn)
            }
          }
        }
      }
  }

  it should "return 200 on get entity" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(testData.sample2) {
              entityDAO.get(workspaceContext, testData.sample2.entityType, testData.sample2.name, txn).get
            }
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            val entityTypes = entityDAO.getEntityTypes(workspaceContext, txn)
            assertResult(entityTypes) {
              responseAs[Array[String]]
            }
          }
        }
      }
  }

  it should "return 200 on list all samples" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            val samples = entityDAO.list(workspaceContext, testData.sample2.entityType, txn).toSet
            assertResult(samples) {
              responseAs[Array[Entity]].toSet
            }
          }
        }
      }
  }

  it should "return 404 on non-existing entity" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}x") ~>
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

  it should "return 200 on update entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          services.dataSource.inTransaction { txn =>
            withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
              entityDAO.get(workspaceContext, testData.sample2.entityType, testData.sample2.name, txn).get.attributes.get("boo")
            }
          }
        }
      }
  }

  it should "return 200 on remove attribute from entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("bar"): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(None) {
          services.dataSource.inTransaction { txn =>
            withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
              entityDAO.get(workspaceContext, testData.sample2.entityType, testData.sample2.name, txn).get.attributes.get("bar")
            }
          }
        }
      }
  }

  it should "return 404 on update to non-existing entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}x", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 400 on remove from an attribute that is not a list" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveListMember("foo", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on remove from list attribute that does not exist" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveListMember("grip", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on add to list attribute that is not a list" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample1.entityType}/${testData.sample1.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddListMember("somefoo", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 409 on entity rename when rename already exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}/rename", HttpEntity(ContentTypes.`application/json`, EntityName("sample1").toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on entity rename" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}/rename", HttpEntity(ContentTypes.`application/json`, EntityName("s2_changed").toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(true) {
              entityDAO.get(workspaceContext, testData.sample2.entityType, "s2_changed", txn).isDefined
            }
          }
        }
      }
  }

  it should "return 404 on entity rename, entity does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/foox/rename", HttpEntity(ContentTypes.`application/json`, EntityName("s2_changed").toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(None) {
              entityDAO.get(workspaceContext, testData.sample2.entityType, "s2_changed", txn)
            }
          }
        }
      }
  }

  it should "return 204 entity delete" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(None) {
              entityDAO.get(workspaceContext, testData.sample2.entityType, testData.sample2.name, txn)
            }
          }
        }
      }
  }
  it should "return 404 entity delete, entity does not exist" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/s2_changed") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on successfully parsing an expression" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/SampleSet/sset1/evaluate", HttpEntity(ContentTypes.`application/json`, "this.samples.type")) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 201 on create method configuration" in withTestDataApiServices { services =>
    val newMethodConfig = MethodConfiguration("dsde", "testConfigNew", "samples", Map("ready" -> AttributeString("true")), Map("param1" -> AttributeString("foo")), Map("out" -> AttributeString("bar")),
      MethodRepoConfiguration(testData.wsName.namespace+"_config", "method-a", "1"), MethodRepoMethod(testData.wsName.namespace, "method-a", "1"))

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, newMethodConfig.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(newMethodConfig) {
              methodConfigDAO.get(workspaceContext, newMethodConfig.namespace, newMethodConfig.name, txn).get
            }
          }
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(newMethodConfig.path(testData.wsName)))))) {
          header("Location")
        }
      }
  }

  it should "return 409 on method configuration rename when rename already exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName(testData.methodConfig.name, testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on method configuration rename" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName("testConfig2_changed", testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(true) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, "testConfig2_changed", txn).isDefined
            }
            assertResult(None) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn)
            }
          }
        }
      }
  }

  it should "return 404 on method configuration rename, method configuration does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/foox/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName(testData.methodConfig.name, testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(true) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).isDefined
            }
            assertResult(None) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, "foox", txn)
            }
          }
        }
      }
  }

  it should "return 204 method configuration delete" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(None) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn)
            }
          }
        }
      }
  }
  it should "return 404 method configuration delete, method configuration does not exist" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}x") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }

        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(true) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).isDefined
            }
            assertResult(None) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, "foox", txn)
            }
          }
        }
      }
  }

  it should "return 200 on update method configuration" in withTestDataApiServices { services =>
    val modifiedMethodConfig = testData.methodConfig.copy(inputs = testData.methodConfig.inputs + ("param2" -> AttributeString("foo2")))
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}", HttpEntity(ContentTypes.`application/json`, modifiedMethodConfig.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(modifiedMethodConfig) {
          responseAs[MethodConfiguration]
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(Option(AttributeString("foo2"))) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.inputs.get("param2")
            }
          }
        }
      }
  }

  it should "return 404 on update method configuration" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/update}", HttpEntity(ContentTypes.`application/json`, testData.methodConfig.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 on copy method configuration" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairCreated.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult("testConfig1") {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.name
            }
          }
        }
      }
  }

  it should "return 409 on copy method configuration to existing name" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairConflict.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairNotFound.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
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
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult("testConfig1") {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.name
            }
          }
        }
      }
  }

  it should "return 409 on copy method configuration from method repo to existing name" in withTestDataApiServices { services =>
    val existingMethodConfigCopy = MethodRepoConfigurationQuery("workspace_test", "rawls_test_good", "1", testData.methodConfigName)
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, existingMethodConfigCopy.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source in method repo" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoMissing.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is missing" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoEmptyPayload.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is unparseable" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoBadPayload.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 200 on get method configuration" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "list method Configuration" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            val configs = methodConfigDAO.list(workspaceContext, txn).toSet
            assertResult(configs) {
              responseAs[Array[MethodConfigurationShort]].toSet
            }
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

  it should "return 404 Not Found when creating a submission using a MethodConfiguration that doesn't exist in the workspace" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", HttpEntity(ContentTypes.`application/json`, SubmissionRequest("dsde","not there","Pattern","pattern1", None).toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.submissionRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 404 Not Found when creating a submission using an Entity that doesn't exist in the workspace" in withTestDataApiServices { services =>
    val mcName = MethodConfigurationName("three_step_1","dsde", testData.wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name,"Pattern", Map.empty, Map("pattern"->AttributeString("String")), Map.empty, MethodRepoConfiguration("dsde_config","three_step","1"), MethodRepoMethod("dsde","three_step","1"))
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, methodConf.toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check { assertResult(StatusCodes.Created) {status} }
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", HttpEntity(ContentTypes.`application/json`, SubmissionRequest(mcName.namespace, mcName.name,"Pattern","pattern1", None).toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.submissionRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  private def createAndMonitorSubmission(wsName: WorkspaceName, methodConf: MethodConfiguration,
                                         submissionEntity: Entity, submissionExpression: Option[String],
                                          services: TestApiService): Submission = {
      Post(s"/workspaces/${wsName.namespace}/${wsName.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, methodConf.toJson.toString)) ~>
        addMockOpenAmCookie ~>
        sealRoute(services.methodConfigRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      val submissionRq = SubmissionRequest(methodConf.namespace, methodConf.name, submissionEntity.entityType, submissionEntity.name, submissionExpression)
      Post(s"/workspaces/${wsName.namespace}/${wsName.name}/submissions", HttpEntity(ContentTypes.`application/json`, submissionRq.toJson.toString)) ~>
        addMockOpenAmCookie ~>
        sealRoute(services.submissionRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          val submission = responseAs[Submission]
          Get(s"/workspaces/${wsName.namespace}/${wsName.name}/submissions/${submission.submissionId}") ~>
            addMockOpenAmCookie ~>
            sealRoute(services.submissionRoutes) ~>
            check {
              assertResult(StatusCodes.OK) {
                status
              }
              return responseAs[Submission]
            }
        }

    fail("Unable to create and monitor submissions")
  }

  it should "return 201 Created when creating and monitoring a submission with no expression" in withTestDataApiServices { services =>
  val wsName = testData.wsName
    val mcName = MethodConfigurationName("three_step", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoConfiguration("dsde_config", "three_step", "1"), MethodRepoMethod("dsde", "three_step", "1"))

    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sample1, None, services)

    assertResult(1) {
      submission.workflows.size
    }
  }
  it should "return 201 Created when creating and monitoring a submission with valid expression" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("three_step", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoConfiguration("dsde_config", "three_step", "1"), MethodRepoMethod("dsde", "three_step", "1"))

    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sset1, Option("this.samples"), services)

    assertResult(3) {
      submission.workflows.size
    }
  }

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val z1 = Entity("z1", "Sample", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList))
  val workspace2Name = new WorkspaceName(testData.wsName.namespace + "2", testData.wsName.name + "2")
  val workspace2Request = WorkspaceRequest(
    workspace2Name.namespace,
    workspace2Name.name,
    Map.empty
  )

  it should "return 201 for copying entities into a workspace with no conflicts" in withTestDataApiServices { services =>
    Post("/workspaces", HttpEntity(ContentTypes.`application/json`, workspace2Request.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          assertResult(workspace2Request) {
            val ws = workspaceDAO.load(workspace2Request.toWorkspaceName, txn).get
            WorkspaceRequest(ws.namespace, ws.name, ws.attributes)
          }
        }

        Post(s"/workspaces/${workspace2Request.namespace}/${workspace2Request.name}/entities", HttpEntity(ContentTypes.`application/json`, z1.toJson.toString())) ~>
          addMockOpenAmCookie ~>
          sealRoute(services.entityRoutes) ~>
          check {
            assertResult(StatusCodes.Created) {
              status
            }
            services.dataSource.inTransaction { txn =>
              val workspaceContext = workspaceDAO.loadContext(workspace2Name, txn).get
              assertResult(z1) {
                entityDAO.get(workspaceContext, z1.entityType, z1.name, txn).get
              }
            }

            val sourceWorkspace = WorkspaceName(workspace2Request.namespace, workspace2Request.name)
            val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("z1"))
            Post("/workspaces/entities/copy", HttpEntity(ContentTypes.`application/json`, entityCopyDefinition.toJson.toString())) ~>
              addMockOpenAmCookie ~>
              sealRoute(services.entityRoutes) ~>
              check {
                assertResult(StatusCodes.Created) {
                  status
                }
                services.dataSource.inTransaction { txn =>
                  withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
                    assertResult(z1) {
                      entityDAO.get(workspaceContext, z1.entityType, z1.name, txn).get
                    }
                  }
                }
              }
          }
      }
  }

  it should "return 409 for copying entities into a workspace with conflicts" in withTestDataApiServices { services =>
    val sourceWorkspace = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("sample1"))
    Post("/workspaces/entities/copy", HttpEntity(ContentTypes.`application/json`, entityCopyDefinition.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 200 on getting a submission" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/${testData.submission1.submissionId}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(testData.submission1) {responseAs[Submission]}
      }
  }

  it should "return 404 on getting a nonexistent submission" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/unrealSubmission42") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {status}
      }
  }

  it should "return 200 when listing submissions" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(List(testData.submissionTerminateTest, testData.submission1, testData.submission2)) {
          responseAs[List[Submission]]
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
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when replacing an ACL on a non-existent workspace" in withTestDataApiServices { services =>
    Put(s"/workspaces/xyzzy/plugh/acl", HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
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
        assertResult(StatusCodes.NotFound) {
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
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addOpenAmCookie("owner-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "not allow a write-access user to update an ACL" in withTestDataApiServices { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addOpenAmCookie("write-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "not allow a read-access user to update an ACL" in withTestDataApiServices { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addOpenAmCookie("read-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "not allow a no-access user to update an ACL" in withTestDataApiServices { services =>
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", HttpEntity(ContentTypes.`text/plain`,"{\"acl\": []}")) ~>
      addOpenAmCookie("no-access") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  // End ACL-restriction Tests

  it should "not allow dots in user-defined strings" in withTestDataApiServices { services =>
    val dotSample = Entity("sample.with.dots.in.name", "sample", Map("type" -> AttributeString("tumor")))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", HttpEntity(ContentTypes.`application/json`, dotSample.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) { status }
      }
  }

}
