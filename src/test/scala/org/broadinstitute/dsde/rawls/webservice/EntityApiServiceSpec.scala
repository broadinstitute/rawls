package org.broadinstitute.dsde.rawls.webservice

import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, GraphEntityDAO, GraphMethodConfigurationDAO, GraphWorkspaceDAO, HttpExecutionServiceDAO, HttpMethodRepoDAO, _}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{FlatSpec, Matchers}
import spray.http.HttpHeaders.Cookie
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * Created by dvoet on 4/24/15.
 */
class EntityApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with OrientDbTestFixture {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

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

  case class TestApiService(dataSource: DataSource)(implicit val executionContext: ExecutionContext) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with MockUserInfoDirectives {
    def actorRefFactory = system

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, containerDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), new MockGoogleServicesDAO, submissionSupervisor)_

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

  "EntityApi" should "return 404 on Entity CRUD when workspace does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities", HttpEntity(ContentTypes.`application/json`, testData.sample2.toJson.toString())) ~>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        assertResult(1) {
          responseAs[ErrorReport].causes.length
        }
      }
  }

  it should "return 204 when batch upserting an entity that does not yet exist" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition("newSample", "Sample", Seq(AddUpdateAttribute("newAttribute", AttributeString("foo"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", HttpEntity(ContentTypes.`application/json`, Seq(update1).toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        assertResult(1) {
          responseAs[ErrorReport].causes.length
        }
      }
  }

  it should "return 400 when batch updating an entity that does not yet exist" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition("superDuperNewSample", "Samples", Seq(AddUpdateAttribute("newAttribute", AttributeString("foo"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", HttpEntity(ContentTypes.`application/json`, Seq(update1).toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        assertResult(1) {
          responseAs[ErrorReport].causes.length
        }
      }
  }

  it should "return 204 when batch updating an entity with valid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute("newAttribute", AttributeString("bar"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", HttpEntity(ContentTypes.`application/json`, Seq(update1).toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on update entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
            withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
              entityDAO.get(workspaceContext, testData.sample2.entityType, testData.sample2.name, txn).get.attributes.get("boo")
            }
          }
        }
      }
  }

  it should "return 200 on remove attribute from entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("bar"): AttributeUpdateOperation).toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(None) {
          services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
            withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
              entityDAO.get(workspaceContext, testData.sample2.entityType, testData.sample2.name, txn).get.attributes.get("bar")
            }
          }
        }
      }
  }

  it should "return 404 on update to non-existing entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}x", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 400 on remove from an attribute that is not a list" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveListMember("foo", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on remove from list attribute that does not exist" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveListMember("grip", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on add to list attribute that is not a list" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample1.entityType}/${testData.sample1.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddListMember("somefoo", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 409 on entity rename when rename already exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}/rename", HttpEntity(ContentTypes.`application/json`, EntityName("sample1").toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on entity rename" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}/rename", HttpEntity(ContentTypes.`application/json`, EntityName("s2_changed").toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on successfully parsing an expression" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/SampleSet/sset1/evaluate", HttpEntity(ContentTypes.`application/json`, "this.samples.type")) ~>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val z1 = Entity("z1", "Sample", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList))
  val workspace2Name = new WorkspaceName(testData.wsName.namespace, testData.wsName.name + "2")
  val workspace2Request = WorkspaceRequest(
    workspace2Name.namespace,
    workspace2Name.name,
    Map.empty
  )

  it should "return 201 for copying entities into a workspace with no conflicts" in withTestDataApiServices { services =>
    Post("/workspaces", HttpEntity(ContentTypes.`application/json`, workspace2Request.toJson.toString())) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction(readLocks=Set(workspace2Request.toWorkspaceName)) { txn =>
          assertResult(workspace2Request) {
            val ws = workspaceDAO.loadContext(workspace2Request.toWorkspaceName, txn).get.workspace
            WorkspaceRequest(ws.namespace, ws.name, ws.attributes)
          }
        }

        Post(s"/workspaces/${workspace2Request.namespace}/${workspace2Request.name}/entities", HttpEntity(ContentTypes.`application/json`, z1.toJson.toString())) ~>
              sealRoute(services.entityRoutes) ~>
          check {
            assertResult(StatusCodes.Created) {
              status
            }
            services.dataSource.inTransaction(readLocks=Set(workspace2Name)) { txn =>
              val workspaceContext = workspaceDAO.loadContext(workspace2Name, txn).get
              assertResult(z1) {
                entityDAO.get(workspaceContext, z1.entityType, z1.name, txn).get
              }
            }

            val sourceWorkspace = WorkspaceName(workspace2Request.namespace, workspace2Request.name)
            val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("z1"))
            Post("/workspaces/entities/copy", HttpEntity(ContentTypes.`application/json`, entityCopyDefinition.toJson.toString())) ~>
                      sealRoute(services.entityRoutes) ~>
              check {
                assertResult(StatusCodes.Created) {
                  status
                }
                services.dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
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
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "not allow dots in user-defined strings" in withTestDataApiServices { services =>
    val dotSample = Entity("sample.with.dots.in.name", "sample", Map("type" -> AttributeString("tumor")))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", HttpEntity(ContentTypes.`application/json`, dotSample.toJson.toString())) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) { status }
      }
  }

}
