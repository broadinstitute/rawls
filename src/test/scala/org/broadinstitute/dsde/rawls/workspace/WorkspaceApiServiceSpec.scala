package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.mock.MethodRepoMockServer
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.{MethodConfigApiService, EntityApiService, WorkspaceApiService, JobApiService}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperations._
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.http.HttpHeaders.Cookie
import spray.http._
import spray.testkit.ScalatestRouteTest
import spray.json._
import spray.httpx.SprayJsonSupport
import SprayJsonSupport._
import WorkspaceJsonSupport._
import scala.concurrent.duration._

/**
 * Created by dvoet on 4/24/15.
 */
class WorkspaceApiServiceSpec extends FlatSpec with WorkspaceApiService with EntityApiService with MethodConfigApiService with JobApiService with ScalatestRouteTest with Matchers with OrientDbTestFixture {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  // this token won't work for login to remote services: that requires a password and is therefore limited to the integration test
  def addMockOpenAmCookie = addHeader(Cookie(HttpCookie("iPlanetDirectoryPro", "test_token")))

  def actorRefFactory = system

  override val testDbName = "WorkspaceApiServiceTest"

  val wsns = "namespace"
  val wsname = UUID.randomUUID().toString

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val s1 = Entity("s1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList), WorkspaceName(wsns, wsname))
  val s2 = Entity("s2", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList), WorkspaceName(wsns, wsname))

  val c1 = Entity("c1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList, "cycle1" -> AttributeReferenceSingle("samples", "c2")), WorkspaceName(wsns, wsname))
  val c2 = Entity("c2", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList, "cycle2" -> AttributeReferenceSingle("samples", "c3")), WorkspaceName(wsns, wsname))
  val c3 = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList, "cycle3" -> AttributeReferenceSingle("samples", "c1")), WorkspaceName(wsns, wsname))

  val methodConfig = MethodConfiguration("testConfig", "samples", wsns, "method-a", "1", Map("ready"-> "true"), Map("param1"-> "foo"), Map("out" -> "bar"), WorkspaceName(wsns, wsname), "dsde")
  val methodConfig2 = MethodConfiguration("testConfig2", "samples", wsns, "method-a", "1", Map("ready"-> "true"), Map("param1"-> "foo"), Map("out" -> "bar"), WorkspaceName(wsns, wsname), "dsde")
  val methodConfig3 = MethodConfiguration("testConfig", "samples", wsns, "method-a", "1", Map("ready"-> "true"), Map("param1"-> "foo", "param2"-> "foo2"), Map("out" -> "bar"), WorkspaceName(wsns, wsname), "dsde")
  val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, methodConfig.workspaceName)
  val methodConfigName2 = methodConfigName.copy(name="novelName")
  val methodConfigName3 = methodConfigName.copy(name="noSuchName")
  val methodConfigNamePairCreated = MethodConfigurationNamePair(methodConfigName,methodConfigName2)
  val methodConfigNamePairConflict = MethodConfigurationNamePair(methodConfigName,methodConfigName)
  val methodConfigNamePairNotFound = MethodConfigurationNamePair(methodConfigName3,methodConfigName2)

  val uniqueMethodConfigName = UUID.randomUUID.toString
  val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, methodConfig.namespace, methodConfig.workspaceName)
  val methodRepoGood = MethodRepoConfigurationQuery("workspace_test", "rawls_test_good", "1", newMethodConfigName)
  val methodRepoMissing = MethodRepoConfigurationQuery("workspace_test", "rawls_test_missing", "1", methodConfigName)
  val methodRepoEmptyPayload = MethodRepoConfigurationQuery("workspace_test", "rawls_test_empty_payload", "1", methodConfigName)
  val methodRepoBadPayload = MethodRepoConfigurationQuery("workspace_test", "rawls_test_bad_payload", "1", methodConfigName)

  val workspace = Workspace(
    wsns,
    wsname,
    DateTime.now().withMillis(0),
    "test",
    Map.empty
  )


  val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, MockWorkspaceDAO, MockEntityDAO, MockMethodConfigurationDAO, new HttpMethodRepoDAO(MethodRepoMockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(MethodRepoMockServer.mockServerBaseUrl))
  val dao = MockWorkspaceDAO

  initializeTestGraph()

  override def beforeAll() = MethodRepoMockServer.startServer
  override def afterAll() = MethodRepoMockServer.stopServer

  "WorkspaceApi" should "return 201 for post to workspaces" in {
    Post(s"/workspaces", HttpEntity(ContentTypes.`application/json`, workspace.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(postWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(workspace) {
          MockWorkspaceDAO.store((workspace.namespace, workspace.name))
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${workspace.namespace}/${workspace.name}"))))) {
          header("Location")
        }
      }
  }

  it should "get a workspace" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(getWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(MockWorkspaceDAO.load(workspace.namespace, workspace.name, null).get) {
          responseAs[Workspace]
        }
      }

  }

  it should "return 404 getting a non-existent workspace" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}x") ~>
      addMockOpenAmCookie ~>
      sealRoute(getWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }

  }

  it should "list workspaces" in {
    Get("/workspaces") ~>
      addMockOpenAmCookie ~>
      sealRoute(listWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(MockWorkspaceDAO.store.values.toSet) {
          responseAs[Array[Workspace]].toSet
        }
      }

  }

  val workspaceCopy = WorkspaceName(namespace = workspace.namespace, name = "test_copy")

  it should "return 404 Not Found on copy if the source workspace cannot be found" in {
    Post(s"/workspaces/${workspace.namespace}/nonexistent/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}x/entities/${s2.entityType}/${s2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(getEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Patch(s"/workspaces/${workspace.namespace}/${workspace.name}x/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Delete(s"/workspaces/${workspace.namespace}/${workspace.name}x/entities/${s2.entityType}/${s2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(deleteEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 on Entity CRUD when workspace does not exist" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}x/entities", HttpEntity(ContentTypes.`application/json`, s2.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(createEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }

  }

  it should "return 201 on create entity" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities", HttpEntity(ContentTypes.`application/json`, s2.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(createEntityRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(s2) {
          MockEntityDAO.store(workspace.namespace, workspace.name)(s2.entityType, s2.name)
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/${s2.path}"))))) {
          header("Location")
        }
      }
  }
  it should "return 409 conflict on create entity when entity exists" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities", HttpEntity(ContentTypes.`application/json`, s2.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(createEntityRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 200 on get entity" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(getEntityRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(s2) {
          responseAs[Entity]
        }
      }
  }

  it should "return 200 on list entity types" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}/entities") ~>
      addMockOpenAmCookie ~>
      sealRoute(listEntityTypesRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Array("samples")) {
          responseAs[Array[String]]
        }
      }
  }

  it should "return 200 on list all samples" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}") ~>
      addMockOpenAmCookie ~>
      sealRoute(listEntitiesPerTypeRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Array(s2)) {
          responseAs[Array[Entity]]
        }
      }
  }

  it should "return 404 on non-existing entity" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}x") ~>
      addMockOpenAmCookie ~>
      sealRoute(getEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on update workspace attributes" in {
    Patch(s"/workspaces/${workspace.namespace}/${workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          MockWorkspaceDAO.store(workspace.namespace, workspace.name).attributes.get("boo")
        }
      }

    Patch(s"/workspaces/${workspace.namespace}/${workspace.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("boo"): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(None) {
          MockWorkspaceDAO.store(workspace.namespace, workspace.name).attributes.get("boo")
        }
      }
  }

  it should "return 200 on update entity" in {
    Patch(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          MockEntityDAO.store(workspace.namespace, workspace.name)(s2.entityType, s2.name).attributes.get("boo")
        }
      }
  }

  it should "return 200 on remove attribute from entity" in {
    Patch(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("bar"): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(None) {
          MockEntityDAO.store(workspace.namespace, workspace.name)(s2.entityType, s2.name).attributes.get("bar")
        }
      }
  }

  it should "return 404 on update to non-existing entity" in {
    Patch(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}x", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 400 on remove from an attribute that is not a list" in {
    Patch(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveListMember("foo", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on remove from list attribute that does not exist" in {
    Patch(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveListMember("grip", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on add to list attribute that is not a list" in {
    Patch(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddListMember("foo", AttributeString("adsf")): AttributeUpdateOperation).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 409 on entity rename when rename already exists" in {
    MockEntityDAO.save(workspace.namespace, workspace.name, s1, null)
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}/rename", HttpEntity(ContentTypes.`application/json`, EntityName("s1").toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(renameEntityRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on entity rename" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}/rename", HttpEntity(ContentTypes.`application/json`, EntityName("s2_changed").toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(renameEntityRoute) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(true) {
          MockEntityDAO.store(workspace.namespace, workspace.name).get(s2.entityType, "s2_changed").isDefined
        }
      }
  }

  it should "return 404 on entity rename, entity does not exist" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}/rename", HttpEntity(ContentTypes.`application/json`, EntityName("s2_changed").toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(renameEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        assertResult(true) {
          MockEntityDAO.store(workspace.namespace, workspace.name).get(s2.entityType, "s2_changed").isDefined
        }
      }
  }

  it should "return 204 entity delete" in {
    Delete(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/s2_changed") ~>
      addMockOpenAmCookie ~>
      sealRoute(deleteEntityRoute) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          MockEntityDAO.store(workspace.namespace, workspace.name).get(s2.entityType, s2.name)
        }
      }
  }
  it should "return 404 entity delete, entity does not exist" in {
    Delete(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/s2_changed") ~>
      addMockOpenAmCookie ~>
      sealRoute(deleteEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on successfully parsing an expression" in {
    Post(s"/workspaces/workspaces/test_workspace/entities/SampleSet/sset1/evaluate", HttpEntity(ContentTypes.`application/json`, "this.samples.type")) ~>
      addMockOpenAmCookie ~>
      sealRoute(evaluateExpressionRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Array("normal", "tumor", "tumor")) {
          responseAs[Array[String]]
        }
      }
  }

  it should "return 400 on failing to parse an expression" in {
    Post(s"/workspaces/workspaces/test_workspace/entities/SampleSet/sset1/evaluate", HttpEntity(ContentTypes.`application/json`, "nonexistent.anything")) ~>
      addMockOpenAmCookie ~>
      sealRoute(evaluateExpressionRoute) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 201 on create method configuration" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, methodConfig.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(createMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(methodConfig) {
          MockMethodConfigurationDAO.store(workspace.namespace, workspace.name)(methodConfig.namespace, methodConfig.name)
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/${methodConfig.path}"))))) {
          header("Location")
        }
      }
  }

  it should "return 409 on method configuration rename when rename already exists" in {
    MockMethodConfigurationDAO.save(workspace.namespace, workspace.name, methodConfig2, null)

    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/${methodConfig.namespace}/${methodConfig.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName(methodConfig2.name, methodConfig2.namespace, WorkspaceName(workspace.namespace, workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(renameMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on method configuration rename" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/${methodConfig2.namespace}/${methodConfig2.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName("testConfig2_changed", methodConfig2.namespace, WorkspaceName(workspace.namespace, workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(renameMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(true) {
          MockMethodConfigurationDAO.store(workspace.namespace, workspace.name).get(methodConfig2.namespace, "testConfig2_changed").isDefined
        }
        assertResult(None) {
          MockMethodConfigurationDAO.store(workspace.namespace, workspace.name).get(methodConfig2.namespace, methodConfig2.name)
        }
      }
  }

  it should "return 404 on method configuration rename, method configuration does not exist" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/${methodConfig.namespace}/${methodConfig2.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName("testConfig2_changed", methodConfig.namespace, WorkspaceName(workspace.namespace, workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(renameMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        assertResult(true) {
          MockMethodConfigurationDAO.store(workspace.namespace, workspace.name).get(methodConfig2.namespace, "testConfig2_changed").isDefined
        }
      }
  }

  it should "return 204 method configuration delete" in {
    Delete(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/${methodConfig2.namespace}/testConfig2_changed") ~>
      addMockOpenAmCookie ~>
      sealRoute(deleteMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          MockMethodConfigurationDAO.store(workspace.namespace, workspace.name).get(methodConfig2.namespace, "testConfig2_changed")
        }
      }
  }
  it should "return 404 method configuration delete, method configuration does not exist" in {
    Delete(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/${methodConfig.namespace}/${methodConfig.name}x") ~>
      addMockOpenAmCookie ~>
      sealRoute(deleteMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on update method configuration" in {
    Put(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/${methodConfig3.namespace}/${methodConfig3.name}", HttpEntity(ContentTypes.`application/json`, methodConfig3.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Option("foo2")) {
          MockMethodConfigurationDAO.store(workspace.namespace, workspace.name)(methodConfig3.namespace, methodConfig3.name).inputs.get("param2")
        }
      }
  }

  it should "return 404 on update method configuration" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/update}", HttpEntity(ContentTypes.`application/json`, methodConfig2.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(updateMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 on copy method configuration" in {
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, methodConfigNamePairCreated.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult("testConfig") {
          MockMethodConfigurationDAO.store(workspace.namespace, workspace.name)(methodConfig.namespace, methodConfig.name).name
        }
      }
  }

  it should "return 409 on copy method configuration to existing name" in {
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, methodConfigNamePairConflict.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source" in {
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, methodConfigNamePairNotFound.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  val copyFromMethodRepo = "/methodconfigs/copyFromMethodRepo"

  it should "return 201 on copy method configuration from method repo" in {
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, methodRepoGood.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult("testConfig") {
          MockMethodConfigurationDAO.store(workspace.namespace, workspace.name)(methodConfig.namespace, methodConfig.name).name
        }
      }
  }

  it should "return 409 on copy method configuration from method repo to existing name" in {
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, methodRepoGood.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source in method repo" in {
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, methodRepoMissing.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.InternalServerError) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is missing" in {
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, methodRepoEmptyPayload.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is unparseable" in {
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, methodRepoBadPayload.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 200 on get method configuration" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/${methodConfig.namespace}/${methodConfig.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(getMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "list method Configuration" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs") ~>
      addMockOpenAmCookie ~>
      sealRoute(listMethodConfigurationsRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(MockMethodConfigurationDAO.store(workspace.namespace, workspace.name).values.map(mc =>
          MethodConfigurationShort(mc.name, mc.rootEntityType, mc.methodNamespace, mc.methodName, mc.methodVersion, mc.workspaceName, mc.namespace)).toSet) {
          responseAs[Array[MethodConfigurationShort]].toSet
        }
      }
  }

  it should "copy a workspace if the source exists" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        val copiedWorkspace = MockWorkspaceDAO.store((workspaceCopy.namespace, workspaceCopy.name))

        //Name, namespace, creation date, and owner might change, so this is all that remains.
        assert(copiedWorkspace.attributes == workspace.attributes)
        assertResult(MockEntityDAO.listEntitiesAllTypes(workspace.namespace, workspace.name, null).toSet) {
          MockEntityDAO.listEntitiesAllTypes(workspaceCopy.namespace, workspaceCopy.name, null) map {_.copy(workspaceName=WorkspaceName(workspace.namespace, workspace.name))} toSet
        }
        assertResult(MockMethodConfigurationDAO.list(workspace.namespace, workspace.name, null).toSet) {
          MockMethodConfigurationDAO.list(workspaceCopy.namespace, workspaceCopy.name, null).toSet
        }
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${workspaceCopy.namespace}/${workspaceCopy.name}"))))) {
          header("Location")
        }
      }
  }

  it should "return 409 Conflict on copy if the destination already exists" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 Not Found when submitting a job using a MethodConfiguration that doesn't exist in the workspace" in {
    Post(s"/workspaces/${wsns}/${wsname}/jobs", HttpEntity(ContentTypes.`application/json`,JobDescription("dsde","not there","Pattern","pattern1").toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(submitJobRoute) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 404 Not Found when submitting a job using an Entity that doesn't exist in the workspace" in {
    val mcName = MethodConfigurationName("three_step_1","dsde",WorkspaceName(wsns,wsname))
    val methodConf = MethodConfiguration(mcName.name,"Pattern","dsde","three_step","1",Map.empty,Map("pattern"->"String"),Map.empty,mcName.workspaceName,mcName.namespace)
    Post(s"/workspaces/${wsns}/${wsname}/methodconfigs", HttpEntity(ContentTypes.`application/json`,methodConf.toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(createMethodConfigurationRoute) ~>
      check { assertResult(StatusCodes.Created) {status} }
    Post(s"/workspaces/${wsns}/${wsname}/jobs", HttpEntity(ContentTypes.`application/json`,JobDescription(mcName.namespace,mcName.name,"Pattern","pattern1").toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(submitJobRoute) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 201 Created when submitting a job" in {
    val wsName = WorkspaceName(wsns,wsname)
    val mcName = MethodConfigurationName("three_step","dsde",wsName)
    val methodConf = MethodConfiguration(mcName.name,"Pattern","dsde","three_step","1",Map.empty,Map("pattern"->"String"),Map.empty,mcName.workspaceName,mcName.namespace)
    Post(s"/workspaces/${wsns}/${wsname}/methodconfigs", HttpEntity(ContentTypes.`application/json`,methodConf.toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(createMethodConfigurationRoute) ~>
      check { assertResult(StatusCodes.Created) {status} }
    val entity = Entity("pattern1","Pattern",Map("pattern"->AttributeString("hello")),wsName)
    Post(s"/workspaces/${wsns}/${wsname}/entities", HttpEntity(ContentTypes.`application/json`,entity.toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(createEntityRoute)
      check { assertResult(StatusCodes.Created) {status} }
    Post(s"/workspaces/${wsns}/${wsname}/jobs", HttpEntity(ContentTypes.`application/json`,JobDescription(mcName.namespace,mcName.name,"Pattern","pattern1").toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(submitJobRoute) ~>
      check { assertResult(StatusCodes.Created) {status} }
  }
}
