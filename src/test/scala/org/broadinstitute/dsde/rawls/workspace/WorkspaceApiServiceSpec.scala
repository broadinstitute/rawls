package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.WorkspaceApiService
import org.broadinstitute.dsde.rawls.workspace.EntityUpdateOperations._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}
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
class WorkspaceApiServiceSpec extends FlatSpec with WorkspaceApiService with ScalatestRouteTest with Matchers {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  def actorRefFactory = system
  val dataSource = DataSource("memory:rawls", "admin", "admin")

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

  val workspace = Workspace(
    wsns,
    wsname,
    DateTime.now().withMillis(0),
    "test",
    Map.empty
  )

  val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, MockWorkspaceDAO, MockEntityDAO, MockMethodConfigurationDAO)

  val dao = MockWorkspaceDAO

  "WorkspaceApi" should "return 201 for post to workspaces" in {
    Post(s"/workspaces", HttpEntity(ContentTypes.`application/json`, workspace.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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

  it should "list workspaces" in {
    Get("/workspaces") ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(listWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(MockWorkspaceDAO.store.values.toSeq) {
          responseAs[Array[Workspace]]
        }
      }

  }

  val workspaceCopy = WorkspaceName(namespace = workspace.namespace, name = "test_copy")

  it should "return 404 Not Found on copy if the source workspace cannot be found" in {
    Post(s"/workspaces/${workspace.namespace}/nonexistent/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}x/entities/${s2.entityType}/${s2.name}") ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(getEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}x/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): EntityUpdateOperation).toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
    Delete(s"/workspaces/${workspace.namespace}/${workspace.name}x/entities/${s2.entityType}/${s2.name}") ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(deleteEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 on Entity CRUD when workspace does not exist" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}x/entities", HttpEntity(ContentTypes.`application/json`, s2.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(createEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }

  }

  it should "return 201 on create entity" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities", HttpEntity(ContentTypes.`application/json`, s2.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(createEntityRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 200 on get entity" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}") ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
  it should "return 404 on non-existing entity" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}x") ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(getEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on update entity" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): EntityUpdateOperation).toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveAttribute("bar"): EntityUpdateOperation).toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}x", HttpEntity(ContentTypes.`application/json`, Seq(AddUpdateAttribute("boo", AttributeString("bang")): EntityUpdateOperation).toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 400 on remove from an attribute that is not a list" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveListMember("foo", AttributeString("adsf")): EntityUpdateOperation).toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on remove from list attribute that does not exist" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(RemoveListMember("grip", AttributeString("adsf")): EntityUpdateOperation).toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(updateEntityRoute) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on add to list attribute that is not a list" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}", HttpEntity(ContentTypes.`application/json`, Seq(AddListMember("foo", AttributeString("adsf")): EntityUpdateOperation).toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(renameEntityRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on entity rename" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/entities/${s2.entityType}/${s2.name}/rename", HttpEntity(ContentTypes.`application/json`, EntityName("s2_changed").toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(deleteEntityRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }
  it should "return 201 on create method configuration" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, methodConfig.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(renameMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on method configuration rename" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/${methodConfig2.namespace}/${methodConfig2.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName("testConfig2_changed", methodConfig2.namespace, WorkspaceName(workspace.namespace, workspace.name)).toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(deleteMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on update method configuration" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/update", HttpEntity(ContentTypes.`application/json`, methodConfig3.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(updateMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 on copy method configuration" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, methodConfigName.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
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

  it should "return 404 on copy method configuration" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/update}", HttpEntity(ContentTypes.`application/json`, methodConfig2.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(copyMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on get method configuration" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs/${methodConfig.namespace}/${methodConfig.name}") ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(getMethodConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "list method Configuration" in {
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}/methodconfigs") ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(listMethodConfigurationsRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(MockMethodConfigurationDAO.store(workspace.namespace, workspace.name).values.map(mc =>
          MethodConfigurationShort(mc.name, mc.rootEntityType, mc.methodNamespace, mc.methodName, mc.methodVersion, mc.workspaceName, mc.namespace)).toSeq) {
          responseAs[Array[MethodConfigurationShort]]
        }
      }
  }

  it should "copy a workspace if the source exists" in {
    //MockEntityDAO.save(workspace.namespace, workspace.name, c1, null)
    //MockEntityDAO.save(workspace.namespace, workspace.name, c2, null)
    //MockEntityDAO.save(workspace.namespace, workspace.name, c3, null)
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        val copiedWorkspace = MockWorkspaceDAO.store((workspaceCopy.namespace, workspaceCopy.name))

        println(MockEntityDAO.listEntitiesAllTypes(workspaceCopy.namespace, workspaceCopy.name, null).mkString + "\n")
        println(MockEntityDAO.listEntitiesAllTypes(workspace.namespace, workspace.name, null).mkString)

        //Name, namespace, creation date, and owner might change, so this is all that remains.
        assert(copiedWorkspace.attributes == workspace.attributes)
        assertResult(MockEntityDAO.listEntitiesAllTypes(workspace.namespace, workspace.name, null).toSeq) {
          MockEntityDAO.listEntitiesAllTypes(workspaceCopy.namespace, workspaceCopy.name, null).toSeq
        }
        assertResult(MockMethodConfigurationDAO.list(workspace.namespace, workspace.name, null).toSeq) {
          MockMethodConfigurationDAO.list(workspaceCopy.namespace, workspaceCopy.name, null).toSeq
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
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

}
