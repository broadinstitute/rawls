package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import java.util.UUID

import akka.testkit.TestActorRef
import org.broadinstitute.dsde.rawls.dataaccess.{MockWorkspaceDAO, MockEntityDAO, EntityDAO, WorkspaceDAO}
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

import scala.collection.mutable

/**
 * Created by dvoet on 4/24/15.
 */
class WorkspaceApiServiceSpec extends FlatSpec with WorkspaceApiService with ScalatestRouteTest with Matchers {
  def actorRefFactory = system

  val wsns = "namespace"
  val wsname = UUID.randomUUID().toString

  val attributeList = AttributeList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val s1 = Entity("s1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList), WorkspaceName(wsns, wsname))
  val s2 = Entity("s2", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList), WorkspaceName(wsns, wsname))
  val workspace = Workspace(
    wsns,
    wsname,
    DateTime.now().withMillis(0),
    "test",
    Map(
      "samples" -> Map("s1" -> s1),
      "individuals" -> Map("i" -> Entity("i", "individuals", Map("samples" -> AttributeList(Seq(AttributeReference("samples", "s2"), AttributeReference("samples", "s1")))), WorkspaceName(wsns, wsname)))
    )
  )

  val workspaceServiceConstructor = WorkspaceService.constructor(MockWorkspaceDAO, MockEntityDAO)

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
        assertResult(MockWorkspaceDAO.store.values.map(w => WorkspaceShort(w.namespace, w.name, w.createdDate, w.createdBy)).toSeq) {
          responseAs[Array[WorkspaceShort]]
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

  it should "copy a workspace if the source exists" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        val copiedWorkspace = MockWorkspaceDAO.store((workspaceCopy.namespace, workspaceCopy.name))

        //Name, namespace, creation date, and owner might change, so this is all that remains.
        assert(copiedWorkspace.entities == workspace.entities)

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
    MockEntityDAO.save(workspace.namespace, workspace.name, s1)
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

  //  delete
//  delete 404
}
