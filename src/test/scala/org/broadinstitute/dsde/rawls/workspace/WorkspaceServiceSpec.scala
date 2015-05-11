package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.WorkspaceApiService
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

  val s1 = Entity("s1", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> AttributeList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))), WorkspaceName(wsns, wsname))
  val workspace = Workspace(
    wsns,
    wsname,
    DateTime.now().withMillis(0),
    "test",
    Map(
      "samples" -> Map("s1" -> s1),
      "individuals" -> Map("i" -> Entity("i", Map("samples" -> AttributeList(Seq(AttributeReference("samples", "s2"), AttributeReference("samples", "s1")))), WorkspaceName(wsns, wsname)))
    )
  )

  val workspaceServiceConstructor = WorkspaceService.constructor(MockWorkspaceDAO)

  val dao = MockWorkspaceDAO

  "rawls" should "return 201 for put to workspaces" in {
    Post(s"/workspaces", HttpEntity(ContentTypes.`application/json`, workspace.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(putWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) { status }
        assertResult(workspace) { MockWorkspaceDAO.store((workspace.namespace, workspace.name)) }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${workspace.namespace}/${workspace.name}"))))) { header("Location") }
      }
  }

  it should "list workspaces" in {
    Get("/workspaces") ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(listWorkspacesRoute) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(MockWorkspaceDAO.store.values.map(w => WorkspaceShort(w.namespace, w.name, w.createdDate, w.createdBy)).toSeq) {
          responseAs[Array[WorkspaceShort]]
        }
      }

  }

  val workspaceCopy = WorkspaceName(namespace=workspace.namespace, name="test_copy")

  it should "return 404 Not Found on copy if the source workspace cannot be found" in {
    Post(s"/workspaces/${workspace.namespace}/nonexistent/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString()) ) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "copy a workspace if the source exists" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString()) ) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) { status }
        val copiedWorkspace = MockWorkspaceDAO.store((workspaceCopy.namespace, workspaceCopy.name))

        //Name, namespace, creation date, and owner might change, so this is all that remains.
        assert( copiedWorkspace.entities == workspace.entities )

        assertResult(StatusCodes.Created) { status }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(s"/workspaces/${workspaceCopy.namespace}/${workspaceCopy.name}"))))) { header("Location") }
      }
  }

  it should "return 409 Conflict on copy if the destination already exists" in {
    Post(s"/workspaces/${workspace.namespace}/${workspace.name}/clone", HttpEntity(ContentTypes.`application/json`, workspaceCopy.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(copyWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) { status }
      }
  }

}

object MockWorkspaceDAO extends WorkspaceDAO {
  var store = new mutable.HashMap[Tuple2[String, String], Workspace]()
  def save(workspace: Workspace): Unit = {
    store.put((workspace.namespace, workspace.name), workspace)
  }
  def load(namespace: String, name: String): Option[Workspace] = {
    try {
      Option( store((namespace, name)) )
    } catch {
      case t: NoSuchElementException => None
    }
  }

  override def list(): Seq[WorkspaceShort] = store.values.map(w => WorkspaceShort(w.namespace, w.name, w.createdDate, w.createdBy)).toSeq
}