package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.ws.WorkspaceApiService
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

  val s1 = Entity("s1", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> AttributeList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))))
  val workspace = Workspace(
    "namespace",
    UUID.randomUUID().toString,
    DateTime.now().withMillis(0),
    "test",
    Map(
      "samples" -> Map("s1" -> s1),
      "individuals" -> Map("i" -> Entity("i", Map("samples" -> AttributeList(Seq(AttributeReference("samples", "s2"), AttributeReference("samples", "s1"))))))
    )
  )

  val workspaceServiceConstructor = WorkspaceService.constructor(MockWorkspaceDAO)

  val dao = MockWorkspaceDAO

  "rawls" should "return 201 for put to workspaces" in {
    Put(s"/workspaces/${workspace.namespace}/${workspace.name}", HttpEntity(ContentTypes.`application/json`, workspace.toJson.toString())) ~>
      addHeader(HttpHeaders.`Cookie`(HttpCookie("iPlanetDirectoryPro", "test_token"))) ~>
      sealRoute(putWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) { status }
        assertResult(workspace) { MockWorkspaceDAO.store((workspace.namespace, workspace.name)) }
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
}

object MockWorkspaceDAO extends WorkspaceDAO {
  var store = new mutable.HashMap[Tuple2[String, String], Workspace]()
  def save(workspace: Workspace): Unit = {
    store.put((workspace.namespace, workspace.name), workspace)
  }
  def load(namespace: String, name: String): Workspace = store((namespace, name))

  override def list(): Seq[WorkspaceShort] = store.values.map(w => WorkspaceShort(w.namespace, w.name, w.createdDate, w.createdBy)).toSeq
}