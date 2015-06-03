package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

class GraphWorkspaceDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {
  override val testDbName = "GraphWorkspaceDAOSpec"
  lazy val dao: GraphWorkspaceDAO = new GraphWorkspaceDAO()
  lazy val entityDao = new GraphEntityDAO()

  // setup workspace objects
  val wsName = WorkspaceName("myNamespace", "myWorkspace")
  val sample1 = Entity("sample1", "sample", Map("foo" -> AttributeString("x")), wsName)
  val individual1 = Entity("individual1", "individual", Map("hasSample" -> AttributeReferenceSingle("sample", "sample1")), wsName)

  val workspace = Workspace(
    namespace = wsName.namespace,
    name = wsName.name,
    createdDate = DateTime.now(),
    createdBy = "Barack Obama",
    attributes = Map("workspace_attrib" -> AttributeString("foo"))
  )

  "GraphWorkspaceDAO" should "save a new workspace" in {
    dao.save(workspace, txn)
    // now check explicitly that the vertex exists. note that this will fail if our reserved keywords change.
    assert {
      txn.withGraph { graph =>
        graph.getVertices("_clazz", classOf[Workspace].getSimpleName)
          .filter(v => v.getProperty[String]("_name") == workspace.name && v.getProperty[String]("_namespace") == workspace.namespace)
          .headOption.isDefined
      }
    }
  }

  it should "save updates to an existing workspace" in {
    // TODO since workspaces don't currently have mutable properties, is this redundant with EntityDAO?
    // for now, just save the workspace again (making sure it doesn't crash)
    dao.save(workspace, txn)
  }

  it should "load a workspace" in {
    assertResult(Some(workspace)) { dao.load(workspace.namespace, workspace.name, txn) }
  }

  it should "return None when a workspace does not exist" in {
    assertResult(None) { new GraphWorkspaceDAO().load(workspace.namespace, "fnord", txn) }
    assertResult(None) { new GraphWorkspaceDAO().load("fnord", workspace.name, txn) }
  }

  it should "load the short version of a workspace" in {
    assertResult(Some(workspace)) { dao.load(workspace.namespace, workspace.name, txn) }
  }

  it should "show workspace in list" in {
    assert { dao.list(txn).contains(workspace) }
  }
}
