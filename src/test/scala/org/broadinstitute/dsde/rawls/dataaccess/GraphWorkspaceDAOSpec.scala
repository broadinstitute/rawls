package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

class GraphWorkspaceDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {

  "GraphWorkspaceDAO" should "save a new workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      val workspace2 = Workspace(
        namespace = testData.wsName.namespace,
        name = testData.wsName.name,
        bucketName = "aBucket",
        createdDate = DateTime.now(),
        createdBy = "Barack Obama",
        attributes = Map("workspace_attrib" -> AttributeString("foo"))
      )

      workspaceDAO.save(workspace2, txn)
      // now check explicitly that the vertex exists. note that this will fail if our reserved keywords change.
      assert {
        txn.withGraph { graph =>
          graph.getVertices("_clazz", classOf[Workspace].getSimpleName)
            .filter(v => v.getProperty[String]("_name") == workspace2.name && v.getProperty[String]("_namespace") == workspace2.namespace)
            .headOption.isDefined
        }
      }
    }
  }

  it should "save updates to an existing workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      // TODO since workspaces don't currently have mutable properties, is this redundant with EntityDAO?
      // for now, just save the workspace again (making sure it doesn't crash)
      workspaceDAO.save(testData.workspace, txn)
    }
  }

  it should "load a workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Some(testData.workspace)) {
        workspaceDAO.load(testData.workspace.namespace, testData.workspace.name, txn)
      }
    }
  }

  it should "return None when a workspace does not exist" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(None) {
        new GraphWorkspaceDAO().load(testData.workspace.namespace, "fnord", txn)
      }
      assertResult(None) {
        new GraphWorkspaceDAO().load("fnord", testData.workspace.name, txn)
      }
    }
  }

  it should "load the short version of a workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Some(testData.workspace)) {
        workspaceDAO.load(testData.workspace.namespace, testData.workspace.name, txn)
      }
    }
  }

  it should "show workspace in list" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assert {
        workspaceDAO.list(txn).contains(testData.workspace)
      }
    }
  }
}
