package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.broadinstitute.dsde.rawls.RawlsException
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
        workspaceId = "aWorkspaceId",
        bucketName = "aBucket",
        createdDate = DateTime.now(),
        createdBy = "Barack Obama",
        attributes = Map("workspace_attrib" -> AttributeString("foo"))
      )

      workspaceDAO.save(workspace2, txn)
      // now check explicitly that the vertex exists. note that this will fail if our reserved keywords change.
      assert {
        txn.withGraph { graph =>
          graph.getVertices.exists(v => {
            v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.Workspace) &&
            v.getProperty[String]("name") == workspace2.name && v.getProperty[String]("namespace") == workspace2.namespace
          })
        }
      }
    }
  }

  it should "save updates to an existing workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      // for now, just save the workspace again (making sure it doesn't crash)
      workspaceDAO.save(testData.workspace, txn)
    }
  }

  it should "load a workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Some(testData.workspace)) {
        workspaceDAO.loadContext(testData.workspace.toWorkspaceName, txn) map( _.workspace )
      }
    }
  }

  it should "return None when a workspace does not exist" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(None) {
        new GraphWorkspaceDAO().loadContext(WorkspaceName(testData.workspace.namespace, "fnord"), txn)
      }
      assertResult(None) {
        new GraphWorkspaceDAO().loadContext(WorkspaceName("fnord", testData.workspace.name), txn)
      }
    }
  }

  it should "load the short version of a workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Some(testData.workspace)) {
        workspaceDAO.loadContext(testData.workspace.toWorkspaceName, txn) map( _.workspace )
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

  it should "fail when attempting to put dots in attribute keys" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      val dottyWorkspace = Workspace(
        namespace = testData.wsName.namespace,
        name = "badness",
        workspaceId = "badWorkspaceId",
        bucketName = "badBucket",
        createdDate = DateTime.now(),
        createdBy = "Mitt Romney",
        attributes = Map("dots.dots.more.dots" -> AttributeString("foo"))
      )

      intercept[RawlsException] {
        workspaceDAO.save(dottyWorkspace, txn)
      }
    }
  }

  Attributable.reservedAttributeNames.foreach { reserved =>
    it should "fail using reserved attribute name " + reserved in withDefaultTestDatabase { dataSource =>
      val e = Workspace(
        namespace = testData.wsName.namespace,
        name = "badness",
        workspaceId = "badWorkspaceId",
        bucketName = "badBucket",
        createdDate = DateTime.now(),
        createdBy = "Mitt Romney",
        attributes = Map(reserved -> AttributeString("foo"))
      )
      dataSource inTransaction { txn =>
        withWorkspaceContext(testData.workspace, writeLock = true, txn) { context =>
          intercept[RawlsException] { workspaceDAO.save(e, txn) }
        }
      }
    }
  }
}
