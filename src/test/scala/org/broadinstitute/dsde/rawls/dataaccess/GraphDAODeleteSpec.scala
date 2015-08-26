package org.broadinstitute.dsde.rawls.dataaccess

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

import org.joda.time.DateTime
import org.scalatest.{FreeSpec, Matchers, FlatSpec}

import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._

/**
 * Created with IntelliJ IDEA.
 * User: hussein
 * Date: 08/18/2015
 * Time: 15:05
 */
class GraphDAODeleteSpec extends FreeSpec with Matchers with OrientDbTestFixture {
  val wsName = WorkspaceName("myNamespace", "myWorkspace")
  val workspace = Workspace(wsName.namespace, wsName.name, "aBucket", DateTime.now, "testUser", new HashMap[String, Attribute]() )
  val wsWithAttributeVals = Workspace(wsName.namespace, wsName.name, "aBucket", DateTime.now, "testUser",
    Map(
      "str" -> AttributeString("str"), "num" -> AttributeNumber(4), "bool" -> AttributeBoolean(true),
      "empty" -> AttributeEmptyList, "valueList" -> AttributeValueList(Seq(AttributeNumber(4), AttributeNumber(5))) ))

  "Graph objects shouldn't leave stray vertices behind when they're deleted:" - {
    "Empty workspace" in withEmptyTestDatabase { dataSource =>
      dataSource.inTransaction { txn =>
        txn.withGraph { graph =>
          workspaceDAO.save(workspace, txn)
          workspaceDAO.delete(wsName, txn)
          assertResult(0) {
            graph.getVertices.toList.size
          }
        }
      }
    }

    "Workspace with value attributes and lists" in withEmptyTestDatabase { dataSource =>
      dataSource.inTransaction { txn =>
        txn.withGraph { graph =>
          workspaceDAO.save(wsWithAttributeVals, txn)
          workspaceDAO.delete(wsName, txn)
          assertResult(0) {
            graph.getVertices.toList.size
          }
        }
      }
    }


  }

}
