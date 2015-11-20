package org.broadinstitute.dsde.rawls.dataaccess

import scala.collection.JavaConversions._

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
  val workspace = Workspace(wsName.namespace, wsName.name, "aWorkspaceId", "aBucket", DateTime.now, DateTime.now, "testUser", Map.empty, Map.empty)
  val wsWithAttributeVals = Workspace(wsName.namespace, wsName.name, "aWorkspaceId", "aBucket", DateTime.now, DateTime.now, "testUser",
    Map(
      "str" -> AttributeString("str"), "num" -> AttributeNumber(4), "bool" -> AttributeBoolean(true),
      "empty" -> AttributeEmptyList, "valueList" -> AttributeValueList(Seq(AttributeNumber(4), AttributeNumber(5))) ), Map.empty)

  "Graph objects shouldn't leave stray vertices behind when they're deleted:" - {
    "Empty workspace" in withEmptyTestDatabase { dataSource =>
      dataSource.inTransaction(writeLocks=Set(workspace.toWorkspaceName)) { txn =>
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
      dataSource.inTransaction(writeLocks=Set(workspace.toWorkspaceName)) { txn =>
        txn.withGraph { graph =>
          workspaceDAO.save(wsWithAttributeVals, txn)
          workspaceDAO.delete(wsName, txn)
          assertResult(0) {
            graph.getVertices.toList.size
          }
        }
      }
    }

    "Workspace with some entities" in withEmptyTestDatabase { dataSource =>
      dataSource.inTransaction(writeLocks=Set(workspace.toWorkspaceName)) { txn =>
        txn.withGraph { graph =>
          workspaceDAO.save(workspace, txn)
          withWorkspaceContext(workspace, txn) { context =>
            entityDAO.save(context, testData.aliquot1, txn)
            entityDAO.save(context, testData.aliquot2, txn)
            entityDAO.save(context, testData.sample1, txn)
          }
          workspaceDAO.delete(wsName, txn)
          assertResult(0) {
            graph.getVertices.toList.size
          }
        }
      }
    }

    "Deleting entities" in withEmptyTestDatabase { dataSource =>
      dataSource.inTransaction(writeLocks=Set(workspace.toWorkspaceName)) { txn =>
        txn.withGraph { graph =>
          workspaceDAO.save(workspace, txn)
          val wsVerts = graph.getVertices.toList.size
          withWorkspaceContext(workspace, txn) { context =>
            entityDAO.save(context, testData.aliquot1, txn)
            entityDAO.save(context, testData.aliquot2, txn)

            entityDAO.delete(context, testData.aliquot1.entityType, testData.aliquot1.name, txn)
            entityDAO.delete(context, testData.aliquot2.entityType, testData.aliquot2.name, txn)
          }
          assertResult(wsVerts) {
            graph.getVertices.toList.size
          }
        }
      }
    }

    "Deleting submissions" in withEmptyTestDatabase { dataSource =>
      dataSource.inTransaction(writeLocks=Set(workspace.toWorkspaceName)) { txn =>
        txn.withGraph { graph =>
          workspaceDAO.save(workspace, txn)
          withWorkspaceContext(workspace, txn) { context =>
            entityDAO.save(context, testData.aliquot1, txn)
            entityDAO.save(context, testData.sample1, txn)
            entityDAO.save(context, testData.sample2, txn)
            entityDAO.save(context, testData.sample3, txn)
            entityDAO.save(context, testData.sset1, txn)
            entityDAO.save(context, testData.indiv1, txn)
            authDAO.saveUser(testData.userOwner, txn)

            val wsVerts = graph.getVertices.toList.size

            submissionDAO.save(context, testData.submission1, txn)
            submissionDAO.delete(context, testData.submission1.submissionId, txn)

            assertResult(wsVerts) {
              graph.getVertices.toList.size
            }
          }
        }
      }
    }

    "Deleting method configs" in withEmptyTestDatabase { dataSource =>
      dataSource.inTransaction(writeLocks=Set(workspace.toWorkspaceName)) { txn =>
        txn.withGraph { graph =>
          workspaceDAO.save(workspace, txn)
          withWorkspaceContext(workspace, txn) { context =>
            val wsVerts = graph.getVertices.toList.size

            methodConfigDAO.save(context, testData.methodConfig, txn)
            methodConfigDAO.delete(context, testData.methodConfig.namespace, testData.methodConfig.name, txn)

            assertResult(wsVerts) {
              graph.getVertices.toList.size
            }
          }
        }
      }
    }
  }

}