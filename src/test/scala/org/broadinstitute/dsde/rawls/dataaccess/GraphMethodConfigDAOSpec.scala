package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by dvoet on 5/21/15.
 */
class GraphMethodConfigDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {

  "GraphMethodConfigDAO" should "save and get a method config" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction(writeLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        val methodConfig2 = MethodConfiguration(
          "ns",
          "config2",
          "sample",

          Map("input.expression" -> AttributeString("this..wont.parse")),
          Map("output.expression" -> AttributeString("output.expr")),
          Map("prereq.expression" -> AttributeString("prereq.expr")),
          MethodRepoMethod("ns-config", "meth2", 2)
        )

        new GraphMethodConfigurationDAO().save(context, methodConfig2, txn)

        assertResult(Option(methodConfig2)) {
          new GraphMethodConfigurationDAO().get(context, methodConfig2.namespace, methodConfig2.name, txn)
        }
      }
    }
  }

  it should "overwrite method configs" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction(writeLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        val changed = testData.methodConfig.copy(rootEntityType = "goober")
        new GraphMethodConfigurationDAO().save(context, changed, txn)
        assertResult(Option(changed)) {
          new GraphMethodConfigurationDAO().get(context, changed.namespace, changed.name, txn)
        }
      }
    }
  }

  it should "list method configs" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction(readLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(List(testData.methodConfig, testData.methodConfig2, testData.methodConfigValid, testData.methodConfigUnparseable, testData.methodConfigNotAllSamples, testData.methodConfigAttrTypeMixup).map(_.toShort)) {
          new GraphMethodConfigurationDAO().list(context, txn).toList
        }
      }
    }
  }

  it should "rename method configs" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction(writeLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        val changed = testData.methodConfig.copy(name = "sample", rootEntityType = "Sample")
        new GraphMethodConfigurationDAO().rename(context, testData.methodConfig.namespace, testData.methodConfig.name, changed.name, txn)

        assertResult(Option(changed)) {
          new GraphMethodConfigurationDAO().get(context, changed.namespace, changed.name, txn)
        }
        assertResult(None) {
          new GraphMethodConfigurationDAO().get(context, testData.methodConfig.namespace, testData.methodConfig.name, txn)
        }
      }
    }
  }

  it should "delete method configs" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction(writeLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(Option("testConfig1")) {
          new GraphMethodConfigurationDAO().get(context, testData.methodConfig.namespace, "testConfig1", txn).map(_.name)
        }
        new GraphMethodConfigurationDAO().delete(context, testData.methodConfig.namespace, "testConfig1", txn)
        assertResult(None) {
          new GraphMethodConfigurationDAO().get(context, testData.methodConfig.namespace, "testConfig1", txn)
        }
      }
    }
  }
}
