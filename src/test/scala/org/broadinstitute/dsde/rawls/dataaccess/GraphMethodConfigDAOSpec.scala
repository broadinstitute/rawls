package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by dvoet on 5/21/15.
 */
class GraphMethodConfigDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {

  val testDbName = "GraphMethodConfigDAOSpec"
  val wsName = WorkspaceName("myNamespace", "myWorkspace")
  val workspace = Workspace(
    namespace = wsName.namespace,
    name = wsName.name,
    createdDate = DateTime.now(),
    createdBy = "Joe Biden",
    Map()
  )


  val methodConfig = MethodConfiguration(
    "config1",
    "sample",
    "ns",
    "meth1",
    "1",
    Map("i1" -> "input expr"),
    Map("o1" -> "output expr"),
    Map("p1" -> "prereq expr"),
    wsName,
    "ns"
  )

  "GraphMethodConfigDAO" should "save and get a method config" in {
    new GraphWorkspaceDAO().save(workspace, txn)

    new GraphMethodConfigurationDAO().save(wsName.namespace, wsName.name, methodConfig, txn)

    assertResult(Option(methodConfig)) {
      new GraphMethodConfigurationDAO().get(wsName.namespace, wsName.name, methodConfig.namespace, methodConfig.name, txn)
    }
  }

  it should "overwrite method configs" in {
    val changed = methodConfig.copy(rootEntityType = "goober")
    new GraphMethodConfigurationDAO().save(wsName.namespace, wsName.name, changed, txn)

    assertResult(Option(changed)) {
      new GraphMethodConfigurationDAO().get(wsName.namespace, wsName.name, changed.namespace, changed.name, txn)
    }
  }

  it should "list method configs" in {
    assertResult(List(MethodConfigurationShort("config1", "goober", "ns", "meth1", "1", wsName, "ns"))) {
      new GraphMethodConfigurationDAO().list(wsName.namespace, wsName.name, txn).toList
    }
  }

  it should "rename method configs" in {
    val changed = methodConfig.copy(name = "goober", rootEntityType = "goober")
    new GraphMethodConfigurationDAO().rename(wsName.namespace, wsName.name, methodConfig.namespace, methodConfig.name, changed.name, txn)

    assertResult(Option(changed)) {
      new GraphMethodConfigurationDAO().get(wsName.namespace, wsName.name, changed.namespace, changed.name, txn)
    }
    assertResult(None) {
      new GraphMethodConfigurationDAO().get(wsName.namespace, wsName.name, methodConfig.namespace, methodConfig.name, txn)
    }
  }

  it should "delete method configs" in {
    assertResult(Option("goober")) {
      new GraphMethodConfigurationDAO().get(wsName.namespace, wsName.name, methodConfig.namespace, "goober", txn).map(_.name)
    }
    new GraphMethodConfigurationDAO().delete(wsName.namespace, wsName.name, methodConfig.namespace, "goober", txn)
    assertResult(None) {
      new GraphMethodConfigurationDAO().get(wsName.namespace, wsName.name, methodConfig.namespace, "goober", txn)
    }
  }
}
