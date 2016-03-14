package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import _root_.slick.dbio.DBIOAction
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime

/**
 * Created by mbemis on 2/17/16.
 */
class MethodConfigurationComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {
  import driver.api._

  "MethodConfigurationComponenent" should "save and get a method config" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val methodConfig2 = MethodConfiguration(
      "ns",
      "config2",
      "sample",

      Map("input.expression" -> AttributeString("this..wont.parse")),
      Map("output.expression" -> AttributeString("output.expr")),
      Map("prereq.expression" -> AttributeString("prereq.expr")),
      MethodRepoMethod("ns-config", "meth2", 2)
    )

    runAndWait(methodConfigurationQuery.save(workspaceContext, methodConfig2))

    assertResult(Option(methodConfig2)) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, methodConfig2.namespace, methodConfig2.name))
    }
  }

  it should "overwrite method configs" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val changed = testData.methodConfig.copy(rootEntityType = "goober",
      prerequisites = Map.empty,
      inputs = Map("input.expression.new" -> AttributeString("input.expr")),
      outputs = Map("output.expression.new" -> AttributeString("output.expr"))
    )

    runAndWait(methodConfigurationQuery.save(workspaceContext, changed))

    assertResult(Option(changed)) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, changed.namespace, changed.name))
    }
  }

  it should "list method configs" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    List(testData.methodConfig, testData.methodConfig2, testData.methodConfigValid, testData.methodConfigUnparseable, testData.methodConfigNotAllSamples, testData.methodConfigAttrTypeMixup).map(_.toShort) should contain
    theSameElementsAs(runAndWait(methodConfigurationQuery.list(workspaceContext)).toList)
  }

  it should "rename method configs" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val changed = testData.methodConfig.copy(name = "sample", rootEntityType = "Sample")

    runAndWait(methodConfigurationQuery.rename(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, changed.name))

    assertResult(Option(changed)) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, changed.namespace, changed.name))
    }

    assertResult(None) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name))
    }
  }

  it should "delete method configs" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    assertResult(Option("testConfig1")) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, testData.methodConfig.namespace, "testConfig1")).map(_.name)
    }

    runAndWait(methodConfigurationQuery.delete(workspaceContext, testData.methodConfig.namespace, "testConfig1"))

    assertResult(None) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, testData.methodConfig.namespace, "testConfig1"))
    }
  }
}
