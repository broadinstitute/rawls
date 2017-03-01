package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._

/**
 * Created by mbemis on 2/17/16.
 */
class MethodConfigurationComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils {
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

  it should "hide old method config and save new method config with incremented version" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val changed = testData.methodConfig.copy(rootEntityType = "goober",
      prerequisites = Map.empty,
      inputs = Map("input.expression.new" -> AttributeString("input.expr")),
      outputs = Map("output.expression.new" -> AttributeString("output.expr")),
      methodRepoMethod = testData.methodConfig.methodRepoMethod.copy(methodVersion = 2)
    )

    runAndWait(methodConfigurationQuery.save(workspaceContext, changed))

    assertResult(Option(changed.copy(methodConfigVersion = 2))) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, changed.namespace, changed.name))
    }

    val list = runAndWait(methodConfigurationQuery.list(workspaceContext))

    assertResult(1) {
      list.filter(_.name.contains(testData.methodConfig.name + "_")).size
    }

    val listActive = runAndWait(methodConfigurationQuery.listActive(workspaceContext))

    assertResult(0) {
      listActive.filter(_.name.contains(testData.methodConfig.name + "_")).size
    }
  }

  it should "list method configs" in withConstantTestDatabase {
    val workspaceContext = SlickWorkspaceContext(constantData.workspace)
    assertSameElements(constantData.allMCs.map(_.toShort), runAndWait(methodConfigurationQuery.list(workspaceContext)))
  }

  it should "rename method configs" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val methodConfigOldName = MethodConfiguration(
      "ns",
      "oldName",
      "sample",
      Map("input.expression" -> AttributeString("this..wont.parse")),
      Map("output.expression" -> AttributeString("output.expr")),
      Map("prereq.expression" -> AttributeString("prereq.expr")),
      MethodRepoMethod("ns-config", "meth2", 2)
    )

    runAndWait(methodConfigurationQuery.save(workspaceContext, methodConfigOldName ))

    val changed = methodConfigOldName.copy(name = "newName")

    runAndWait(methodConfigurationQuery.rename(workspaceContext, methodConfigOldName.namespace, methodConfigOldName.name, changed))

    assertResult(Option(changed.copy(methodConfigVersion = 2))) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, changed.namespace, changed.name))
    }

    val list = runAndWait(methodConfigurationQuery.list(workspaceContext))

    assertResult(1) {
      list.filter(_.name.contains(methodConfigOldName.name + "_")).size
    }
  }

  it should "delete method configs by hiding them" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    //get the to-be-deleted method config record
    val method = runAndWait(methodConfigurationQuery.findActiveByName(workspaceContext.workspaceId,testData.methodConfig3.namespace, testData.methodConfig3.name).result)

    //assert that the result is unique (only one method config was returned)
    assertResult(1) {
      method.length
    }

    //assert that the name is what we think it is
    assertResult(Vector(testData.methodConfig3.name)) {
      method.map(_.name)
    }

    //delete (or hide) the method config
    runAndWait(methodConfigurationQuery.delete(workspaceContext, testData.methodConfig3.namespace, testData.methodConfig3.name))

    //load the deleted/hidden method config
    val deletedMethod = runAndWait(methodConfigurationQuery.loadMethodConfigurationById(method.head.id))

    //Check that the deleted method has an updated name
    assert(deletedMethod.map(_.name).get.contains(testData.methodConfig3.name + "_"))

    //Check that the deleted method has the deleted field set to true
    assertResult(Some(true)) {
      deletedMethod.map(_.deleted)
    }
  }
}
