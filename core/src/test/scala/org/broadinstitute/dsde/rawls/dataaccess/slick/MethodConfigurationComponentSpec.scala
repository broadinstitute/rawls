package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._

/**
 * Created by mbemis on 2/17/16.
 */
class MethodConfigurationComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils {
  import driver.api._

  "MethodConfigurationComponent" should "save and get a method config" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val methodConfig2 = MethodConfiguration(
      "ns",
      "config2",
      Some("sample"),

      Map("input.expression" -> AttributeString("this..wont.parse")),
      Map("output.expression" -> AttributeString("output.expr")),
      Map("prereq.expression" -> AttributeString("prereq.expr")),
      AgoraMethod("ns-config", "meth2", 2)
    )

    runAndWait(methodConfigurationQuery.create(workspaceContext, methodConfig2))

    assertResult(Option(methodConfig2)) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, methodConfig2.namespace, methodConfig2.name))
    }
  }

  it should "list method configs" in withConstantTestDatabase {
    val workspaceContext = SlickWorkspaceContext(constantData.workspace)
    assertSameElements(constantData.allMCs.map(_.toShort), runAndWait(methodConfigurationQuery.listActive(workspaceContext)))
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

    //Check that the deleted method dumped a timestamp in the deleted timestamp
    assertResult(Some(true)) {
      deletedMethod.map(_.deletedDate.isDefined)
    }
  }

  "MethodConfigurationComponent.upsert" should "in-place update a method config with incremented version" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val oldMethod = runAndWait(uniqueResult[MethodConfigurationRecord](methodConfigurationQuery.findActiveByName(workspaceContext.workspaceId, testData.agoraMethodConfig.namespace, testData.agoraMethodConfig.name))).get

    val changed = testData.agoraMethodConfig.copy(rootEntityType = Some("goober"),
      prerequisites = Map.empty,
      inputs = Map("input.expression.new" -> AttributeString("input.expr")),
      outputs = Map("output.expression.new" -> AttributeString("output.expr")),
      methodRepoMethod = testData.agoraMethod.copy(methodVersion = 2)
    )

    runAndWait(methodConfigurationQuery.upsert(workspaceContext, changed))

    assertResult(Option(changed.copy(methodConfigVersion = 2))) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, changed.namespace, changed.name))
    }

    val oldConfigName = runAndWait(uniqueResult[MethodConfigurationRecord](methodConfigurationQuery.findById(oldMethod.id))).get.name

    assert(oldConfigName.startsWith(oldMethod.name + "_"))

    val listActive = runAndWait(methodConfigurationQuery.listActive(workspaceContext))

    assertResult(0) {
      listActive.filter(_.name.contains(testData.agoraMethodConfig.name + "_")).size
    }
  }

  "MethodConfigurationComponent.update" should "in-place update a method config with incremented version" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val oldMethod = runAndWait(uniqueResult[MethodConfigurationRecord](methodConfigurationQuery.findActiveByName(workspaceContext.workspaceId, testData.agoraMethodConfig.namespace, testData.agoraMethodConfig.name))).get

    val changed = testData.agoraMethodConfig.copy(rootEntityType = Some("goober"),
      prerequisites = Map.empty,
      inputs = Map("input.expression.new" -> AttributeString("input.expr")),
      outputs = Map("output.expression.new" -> AttributeString("output.expr")),
      methodRepoMethod = testData.agoraMethod.copy(methodVersion = 2)
    )

    runAndWait(methodConfigurationQuery.update(workspaceContext, testData.agoraMethodConfig.namespace, testData.agoraMethodConfig.name, changed))

    assertResult(Option(changed.copy(methodConfigVersion = 2))) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, changed.namespace, changed.name))
    }

    val oldConfigName = runAndWait(uniqueResult[MethodConfigurationRecord](methodConfigurationQuery.findById(oldMethod.id))).get.name

    assert(oldConfigName.startsWith(oldMethod.name + "_"))

    val listActive = runAndWait(methodConfigurationQuery.listActive(workspaceContext))

    assertResult(0) {
      listActive.filter(_.name.contains(testData.agoraMethodConfig.name + "_")).size
    }
  }

  it should "update method configs to a new, empty location" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val methodConfigOldName = MethodConfiguration(
      "ns",
      "oldName",
      Some("sample"),
      Map("input.expression" -> AttributeString("this..wont.parse")),
      Map("output.expression" -> AttributeString("output.expr")),
      Map("prereq.expression" -> AttributeString("prereq.expr")),
      AgoraMethod("ns-config", "meth2", 2)
    )

    runAndWait(methodConfigurationQuery.create(workspaceContext, methodConfigOldName))

    val oldMethodId = runAndWait(uniqueResult[MethodConfigurationRecord](methodConfigurationQuery.findActiveByName(workspaceContext.workspaceId, methodConfigOldName.namespace, methodConfigOldName.name))).get.id

    val changed = methodConfigOldName.copy(name = "newName")

    runAndWait(methodConfigurationQuery.update(workspaceContext, methodConfigOldName.namespace, methodConfigOldName.name, changed))

    //there was no config at that location, so the version should be 1
    assertResult(Option(changed.copy(methodConfigVersion = 1))) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, changed.namespace, changed.name))
    }

    val oldConfigName = runAndWait(uniqueResult[MethodConfigurationRecord](methodConfigurationQuery.findById(oldMethodId))).get.name

    assert(oldConfigName.startsWith(methodConfigOldName.name + "_"))
  }

  it should "update method configs to a new location with something there already" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val methodConfigToMove = MethodConfiguration(
      "ns",
      "oldName",
      Some("sample"),
      Map("input.expression" -> AttributeString("this..wont.parse")),
      Map("output.expression" -> AttributeString("output.expr")),
      Map("prereq.expression" -> AttributeString("prereq.expr")),
      AgoraMethod("ns-config", "meth2", 2)
    )

    val methodConfigAlreadyThere = MethodConfiguration(
      "ns",
      "newName",
      Some("sample"),
      Map("input.expression" -> AttributeString("this..wont.parse")),
      Map("output.expression" -> AttributeString("already.there")),
      Map("prereq.expression" -> AttributeString("already.there")),
      AgoraMethod("ns-config", "meth2", 2),
      methodConfigVersion = 10
    )

    runAndWait(methodConfigurationQuery.create(workspaceContext, methodConfigToMove))

    //do it a few times to bump the version
    runAndWait(methodConfigurationQuery.create(workspaceContext, methodConfigAlreadyThere))
    runAndWait(methodConfigurationQuery.create(workspaceContext, methodConfigAlreadyThere))
    runAndWait(methodConfigurationQuery.create(workspaceContext, methodConfigAlreadyThere))

    val oldMethodId = runAndWait(uniqueResult[MethodConfigurationRecord](methodConfigurationQuery.findActiveByName(workspaceContext.workspaceId, methodConfigToMove.namespace, methodConfigToMove.name))).get.id

    val changed = methodConfigToMove.copy(name = "newName")

    runAndWait(methodConfigurationQuery.update(workspaceContext, methodConfigToMove.namespace, methodConfigToMove.name, changed))

    //the version number should be incremented relative to the one that was already there
    assertResult(Option(changed.copy(methodConfigVersion = 4))) {
      runAndWait(methodConfigurationQuery.get(workspaceContext, changed.namespace, changed.name))
    }

    val oldConfigName = runAndWait(uniqueResult[MethodConfigurationRecord](methodConfigurationQuery.findById(oldMethodId))).get.name

    assert(oldConfigName.startsWith(methodConfigToMove.name + "_"))
  }


}
