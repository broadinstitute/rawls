package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import _root_.slick.dbio.DBIOAction
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import java.lang.String

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
      MethodRepoMethod("ns-config", "meth2", 2),
      false
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
      outputs = Map("output.expression.new" -> AttributeString("output.expr")),
      methodRepoMethod = testData.methodConfig.methodRepoMethod.copy(methodVersion = 2)
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


  it should "deleting method configs should hide them" in withDefaultTestDatabase {

    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    //get the to-be-deleted method config record
    val method = runAndWait(methodConfigurationQuery.findByName(workspaceContext.workspaceId,testData.methodConfig3.namespace, testData.methodConfig3.name).result)

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
    assertResult(testData.methodConfig3.name + "-deleted-") {
      deletedMethod.map(_.name).get.substring(0, 18)
    }

    //Check that the deleted method has the deleted field set to true
    assertResult(1) {
      deletedMethod.map(_.deleted)
    }
  }
}