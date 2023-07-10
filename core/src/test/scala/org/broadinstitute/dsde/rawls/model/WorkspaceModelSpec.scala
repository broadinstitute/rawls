package org.broadinstitute.dsde.rawls.model

import bio.terra.workspace.model.{WsmPolicyInput, WsmPolicyPair}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util

class WorkspaceModelSpec extends AnyFlatSpecLike with MockitoSugar with Matchers {

  behavior of "Converting WsmPolicyInput to rawls' WorkspacePolicy"

  it should "use the WsmPolicyInput name and namespace" in {
    val wsmPolicyInput = new WsmPolicyInput()
    wsmPolicyInput.name("test_name")
    wsmPolicyInput.namespace("test_namespace")
    wsmPolicyInput.additionalData(new util.ArrayList[WsmPolicyPair]())

    val policy = WorkspacePolicy(wsmPolicyInput)
    policy.name shouldEqual wsmPolicyInput.getName
    policy.namespace shouldEqual wsmPolicyInput.getNamespace
  }

  it should "use an empty map when additionalData is null" in {
    val wsmPolicyInput = new WsmPolicyInput()
    wsmPolicyInput.name("test_name")
    wsmPolicyInput.namespace("test_namespace")

    val policy = WorkspacePolicy(wsmPolicyInput)

    policy.additionalData should not be null
    policy.additionalData shouldBe empty
  }

  it should "convert the additionalData to a map" in {
    val wsmPolicyInput = new WsmPolicyInput()
    wsmPolicyInput.name("test_name")
    wsmPolicyInput.namespace("test_namespace")
    val wsmPolicyPairs = new util.ArrayList[WsmPolicyPair]()
    val pair1 = new WsmPolicyPair()
    pair1.value("pair1Val")
    pair1.key("pair1Key")
    wsmPolicyPairs.add(pair1)
    val pair2 = new WsmPolicyPair()
    pair2.value("pair2Val")
    pair2.key("pair2Key")
    wsmPolicyPairs.add(pair2)
    wsmPolicyInput.additionalData(wsmPolicyPairs)

    val policy = WorkspacePolicy(wsmPolicyInput)

    policy.additionalData.getOrElse(pair1.getKey, "fail") shouldEqual pair1.getValue
    policy.additionalData.getOrElse(pair2.getKey, "fail") shouldEqual pair2.getValue

  }

}
