package org.broadinstitute.dsde.rawls.jobexec

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.MockCromwellSwaggerClient._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.jobexec.wdlparsing.WDLParser
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable.Map
import scala.jdk.CollectionConverters._

class MethodConfigResolverSpec
    extends AnyWordSpecLike
    with Matchers
    with TestDriverComponent
    with MethodConfigTestSupport {
  "MethodConfigResolver" should {
    "remove missing inputs from processable inputs in GatherInputsResult" in withConfigData {
      val gatheredInputs = methodConfigResolver.gatherInputs(userInfo, configMissingExpr, littleWdl)
      gatheredInputs shouldBe 'success
      gatheredInputs.get.processableInputs shouldBe 'empty
      gatheredInputs.get.missingInputs shouldBe Set(intArgNameWithWfName)
      gatheredInputs.get.emptyOptionalInputs.map(_.workflowInput.getName) shouldBe Set("w1.t1.int_opt")
    }

    /* IGNORED - Failure case.
       This is the failure case described in MethodConfigResolver.getArrayResult.
   "unpack AttributeValueRawJson into single-element lists-of WDL-arrays" in withConfigData {
     val context = SlickWorkspaceContext(workspace)

     val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonTripleArray, sampleSet3, tripleArrayWdl, this))
     val methodProps = resolvedInputs(sampleSet3.name).map { svv: SubmissionValidationValue =>
       svv.inputName -> svv.value.get
     }
     val wdlInputs: String = MethodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

     wdlInputs shouldBe """{"w1.aaint_array":[[[0,1,2],[3,4,5]]]}"""
     //actually returns: {"w1.aaint_array":[[0,1,2],[3,4,5]]}
     //(note the scalatest output adds an extra set of square brackets to everything for no reason i can discern)
   }
     */

    "parse draft2 WDL" in withConfigData {
      val littleWorkflow = methodConfigResolver.parseWDL(userInfo, littleWdl).get

      littleWorkflow.getName shouldBe littleWdlName
      littleWorkflow.getValid shouldBe true
      littleWorkflow.getInputs.size shouldBe 2
      littleWorkflow.getInputs shouldBe List(
        makeToolInputParameter(intArgNameWithWfName, false, makeValueType("Int"), "Int"),
        makeToolInputParameter(intOptNameWithWfName, true, makeValueType("Int"), "Int?")
      ).asJava

      val arrayWorkflow = methodConfigResolver.parseWDL(userInfo, arrayWdl).get

      arrayWorkflow.getName shouldBe littleWdlName
      arrayWorkflow.getValid shouldBe true
      arrayWorkflow.getInputs.size shouldBe 1
      arrayWorkflow.getInputs shouldBe List(
        makeToolInputParameter(intArrayNameWithWfName, false, makeArrayValueType(makeValueType("Int")), "Array[Int]")
      ).asJava

    }

    "parse WDL 1.0 wdl" in withConfigData {
      val wdlVersionOne = methodConfigResolver.parseWDL(userInfo, wdlVersionOneWdl).get

      assertResult(WDLParser.appendWorkflowNameToInputsAndOutputs(wdlVersionOneWdlWorkflowDescription)) {
        wdlVersionOne
      }
    }

    "parse WDL with syntax errors" in withConfigData {

      val badWdlParse = methodConfigResolver.parseWDL(userInfo, badWdl)

      assert(badWdlParse.isSuccess)
      assertResult(badWdlWorkflowDescription) {
        badWdlParse.get
      }

    }

    "get method config inputs and outputs" in withConfigData {
      val expectedLittleIO = MethodInputsOutputs(Seq(MethodInput(intArgNameWithWfName, "Int", false),
                                                     MethodInput(intOptNameWithWfName, "Int?", true)
                                                 ),
                                                 Seq()
      )

      assertResult(expectedLittleIO) {
        methodConfigResolver.getMethodInputsOutputs(userInfo, littleWdl).get
      }

      val expectedArrayIO = MethodInputsOutputs(Seq(MethodInput(intArrayNameWithWfName, "Array[Int]", false)), Seq())

      assertResult(expectedArrayIO) {
        methodConfigResolver.getMethodInputsOutputs(userInfo, arrayWdl).get
      }

      val badIO = methodConfigResolver.getMethodInputsOutputs(userInfo, badWdl)
      assert(badIO.isFailure)
      intercept[RawlsException] {
        badIO.get
      }
    }

    "create a Method Config from a template" in withConfigData {
      val expectedLittleInputs =
        Map(intArgNameWithWfName -> AttributeString(""), intOptNameWithWfName -> AttributeString(""))
      val expectedLittleTemplate = MethodConfiguration("namespace",
                                                       "name",
                                                       Some("rootEntityType"),
                                                       Some(Map()),
                                                       expectedLittleInputs,
                                                       Map(),
                                                       dummyMethod
      )

      assertResult(expectedLittleTemplate) {
        methodConfigResolver.toMethodConfiguration(userInfo, littleWdl, dummyMethod).get
      }

      val expectedArrayInputs = Map(intArrayNameWithWfName -> AttributeString(""))
      val expectedArrayTemplate = MethodConfiguration("namespace",
                                                      "name",
                                                      Some("rootEntityType"),
                                                      Some(Map()),
                                                      expectedArrayInputs,
                                                      Map(),
                                                      dummyMethod
      )

      assertResult(expectedArrayTemplate) {
        methodConfigResolver.toMethodConfiguration(userInfo, arrayWdl, dummyMethod).get
      }

      val badTemplate = methodConfigResolver.toMethodConfiguration(userInfo, badWdl, dummyMethod)
      assert(badTemplate.isFailure)
      intercept[RawlsException] {
        badTemplate.get
      }
    }
  }
}
