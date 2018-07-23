package org.broadinstitute.dsde.rawls.jobexec

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._

import scala.collection.immutable.Map
import org.scalatest.{Matchers, WordSpecLike}
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import java.util.UUID

import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import spray.json.JsArray
import wom.callable.Callable.{InputDefinition, InputDefinitionWithDefault, OptionalInputDefinition, RequiredInputDefinition}
import wom.types.{WomArrayType, WomIntegerType, WomOptionalType}

import scala.concurrent.ExecutionContext

class MethodConfigResolverSpec extends WordSpecLike with Matchers with TestDriverComponent {

  import driver.api._

  val littleWdl =
    """
      |task t1 {
      |  Int int_arg
      |  Int? int_opt
      |  command {
      |    echo ${int_arg}
      |    echo ${int_opt}
      |  }
      |}
      |
      |workflow w1 {
      |  call t1
      |}
    """.stripMargin

  val arrayWdl =
    """
      |task t1 {
      |  Int int_arg
      |  command {
      |    echo ${int_arg}
      |  }
      |}
      |
      |workflow w1 {
      |  Array[Int] int_array
      |  scatter(i in int_array) {
      |    call t1 { input: int_arg = i }
      |  }
      |}
    """.stripMargin

  val doubleArrayWdl =
    """
      |task t1 {
      |  Array[Int] aint_arg
      |  command {
      |    echo ${aint_arg}
      |  }
      |}
      |
      |workflow w1 {
      |  Array[Array[Int]] aint_array
      |  scatter(ai in aint_array) {
      |    call t1 { input: aint_arg = i }
      |  }
      |}
    """.stripMargin

  val optionalDoubleArrayWdl =
    """
      |task t1 {
      |  Array[Int] aint_arg
      |  command {
      |    echo ${aint_arg}
      |  }
      |}
      |
      |workflow w1 {
      |  Array[Array[Int]]? aint_array
      |  scatter(ai in aint_array) {
      |    call t1 { input: aint_arg = i }
      |  }
      |}
    """.stripMargin

  val tripleArrayWdl =
    """
      |task t1 {
      |  Array[Array[Int]] aint_arg
      |  command {
      |    echo ${aint_arg}
      |  }
      |}
      |
      |workflow w1 {
      |  Array[Array[Array[Int]]] aaint_array
      |  scatter(ai in aaint_array) {
      |    call t1 { input: aint_arg = i }
      |  }
      |}
    """.stripMargin

  val badWdl = littleWdl.replace("workflow", "not-a-workflow")

  val intArgName = "w1.t1.int_arg"
  val intOptName = "w1.t1.int_opt"
  val intArrayName = "w1.int_array"
  val doubleIntArrayName = "w1.aint_array"
  val tripleIntArrayName = "w1.aaint_array"

  val workspace = Workspace("workspaces", "test_workspace", UUID.randomUUID().toString(), "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", Map.empty)

  import spray.json._
  val sampleGood = Entity("sampleGood", "Sample",
    Map(AttributeName.withDefaultNS("blah") -> AttributeNumber(1),
        AttributeName.withDefaultNS("rawJsonDoubleArray") -> AttributeValueRawJson( "[[0,1,2],[3,4,5]]".parseJson)))
  val sampleGood2 = Entity("sampleGood2", "Sample",
    Map(AttributeName.withDefaultNS("blah") -> AttributeNumber(2),
        AttributeName.withDefaultNS("rawJsonDoubleArray") -> AttributeValueRawJson( "[[3,4,5],[6,7,8]]".parseJson)))
  val sampleMissingValue = Entity("sampleMissingValue", "Sample", Map.empty)

  val sampleSet = Entity("daSampleSet", "SampleSet",
    Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      sampleGood.toReference,
      sampleMissingValue.toReference)
    ))
  )

  val sampleSet2 = Entity("daSampleSet2", "SampleSet",
    Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      sampleGood.toReference,
      sampleGood2.toReference
    )),
      AttributeName.withDefaultNS("rawJsonDoubleArray") -> AttributeValueRawJson( "[[0,1,2],[3,4,5]]".parseJson )
    )
  )

  val sampleSet3 = Entity("daSampleSet3", "SampleSet",
    Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      sampleGood.toReference))))

  val dummyMethod = AgoraMethod("method_namespace", "test_method", 1)

  val configGood = MethodConfiguration("config_namespace", "configGood", Some("Sample"),
    Map.empty, Map(intArgName -> AttributeString("this.blah")), Map.empty, dummyMethod)

  val configEvenBetter = MethodConfiguration("config_namespace", "configGood", Some("Sample"),
    Map.empty, Map(intArgName -> AttributeString("this.blah"), intOptName -> AttributeString("this.blah")),
    Map.empty, dummyMethod)

  val configMissingExpr = MethodConfiguration("config_namespace", "configMissingExpr", Some("Sample"),
    Map.empty, Map.empty, Map.empty, dummyMethod)

  val configSampleSet = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    Map.empty, Map(intArrayName -> AttributeString("this.samples.blah")), Map.empty, dummyMethod)

  val configEmptyArray = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    Map.empty, Map(intArrayName -> AttributeString("this.nonexistent")), Map.empty, dummyMethod)

  val configRawJsonDoubleArray = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
      Map.empty, Map(doubleIntArrayName -> AttributeString("this.rawJsonDoubleArray")), Map.empty, dummyMethod)

  val configRawJsonTripleArray = MethodConfiguration("config_namespace", "configSample", Some("Sample"),
    Map.empty, Map(tripleIntArrayName -> AttributeString("this.samples.rawJsonDoubleArray")), Map.empty, dummyMethod)

  class ConfigData extends TestData {
    override def save() = {
      DBIO.seq(
        workspaceQuery.save(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(context, sampleGood),
            entityQuery.save(context, sampleGood2),
            entityQuery.save(context, sampleMissingValue),
            entityQuery.save(context, sampleSet),
            entityQuery.save(context, sampleSet2),
            entityQuery.save(context, sampleSet3),
            methodConfigurationQuery.create(context, configGood),
            methodConfigurationQuery.create(context, configMissingExpr),
            methodConfigurationQuery.create(context, configSampleSet)
          )
        }
      )
    }
  }

  val configData = new ConfigData()

  def withConfigData[T](testCode: => T): T = {
    withCustomTestDatabaseInternal(configData)(testCode)
  }

  //Test harness to call resolveInputsForEntities without having to go via the WorkspaceService
  def testResolveInputs(workspaceContext: SlickWorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String, dataAccess: DataAccess)
                       (implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
    dataAccess.entityQuery.findEntityByName(workspaceContext.workspaceId, entity.entityType, entity.name).result flatMap { entityRecs =>
      MethodConfigResolver.gatherInputs(methodConfig, wdl) match {
        case scala.util.Failure(exception) =>
          DBIO.failed(exception)
        case scala.util.Success(gatherInputsResult: GatherInputsResult)
          if gatherInputsResult.extraInputs.nonEmpty || gatherInputsResult.missingInputs.nonEmpty =>
          DBIO.failed(new RawlsException(s"gatherInputsResult has missing or extra inputs: $gatherInputsResult"))
        case scala.util.Success(gatherInputsResult: GatherInputsResult) =>
          MethodConfigResolver.evaluateInputExpressions(workspaceContext, gatherInputsResult.processableInputs, Some(entityRecs), dataAccess)
      }
    }
  }


  "MethodConfigResolver" should {
    "resolve method config inputs" in withConfigData {
      val context = SlickWorkspaceContext(workspace)
      runAndWait(testResolveInputs(context, configGood, sampleGood, littleWdl, this)) shouldBe
        Map(sampleGood.name -> Seq(SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgName)))

      runAndWait(testResolveInputs(context, configEvenBetter, sampleGood, littleWdl, this)) shouldBe
        Map(sampleGood.name -> Seq(SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgName), SubmissionValidationValue(Some(AttributeNumber(1)), None, intOptName)))

      runAndWait(testResolveInputs(context, configSampleSet, sampleSet, arrayWdl, this)) shouldBe
        Map(sampleSet.name -> Seq(SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1)))), None, intArrayName)))

      runAndWait(testResolveInputs(context, configSampleSet, sampleSet2, arrayWdl, this)) shouldBe
        Map(sampleSet2.name -> Seq(SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1), AttributeNumber(2)))), None, intArrayName)))

      // failure cases
      assertResult(true, "Missing values should return an error") {
        runAndWait(testResolveInputs(context, configGood, sampleMissingValue, littleWdl, this)).get("sampleMissingValue").get match {
          case List(SubmissionValidationValue(None, Some(_), intArg)) if intArg == intArgName => true
        }
      }

      //MethodConfiguration config_namespace/configMissingExpr is missing definitions for these inputs: w1.t1.int_arg
      intercept[RawlsException] {
        runAndWait(testResolveInputs(context, configMissingExpr, sampleGood, littleWdl, this))
      }
    }

    "remove missing inputs from processable inputs in GatherInputsResult" in withConfigData {
      val gatheredInputs = MethodConfigResolver.gatherInputs(configMissingExpr, littleWdl)
      gatheredInputs shouldBe 'success
      gatheredInputs.get.processableInputs shouldBe 'empty
      gatheredInputs.get.missingInputs shouldBe Set(intArgName)
      gatheredInputs.get.emptyOptionalInputs.map(_.workflowInput.name) shouldBe Set(intOptName)
    }

    "resolve empty lists into AttributeEmptyLists" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      runAndWait(testResolveInputs(context, configEmptyArray, sampleSet2, arrayWdl, this)) shouldBe
        Map(sampleSet2.name -> Seq(SubmissionValidationValue(Some(AttributeValueEmptyList), None, intArrayName)))
    }

    "unpack AttributeValueRawJson into WDL-arrays" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonDoubleArray, sampleSet2, doubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = MethodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
    }

    "unpack AttributeValueRawJson into optional WDL-arrays" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonDoubleArray, sampleSet2, optionalDoubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = MethodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
    }

    "unpack AttributeValueRawJson into lists-of WDL-arrays" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonTripleArray, sampleSet2, tripleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = MethodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aaint_array":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]}"""
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

    "parse WDL" in withConfigData {
      val littleWorkflow = MethodConfigResolver.parseWDL(littleWdl).get

      val expectedLittleInputs = Map(
        intArgName -> RequiredInputDefinition(intArgName, WomIntegerType),
        intOptName -> OptionalInputDefinition(intOptName, WomOptionalType(WomIntegerType)))

      assertResult(expectedLittleInputs) {
        littleWorkflow.inputs
      }
      assertResult(Seq()) {
        littleWorkflow.outputs
      }

      val arrayWorkflow = MethodConfigResolver.parseWDL(arrayWdl).get

      val expectedArrayInputs = Map(intArrayName -> RequiredInputDefinition(intArrayName, WomArrayType(WomIntegerType)))

      assertResult(expectedArrayInputs) {
        arrayWorkflow.inputs
      }
      assertResult(Seq()) {
        arrayWorkflow.outputs
      }
    }

    "catch WDL syntax errors" in withConfigData {
      val tryParse = MethodConfigResolver.parseWDL(badWdl)

      assert(tryParse.isFailure)
      intercept[RawlsException] {
        tryParse.get
      }
    }

    "get method config inputs and outputs" in withConfigData {
      val expectedLittleIO = MethodInputsOutputs(Seq(
        MethodInput(intArgName, "Int", false),
        MethodInput(intOptName, "Int?", true)), Seq())

      assertResult(expectedLittleIO) {
        MethodConfigResolver.getMethodInputsOutputs(littleWdl).get
      }

      val expectedArrayIO = MethodInputsOutputs(Seq(
        MethodInput(intArrayName, "Array[Int]", false)), Seq())

      assertResult(expectedArrayIO) {
        MethodConfigResolver.getMethodInputsOutputs(arrayWdl).get
      }

      val badIO = MethodConfigResolver.getMethodInputsOutputs(badWdl)
      assert(badIO.isFailure)
      intercept[RawlsException] {
        badIO.get
      }
    }

    "create a Method Config from a template" in withConfigData {
      val expectedLittleInputs = Map(intArgName -> AttributeString(""), intOptName -> AttributeString(""))
      val expectedLittleTemplate = MethodConfiguration("namespace", "name", Some("rootEntityType"), Map(), expectedLittleInputs, Map(), dummyMethod)

      assertResult(expectedLittleTemplate) { MethodConfigResolver.toMethodConfiguration(littleWdl, dummyMethod).get }

      val expectedArrayInputs = Map(intArrayName -> AttributeString(""))
      val expectedArrayTemplate = MethodConfiguration("namespace", "name", Some("rootEntityType"), Map(), expectedArrayInputs, Map(), dummyMethod)

      assertResult(expectedArrayTemplate) { MethodConfigResolver.toMethodConfiguration(arrayWdl, dummyMethod).get }

      val badTemplate = MethodConfigResolver.toMethodConfiguration(badWdl, dummyMethod)
      assert(badTemplate.isFailure)
      intercept[RawlsException] {
        badTemplate.get
      }
    }
  }
}
