package org.broadinstitute.dsde.rawls.jobexec

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._

import scala.collection.immutable.Map
import org.scalatest.{Matchers, WordSpecLike}
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import java.util.UUID

import cromwell.client.model.{ToolInputParameter, WorkflowDescription}
import cromwell.client.model.ValueType.TypeNameEnum
import cromwell.client.model.ValueType
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult

import scala.concurrent.ExecutionContext

class MethodConfigResolverSpec extends WordSpecLike with Matchers with TestDriverComponent {

  import driver.api._

  val littleWdlName = "littlewdl"
  val littleWdlIntRequiredInput = s"w1.t1.int_arg"
  val littleWdlIntOptionalInput = s"w1.t1.int_opt"

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

  val arrayWDLName = "arraywdl"
  val arrayWDLIntArrayRequiredInput = "w1.int_array"

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

  val doubleArrayWDLName = "doubleArraywdl"
  val doubleArrayWDLIntArrayRequiredInput = "w1.aint_array"


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
      |    call t1 { input: aint_arg = ai }
      |  }
      |}
    """.stripMargin

  val optionalDoubleArrayWDLName = "optionalDoubleArraywdl"
  val optionalDoubleArrayWDLInput = "w1.aint_array"

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
      |    call t1 { input: aint_arg = ai }
      |  }
      |}
    """.stripMargin

  val tripleArrayWDLName = "tripleArraywdl"
  val tripleArrayWDLRequiredInput = "w1.aaint_array"

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
      |    call t1 { input: aint_arg = ai }
      |  }
      |}
    """.stripMargin

  val badWdl = littleWdl.replace("workflow", "not-a-workflow")

  val badWdlWorkflowDescription = new WorkflowDescription()
  badWdlWorkflowDescription.setValid(false)
  badWdlWorkflowDescription.setValidWorkflow(false)
  badWdlWorkflowDescription.addErrorsItem("ERROR: Finished parsing without consuming all tokens.\\n\\nnot-a-workflow w1 {\\n^\\n    ")
  mockCromwellSwaggerClient.workflowDescriptions += (badWdl -> badWdlWorkflowDescription)


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
    None, Map(intArgName -> AttributeString("this.blah")), Map.empty, dummyMethod)

  val configEvenBetter = MethodConfiguration("config_namespace", "configGood", Some("Sample"),
    None, Map(intArgName -> AttributeString("this.blah"), intOptName -> AttributeString("this.blah")),
    Map.empty, dummyMethod)

  val configMissingExpr = MethodConfiguration("config_namespace", "configMissingExpr", Some("Sample"),
    None, Map.empty, Map.empty, dummyMethod)

  val configSampleSet = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    None, Map(intArrayName -> AttributeString("this.samples.blah")), Map.empty, dummyMethod)

  val configEmptyArray = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    None, Map(intArrayName -> AttributeString("this.nonexistent")), Map.empty, dummyMethod)

  val configRawJsonDoubleArray = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    None, Map(doubleIntArrayName -> AttributeString("this.rawJsonDoubleArray")), Map.empty, dummyMethod)

  val configRawJsonTripleArray = MethodConfiguration("config_namespace", "configSample", Some("Sample"),
    None, Map(tripleIntArrayName -> AttributeString("this.samples.rawJsonDoubleArray")), Map.empty, dummyMethod)

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


  val littleWdlWorkflowDescriptionRequiredInput = new ToolInputParameter()
  littleWdlWorkflowDescriptionRequiredInput.setName(littleWdlIntRequiredInput)
  littleWdlWorkflowDescriptionRequiredInput.setOptional(false)
  littleWdlWorkflowDescriptionRequiredInput.setValueType(new ValueType().typeName(TypeNameEnum.INT))

  val littleWdlWorkflowDescriptionOptionalInput = new ToolInputParameter()
  littleWdlWorkflowDescriptionOptionalInput.setName(littleWdlIntOptionalInput)
  littleWdlWorkflowDescriptionOptionalInput.setOptional(true)
  littleWdlWorkflowDescriptionOptionalInput.setValueType(new ValueType().typeName(TypeNameEnum.INT))

  val littleWdlWorkflowDescription = new WorkflowDescription()
  littleWdlWorkflowDescription.addInputsItem(littleWdlWorkflowDescriptionRequiredInput)
  littleWdlWorkflowDescription.addInputsItem(littleWdlWorkflowDescriptionOptionalInput)
  littleWdlWorkflowDescription.setName(littleWdlName)
  littleWdlWorkflowDescription.setValid(true)


  val wdlWorkflowDescriptionIntArrayRequiredInput = new ToolInputParameter()
  wdlWorkflowDescriptionIntArrayRequiredInput.setName(arrayWDLIntArrayRequiredInput)
  wdlWorkflowDescriptionIntArrayRequiredInput.setOptional(false)
  val intValueType = new ValueType()
  intValueType.typeName(TypeNameEnum.INT)
  val intArrayValueType = new ValueType()
  intArrayValueType.typeName(TypeNameEnum.ARRAY)
  intArrayValueType.arrayType(intValueType)
  wdlWorkflowDescriptionIntArrayRequiredInput.setValueType(intArrayValueType)

  val arrayWdlWorkflowDescription =  new WorkflowDescription()
  arrayWdlWorkflowDescription.addInputsItem(wdlWorkflowDescriptionIntArrayRequiredInput)
  arrayWdlWorkflowDescription.setName(arrayWDLName)
  arrayWdlWorkflowDescription.setValid(true)


  mockCromwellSwaggerClient.workflowDescriptions += (littleWdl -> littleWdlWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (arrayWdl  -> arrayWdlWorkflowDescription)



  //Test harness to call resolveInputsForEntities without having to go via the WorkspaceService
  def testResolveInputs(workspaceContext: SlickWorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String, dataAccess: DataAccess)
                       (implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
    dataAccess.entityQuery.findEntityByName(workspaceContext.workspaceId, entity.entityType, entity.name).result flatMap { entityRecs =>
      methodConfigResolver.gatherInputs(userInfo, methodConfig, wdl) match {
        case scala.util.Failure(exception) =>
          DBIO.failed(exception)
        case scala.util.Success(gatherInputsResult: GatherInputsResult)
          if gatherInputsResult.extraInputs.nonEmpty || gatherInputsResult.missingInputs.nonEmpty =>
          DBIO.failed(new RawlsException(s"gatherInputsResult has missing or extra inputs: $gatherInputsResult"))
        case scala.util.Success(gatherInputsResult: GatherInputsResult) =>
          methodConfigResolver.evaluateInputExpressions(workspaceContext, gatherInputsResult.processableInputs, Some(entityRecs), dataAccess)
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
      val gatheredInputs = methodConfigResolver.gatherInputs(userInfo, configMissingExpr, littleWdl)
      gatheredInputs shouldBe 'success
      gatheredInputs.get.processableInputs shouldBe 'empty
      gatheredInputs.get.missingInputs shouldBe Set(intArgName)
      gatheredInputs.get.emptyOptionalInputs.map(_.workflowInput.getName) shouldBe Set(intOptName)
    }

    "resolve empty lists into AttributeEmptyLists" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      runAndWait(testResolveInputs(context, configEmptyArray, sampleSet2, arrayWdl, this)) shouldBe
        Map(sampleSet2.name -> Seq(SubmissionValidationValue(Some(AttributeValueEmptyList), None, intArrayName)))
    }

    "unpack AttributeValueRawJson into WDL-arrays" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      val doubleArrayWdlWorkflowDescriptionRequiredInput = new ToolInputParameter()
      doubleArrayWdlWorkflowDescriptionRequiredInput.setName(doubleArrayWDLIntArrayRequiredInput)
      doubleArrayWdlWorkflowDescriptionRequiredInput.setOptional(false)
      val intValueType = new ValueType()
      intValueType.typeName(TypeNameEnum.INT)
      val intArrayValueType = new ValueType()
      intArrayValueType.typeName(TypeNameEnum.ARRAY)
      intArrayValueType.arrayType(intValueType)
      val intArrayArrayValueType = new ValueType()
      intArrayArrayValueType.typeName(TypeNameEnum.ARRAY)
      intArrayValueType.arrayType(intArrayValueType)

      doubleArrayWdlWorkflowDescriptionRequiredInput.setValueType(intArrayArrayValueType)

      val doubleArrayWdlWorkflowDescription =  new WorkflowDescription()
      doubleArrayWdlWorkflowDescription.addInputsItem(doubleArrayWdlWorkflowDescriptionRequiredInput)
      doubleArrayWdlWorkflowDescription.setName(doubleArrayWDLName)
      doubleArrayWdlWorkflowDescription.setValid(true)

      mockCromwellSwaggerClient.workflowDescriptions += (doubleArrayWdl -> doubleArrayWdlWorkflowDescription)
      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonDoubleArray, sampleSet2, doubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
    }

    "unpack AttributeValueRawJson into optional WDL-arrays" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      val doubleArrayWdlWorkflowDescriptionOptional= new ToolInputParameter()
      doubleArrayWdlWorkflowDescriptionOptional.setName(optionalDoubleArrayWDLInput)
      doubleArrayWdlWorkflowDescriptionOptional.setOptional(true)
      val intValueType = new ValueType()
      intValueType.typeName(TypeNameEnum.INT)
      val intArrayValueType = new ValueType()
      intArrayValueType.typeName(TypeNameEnum.ARRAY)
      intArrayValueType.arrayType(intValueType)
      val intArrayArrayValueType = new ValueType()
      intArrayArrayValueType.typeName(TypeNameEnum.ARRAY)
      intArrayValueType.arrayType(intArrayValueType)

      doubleArrayWdlWorkflowDescriptionOptional.setValueType(intArrayArrayValueType)

      val doubleArrayWdlWorkflowDescription =  new WorkflowDescription()
      doubleArrayWdlWorkflowDescription.addInputsItem(doubleArrayWdlWorkflowDescriptionOptional)
      doubleArrayWdlWorkflowDescription.setName(optionalDoubleArrayWDLName)
      doubleArrayWdlWorkflowDescription.setValid(true)

      mockCromwellSwaggerClient.workflowDescriptions += (optionalDoubleArrayWdl -> doubleArrayWdlWorkflowDescription)

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonDoubleArray, sampleSet2, optionalDoubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
    }

    "unpack AttributeValueRawJson into lists-of WDL-arrays" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      val doubleArrayWdlWorkflowDescriptionOptional= new ToolInputParameter()
      doubleArrayWdlWorkflowDescriptionOptional.setName(tripleArrayWDLRequiredInput)
      doubleArrayWdlWorkflowDescriptionOptional.setOptional(true)
      val intValueType = new ValueType()
      intValueType.typeName(TypeNameEnum.INT)
      val intArrayValueType = new ValueType()
      intArrayValueType.typeName(TypeNameEnum.ARRAY)
      intArrayValueType.arrayType(intValueType)
      val intArrayArrayValueType = new ValueType()
      intArrayArrayValueType.typeName(TypeNameEnum.ARRAY)
      intArrayValueType.arrayType(intArrayValueType)
      val intTripleArrayValueType = new ValueType()
      intTripleArrayValueType.typeName(TypeNameEnum.ARRAY)
      intTripleArrayValueType.arrayType(intArrayArrayValueType)

      doubleArrayWdlWorkflowDescriptionOptional.setValueType(intTripleArrayValueType)

      val doubleArrayWdlWorkflowDescription =  new WorkflowDescription()
      doubleArrayWdlWorkflowDescription.addInputsItem(doubleArrayWdlWorkflowDescriptionOptional)
      doubleArrayWdlWorkflowDescription.setName(tripleArrayWDLName)
      doubleArrayWdlWorkflowDescription.setValid(true)

      mockCromwellSwaggerClient.workflowDescriptions += (tripleArrayWdl -> doubleArrayWdlWorkflowDescription)

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonTripleArray, sampleSet2, tripleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

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
      val littleWorkflow = methodConfigResolver.parseWDL(userInfo, littleWdl).get

      assertResult(littleWdlWorkflowDescription) {
        littleWorkflow
      }

      val arrayWorkflow = methodConfigResolver.parseWDL(userInfo, arrayWdl).get

      assertResult(arrayWdlWorkflowDescription) {
        arrayWorkflow
      }
    }

    "parse WDL with syntax errors" in withConfigData {

      val badWdlParse = methodConfigResolver.parseWDL(userInfo, badWdl)


      assert(badWdlParse.isSuccess)
      assertResult(badWdlWorkflowDescription) {
        badWdlParse.get
      }

    }

    "get method config inputs and outputs" ignore withConfigData {
      val expectedLittleIO = MethodInputsOutputs(Seq(
        MethodInput(intArgName, "Int", false),
        MethodInput(intOptName, "Int", true)), Seq())

      assertResult(expectedLittleIO) {
        methodConfigResolver.getMethodInputsOutputs(userInfo, littleWdl).get
      }

      val expectedArrayIO = MethodInputsOutputs(Seq(
        MethodInput(intArrayName, "Array[Int]", false)), Seq())

//      assertResult(expectedArrayIO) {
//        methodConfigResolver.getMethodInputsOutputs(userInfo, arrayWdl).get
//      }

      val badIO = methodConfigResolver.getMethodInputsOutputs(userInfo, badWdl)
      assert(badIO.isFailure)
      intercept[RawlsException] {
        badIO.get
      }
    }

    "create a Method Config from a template" ignore withConfigData {
      val expectedLittleInputs = Map(intArgName -> AttributeString(""), intOptName -> AttributeString(""))
      val expectedLittleTemplate = MethodConfiguration("namespace", "name", Some("rootEntityType"), Some(Map()), expectedLittleInputs, Map(), dummyMethod)

      assertResult(expectedLittleTemplate) { methodConfigResolver.toMethodConfiguration(userInfo, littleWdl, dummyMethod).get }

      val expectedArrayInputs = Map(intArrayName -> AttributeString(""))
      val expectedArrayTemplate = MethodConfiguration("namespace", "name", Some("rootEntityType"), Some(Map()), expectedArrayInputs, Map(), dummyMethod)

      assertResult(expectedArrayTemplate) { methodConfigResolver.toMethodConfiguration(userInfo, arrayWdl, dummyMethod).get }

      val badTemplate = methodConfigResolver.toMethodConfiguration(userInfo, badWdl, dummyMethod)
      assert(badTemplate.isFailure)
      intercept[RawlsException] {
        badTemplate.get
      }
    }
  }
}
