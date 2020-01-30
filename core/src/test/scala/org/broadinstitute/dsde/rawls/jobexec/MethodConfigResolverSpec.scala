package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{Matchers, WordSpecLike}
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.dataaccess.MockCromwellSwaggerClient._
import org.broadinstitute.dsde.rawls.jobexec.wdlparsing.WDLParser
import scala.collection.immutable.Map
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class MethodConfigResolverSpec extends WordSpecLike with Matchers with TestDriverComponent {

  import driver.api._

  val littleWdl =
    WdlSource(
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
      """.stripMargin)

  val arrayWdl =
    WdlSource(
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
      """.stripMargin)

  val doubleArrayWdl =
    WdlSource(
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
      """.stripMargin)


  val optionalDoubleArrayWdl =
    WdlSource(
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
      """.stripMargin)


  val tripleArrayWdl =
    WdlSource(
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
      """.stripMargin)

  val wdlVersionOneWdl =
    WdlSource(
      """
        |version 1.0
        |
        |task use_this_name {
        |
        |  input {
        |    String s
        |    File f
        |  }
        |
        |  command {}
        |
        |  meta {
        |    email: "skroob@spaceballs.gov"
        |    author: "President Skroob"
        |    description: "Spaceballs: The Unit Test"
        |  }
        |
        |  output {
        |    File f2 = "a"
        |  }
        |
        |  runtime {
        |    docker: "docker image"
        |  }
        |}
      """.stripMargin)

  val badWdl = WdlSource(littleWdl.source.replace("workflow", "i-am-not-a-workflow"))

  val littleWdlName = "w1"
  val intArgName = "t1.int_arg"
  val intArgNameWithWfName = "w1.t1.int_arg"
  val intOptName = "t1.int_opt"
  val intOptNameWithWfName = "w1.t1.int_opt"
  val intArrayName = "int_array"
  val intArrayNameWithWfName = "w1.int_array"
  val doubleIntArrayName = "aint_array"
  val doubleIntArrayNameWithWfName = "w1.aint_array"
  val tripleIntArrayName = "aaint_array"
  val tripleIntArrayNameWithWfName = "w1.aaint_array"
  val wdlVersionOneWdlName = "use_this_name"
  val wdlVersionOneStringInputName = "s"
  val wdlVersionOneFileInputName = "f"
  val wdlVersionOneFileOutputName = "f2"

  val littleWdlWorkflowDescriptionRequiredInput = makeToolInputParameter(intArgName, false, makeValueType("Int"), "Int")
  val littleWdlWorkflowDescriptionOptionalInput = makeToolInputParameter(intOptName, true, makeValueType("Int"), "Int?")
  val littleWdlWorkflowDescription = makeWorkflowDescription("w1", List(littleWdlWorkflowDescriptionRequiredInput, littleWdlWorkflowDescriptionOptionalInput), List.empty)

  val requiredArrayInput = makeToolInputParameter(intArrayName, false, makeArrayValueType(makeValueType("Int")), "Array[Int]")
  val requiredArrayWorkflowDescription = makeWorkflowDescription("w1", List(requiredArrayInput), List.empty)

  val requiredDoubleArrayInput =   makeToolInputParameter(doubleIntArrayName, false, makeArrayValueType(makeArrayValueType(makeValueType("Int"))), "Array[Array[Int]]")
  val requiredDoubleArrayWorkflowDescription =  makeWorkflowDescription("w1", List(requiredDoubleArrayInput), List.empty)

  val optionalDoubleArrayInput =   makeToolInputParameter(doubleIntArrayName, true, makeOptionalValueType(makeArrayValueType(makeArrayValueType(makeValueType("Int")))), "Array[Array[Int]]?")
  val optionalDoubleArrayWorkflowDescription =  makeWorkflowDescription("w1", List(optionalDoubleArrayInput), List.empty)


  val requiredTripleArrayInput  = makeToolInputParameter(tripleIntArrayName, true, makeArrayValueType(makeArrayValueType(makeArrayValueType(makeValueType("Int")))), "Array[Array[Array[Int]]]")
  val requiredTripleArrayWorkflowDescription =  makeWorkflowDescription("w1", List(requiredTripleArrayInput), List.empty)

  val badWdlWorkflowDescription = makeBadWorkflowDescription("badwdl", List("ERROR: Finished parsing without consuming all tokens.\n\nBad syntax workflow returned from Agora mock server\n^\n     "))

  val wdlVersionOneWdlStringInput = makeToolInputParameter(wdlVersionOneStringInputName, false, makeValueType("String"), "String")
  val wdlVersionOneWdlFileInput   = makeToolInputParameter(wdlVersionOneFileInputName, false, makeValueType("File"), "File")
  val wdlVersionOneWdlFileOutput  = makeToolOutputParameter(wdlVersionOneFileOutputName, makeValueType("File"), "File")
  val wdlVersionOneWdlWorkflowDescription = makeWorkflowDescription(wdlVersionOneWdlName, List(wdlVersionOneWdlStringInput, wdlVersionOneWdlFileInput), List(wdlVersionOneWdlFileOutput))

  mockCromwellSwaggerClient.workflowDescriptions += (littleWdl -> littleWdlWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (arrayWdl  -> requiredArrayWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (doubleArrayWdl -> requiredDoubleArrayWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (optionalDoubleArrayWdl -> optionalDoubleArrayWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (tripleArrayWdl -> requiredTripleArrayWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (WdlSource("Bad syntax workflow returned from Agora mock server") -> badWdlWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (wdlVersionOneWdl -> wdlVersionOneWdlWorkflowDescription)

  println(s"### finished creating workflowDescriptions: ${mockCromwellSwaggerClient.workflowDescriptions.keys.mkString(",")}")

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
    None, Map(intArgNameWithWfName -> AttributeString("this.blah")), Map.empty, dummyMethod)

  val configEvenBetter = MethodConfiguration("config_namespace", "configGood", Some("Sample"),
    None, Map(intArgNameWithWfName -> AttributeString("this.blah"), intOptNameWithWfName -> AttributeString("this.blah")),
    Map.empty, dummyMethod)

  val configMissingExpr = MethodConfiguration("config_namespace", "configMissingExpr", Some("Sample"),
    None, Map.empty, Map.empty, dummyMethod)

  val configSampleSet = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    None, Map(intArrayNameWithWfName -> AttributeString("this.samples.blah")), Map.empty, dummyMethod)

  val configEmptyArray = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    None, Map(intArrayNameWithWfName -> AttributeString("this.nonexistent")), Map.empty, dummyMethod)

  val configRawJsonDoubleArray = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    None, Map(doubleIntArrayNameWithWfName -> AttributeString("this.rawJsonDoubleArray")), Map.empty, dummyMethod)

  val configRawJsonTripleArray = MethodConfiguration("config_namespace", "configSample", Some("Sample"),
    None, Map(tripleIntArrayNameWithWfName -> AttributeString("this.samples.rawJsonDoubleArray")), Map.empty, dummyMethod)

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
  def testResolveInputs(workspaceContext: SlickWorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: WDL, dataAccess: DataAccess)
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
        Map(sampleGood.name -> Seq(SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName)))

      runAndWait(testResolveInputs(context, configEvenBetter, sampleGood, littleWdl, this)) shouldBe
        Map(sampleGood.name -> Seq(SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName), SubmissionValidationValue(Some(AttributeNumber(1)), None, intOptNameWithWfName)))

      runAndWait(testResolveInputs(context, configSampleSet, sampleSet, arrayWdl, this)) shouldBe
        Map(sampleSet.name -> Seq(SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1)))), None, intArrayNameWithWfName)))

      runAndWait(testResolveInputs(context, configSampleSet, sampleSet2, arrayWdl, this)) shouldBe
        Map(sampleSet2.name -> Seq(SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1), AttributeNumber(2)))), None, intArrayNameWithWfName)))

      // failure cases
      assertResult(true, "Missing values should return an error") {
        runAndWait(testResolveInputs(context, configGood, sampleMissingValue, littleWdl, this)).get("sampleMissingValue").get match {
          case List(SubmissionValidationValue(None, Some(_), intArg)) if intArg == intArgNameWithWfName => true
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
      gatheredInputs.get.missingInputs shouldBe Set(intArgNameWithWfName)
      gatheredInputs.get.emptyOptionalInputs.map(_.workflowInput.getName) shouldBe Set("w1.t1.int_opt")
    }

    "resolve empty lists into AttributeEmptyLists" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      runAndWait(testResolveInputs(context, configEmptyArray, sampleSet2, arrayWdl, this)) shouldBe
        Map(sampleSet2.name -> Seq(SubmissionValidationValue(Some(AttributeValueEmptyList), None, intArrayNameWithWfName)))
    }

    "unpack AttributeValueRawJson into WDL-arrays" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonDoubleArray, sampleSet2, doubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
    }

    "unpack AttributeValueRawJson into optional WDL-arrays" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonDoubleArray, sampleSet2, optionalDoubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
    }

    "unpack AttributeValueRawJson into lists-of WDL-arrays" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

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

    "parse draft2 WDL" in withConfigData {
      val littleWorkflow = methodConfigResolver.parseWDL(userInfo, littleWdl).get

      littleWorkflow.getName shouldBe littleWdlName
      littleWorkflow.getValid shouldBe true
      littleWorkflow.getInputs.size shouldBe (2)
      littleWorkflow.getInputs shouldBe List(
        makeToolInputParameter(intArgNameWithWfName, false, makeValueType("Int"), "Int"),
        makeToolInputParameter(intOptNameWithWfName, true, makeValueType("Int"), "Int?")).asJava

      val arrayWorkflow = methodConfigResolver.parseWDL(userInfo, arrayWdl).get

      arrayWorkflow.getName shouldBe littleWdlName
      arrayWorkflow.getValid shouldBe true
      arrayWorkflow.getInputs.size shouldBe (1)
      arrayWorkflow.getInputs shouldBe List(
        makeToolInputParameter(intArrayNameWithWfName, false, makeArrayValueType(makeValueType("Int")), "Array[Int]")).asJava


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
      val expectedLittleIO = MethodInputsOutputs(Seq(
        MethodInput(intArgNameWithWfName, "Int", false),
        MethodInput(intOptNameWithWfName, "Int?", true)), Seq())

      assertResult(expectedLittleIO) {
        methodConfigResolver.getMethodInputsOutputs(userInfo, littleWdl).get
      }

      val expectedArrayIO = MethodInputsOutputs(Seq(
        MethodInput(intArrayNameWithWfName, "Array[Int]", false)), Seq())

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
      val expectedLittleInputs = Map(intArgNameWithWfName -> AttributeString(""), intOptNameWithWfName -> AttributeString(""))
      val expectedLittleTemplate = MethodConfiguration("namespace", "name", Some("rootEntityType"), Some(Map()), expectedLittleInputs, Map(), dummyMethod)

      assertResult(expectedLittleTemplate) { methodConfigResolver.toMethodConfiguration(userInfo, littleWdl, dummyMethod).get }

      val expectedArrayInputs = Map(intArrayNameWithWfName -> AttributeString(""))
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
