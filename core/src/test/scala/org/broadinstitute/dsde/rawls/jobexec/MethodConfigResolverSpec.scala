package org.broadinstitute.dsde.rawls.jobexec

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._

import scala.collection.immutable.Map
import org.scalatest.{Matchers, WordSpecLike}
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import java.util.UUID

import wdl4s.types.{WdlArrayType, WdlIntegerType, WdlOptionalType}
import wdl4s.WorkflowInput

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

  val badWdl = littleWdl.replace("workflow", "not-a-workflow")

  val intArgName = "w1.t1.int_arg"
  val intOptName = "w1.t1.int_opt"
  val intArrayName = "w1.int_array"

  val workspace = new Workspace("workspaces", "test_workspace", None, UUID.randomUUID().toString(), "aBucket", currentTime(), currentTime(), "testUser", Map.empty, Map.empty, Map.empty)

  val sampleGood = new Entity("sampleGood", "Sample", Map(AttributeName.withDefaultNS("blah") -> AttributeNumber(1)))
  val sampleGood2 = new Entity("sampleGood2", "Sample", Map(AttributeName.withDefaultNS("blah") -> AttributeNumber(2)))
  val sampleMissingValue = new Entity("sampleMissingValue", "Sample", Map.empty)

  val sampleSet = new Entity("daSampleSet", "SampleSet",
    Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      AttributeEntityReference("Sample", "sampleGood"),
      AttributeEntityReference("Sample", "sampleMissingValue")
    )))
  )

  val sampleSet2 = new Entity("daSampleSet2", "SampleSet",
    Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      AttributeEntityReference("Sample", "sampleGood"),
      AttributeEntityReference("Sample", "sampleGood2")
    )))
  )

  val dummyMethod = MethodRepoMethod("method_namespace", "test_method", 1)

  val configGood = MethodConfiguration("config_namespace", "configGood", "Sample",
    Map.empty, Map(intArgName -> AttributeString("this.blah")), Map.empty, dummyMethod)

  val configEvenBetter = MethodConfiguration("config_namespace", "configGood", "Sample",
    Map.empty, Map(intArgName -> AttributeString("this.blah"), intOptName -> AttributeString("this.blah")),
    Map.empty, dummyMethod)

  val configMissingExpr = MethodConfiguration("config_namespace", "configMissingExpr", "Sample",
    Map.empty, Map.empty, Map.empty, dummyMethod)

  val configSampleSet = MethodConfiguration("config_namespace", "configSampleSet", "SampleSet",
    Map.empty, Map(intArrayName -> AttributeString("this.samples.blah")), Map.empty, dummyMethod)

  val configEmptyArray = MethodConfiguration("config_namespace", "configSampleSet", "SampleSet",
    Map.empty, Map(intArrayName -> AttributeString("this.nonexistent")), Map.empty, dummyMethod)

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
        case scala.util.Success(methodInputs) =>
          MethodConfigResolver.resolveInputsForEntities(workspaceContext, methodInputs, entityRecs, dataAccess)
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

    "resolve empty lists into AttributeEmptyLists" in withConfigData {
      val context = SlickWorkspaceContext(workspace)

      runAndWait(testResolveInputs(context, configEmptyArray, sampleSet2, arrayWdl, this)) shouldBe
        Map(sampleSet2.name -> Seq(SubmissionValidationValue(Some(AttributeValueEmptyList), None, intArrayName)))
    }

    "parse WDL" in withConfigData {
      val littleWorkflow = MethodConfigResolver.parseWDL(littleWdl).get

      val expectedLittleInputs = Map(
        intArgName -> WorkflowInput(intArgName, WdlIntegerType),
        intOptName -> WorkflowInput(intOptName, WdlOptionalType(WdlIntegerType)))

      assertResult(expectedLittleInputs) {
        littleWorkflow.inputs
      }
      assertResult(Seq()) {
        littleWorkflow.outputs
      }

      val arrayWorkflow = MethodConfigResolver.parseWDL(arrayWdl).get

      val expectedArrayInputs = Map(intArrayName -> WorkflowInput(intArrayName, WdlArrayType(WdlIntegerType)))

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
      val expectedLittleTemplate = MethodConfiguration("namespace", "name", "rootEntityType", Map(), expectedLittleInputs, Map(), dummyMethod)

      assertResult(expectedLittleTemplate) { MethodConfigResolver.toMethodConfiguration(littleWdl, dummyMethod).get }

      val expectedArrayInputs = Map(intArrayName -> AttributeString(""))
      val expectedArrayTemplate = MethodConfiguration("namespace", "name", "rootEntityType", Map(), expectedArrayInputs, Map(), dummyMethod)

      assertResult(expectedArrayTemplate) { MethodConfigResolver.toMethodConfiguration(arrayWdl, dummyMethod).get }

      val badTemplate = MethodConfigResolver.toMethodConfiguration(badWdl, dummyMethod)
      assert(badTemplate.isFailure)
      intercept[RawlsException] {
        badTemplate.get
      }
    }
  }
}
