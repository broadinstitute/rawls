package org.broadinstitute.dsde.rawls.jobexec

import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{WordSpecLike, Matchers}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestData
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import java.util.UUID

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

  val intArgName = "w1.t1.int_arg"
  val intOptName = "w1.t1.int_opt"
  val intArrayName = "w1.int_array"

  val workspace = new Workspace("workspaces", "test_workspace", UUID.randomUUID().toString(), "aBucket", DateTime.now, DateTime.now, "testUser", Map.empty, Map.empty)

  val sampleGood = new Entity("sampleGood", "Sample", Map("blah" -> AttributeNumber(1)))
  val sampleMissingValue = new Entity("sampleMissingValue", "Sample", Map.empty)

  val sampleSet = new Entity("daSampleSet", "SampleSet",
    Map("samples" -> AttributeEntityReferenceList(Seq(
      AttributeEntityReference("Sample", "sampleGood"),
      AttributeEntityReference("Sample", "sampleMissingValue")
    )))
  )

  val configGood = new MethodConfiguration("config_namespace", "configGood", "Sample",
    Map.empty, Map(intArgName -> AttributeString("this.blah")), Map.empty,
    MethodRepoMethod( "method_namespace", "test_method", 1))

  val configEvenBetter = new MethodConfiguration("config_namespace", "configGood", "Sample",
    Map.empty, Map(intArgName -> AttributeString("this.blah"), intOptName -> AttributeString("this.blah")), Map.empty,
    MethodRepoMethod( "method_namespace", "test_method", 1))

  val configMissingExpr = new MethodConfiguration("config_namespace", "configMissingExpr", "Sample",
    Map.empty, Map.empty, Map.empty,
   MethodRepoMethod( "method_namespace", "test_method", 1))

  val configSampleSet = new MethodConfiguration("config_namespace", "configSampleSet", "SampleSet",
    Map.empty, Map(intArrayName -> AttributeString("this.samples.blah")), Map.empty,
    MethodRepoMethod( "method_namespace", "test_method", 1))

  class ConfigData extends TestData {
    override def save() = {
      DBIO.seq(
        workspaceQuery.save(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(context, sampleGood),
            entityQuery.save(context, sampleMissingValue),
            entityQuery.save(context, sampleSet),
            methodConfigurationQuery.save(context, configGood),
            methodConfigurationQuery.save(context, configMissingExpr),
            methodConfigurationQuery.save(context, configSampleSet)
          )
        }
      )
    }
  }
  val configData = new ConfigData()

  def withConfigData(testCode: => Any): Unit = {
    withCustomTestDatabaseInternal(configData)(testCode)
  }

  "MethodConfigResolver" should {
    "resolve method config inputs" in withConfigData {
      val context = new SlickWorkspaceContext(workspace)
      runAndWait(MethodConfigResolver.resolveInputsOrGatherErrors(context, configGood, sampleGood, littleWdl, this)) shouldBe Right(Map(intArgName -> AttributeNumber(1)))
      runAndWait(MethodConfigResolver.resolveInputsOrGatherErrors(context, configEvenBetter, sampleGood, littleWdl, this)) shouldBe Right(Map(intArgName -> AttributeNumber(1), intOptName -> AttributeNumber(1)))
      runAndWait(MethodConfigResolver.resolveInputsOrGatherErrors(context, configSampleSet, sampleSet, arrayWdl, this)) shouldBe Right(Map(intArrayName -> AttributeValueList(Seq(AttributeNumber(1)))))

      // failure cases
      runAndWait(MethodConfigResolver.resolveInputsOrGatherErrors(context, configGood, sampleMissingValue, littleWdl, this)) should matchPattern { case Left(Seq(_)) => }
      runAndWait(MethodConfigResolver.resolveInputsOrGatherErrors(context, configMissingExpr, sampleGood, littleWdl, this)) should matchPattern { case Left(Seq(_)) => }
    }
  }
}
