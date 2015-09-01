package org.broadinstitute.dsde.rawls.jobexec

import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, RawlsTransaction}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{WordSpecLike, Matchers, FlatSpec}

import scala.util.{Failure, Success}

class MethodConfigResolverSpec extends WordSpecLike with Matchers with OrientDbTestFixture {
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

  val workspace = new Workspace("workspaces", "test_workspace", "aBucket", DateTime.now, "testUser", Map.empty)

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
    MethodRepoConfiguration( "method_config_namespace", "test_method", "1"), MethodRepoMethod( "method_namespace", "test_method", "1"))

  val configEvenBetter = new MethodConfiguration("config_namespace", "configGood", "Sample",
    Map.empty, Map(intArgName -> AttributeString("this.blah"), intOptName -> AttributeString("this.blah")), Map.empty,
    MethodRepoConfiguration( "method_config_namespace", "test_method", "1"), MethodRepoMethod( "method_namespace", "test_method", "1"))

  val configMissingExpr = new MethodConfiguration("config_namespace", "configMissingExpr", "Sample",
    Map.empty, Map.empty, Map.empty,
    MethodRepoConfiguration( "method_config_namespace", "test_method", "1"), MethodRepoMethod( "method_namespace", "test_method", "1"))

  val configSampleSet = new MethodConfiguration("config_namespace", "configSampleSet", "SampleSet",
    Map.empty, Map(intArrayName -> AttributeString("this.samples.blah")), Map.empty,
    MethodRepoConfiguration("method_config_namespace", "test_method", "1"), MethodRepoMethod( "method_namespace", "test_method", "1"))

  class ConfigData extends TestData {
    override def save(txn: RawlsTransaction): Unit = {
      workspaceDAO.save(workspace, txn)
      withWorkspaceContext(workspace, txn) { context =>
        entityDAO.save(context, sampleGood, txn)
        entityDAO.save(context, sampleMissingValue, txn)
        entityDAO.save(context, sampleSet, txn)
        methodConfigDAO.save(context, configGood, txn)
        methodConfigDAO.save(context, configMissingExpr, txn)
        methodConfigDAO.save(context, configSampleSet, txn)
      }
    }
  }
  val configData = new ConfigData()

  def withConfigData(testCode:DataSource => Any): Unit = {
    withCustomTestDatabase(configData) { dataSource =>
      testCode(dataSource)
    }
  }

  "MethodConfigResolver" should {
    "resolve method config inputs" in withConfigData { dataSource =>
      dataSource.inTransaction { txn =>
        withWorkspaceContext(workspace, txn) { context =>
          // success cases
          MethodConfigResolver.resolveInputsOrGatherErrors(context, configGood, sampleGood, littleWdl) shouldBe Right(Map(intArgName -> AttributeNumber(1), intOptName -> AttributeNull))
          MethodConfigResolver.resolveInputsOrGatherErrors(context, configEvenBetter, sampleGood, littleWdl) shouldBe Right(Map(intArgName -> AttributeNumber(1), intOptName -> AttributeNumber(1)))
          MethodConfigResolver.resolveInputsOrGatherErrors(context, configSampleSet, sampleSet, arrayWdl) shouldBe Right(Map(intArrayName -> AttributeValueList(Seq(AttributeNumber(1)))))

          // failure cases
          MethodConfigResolver.resolveInputsOrGatherErrors(context, configGood, sampleMissingValue, littleWdl) should matchPattern { case Left(Seq(_)) => }
          MethodConfigResolver.resolveInputsOrGatherErrors(context, configMissingExpr, sampleGood, littleWdl) should matchPattern { case Left(Seq(_)) => }
        }
      }
    }
  }
}
