package org.broadinstitute.dsde.rawls.jobexec

import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, RawlsTransaction}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{WordSpecLike, Matchers, FlatSpec}

class MethodConfigResolverSpec extends WordSpecLike with Matchers with OrientDbTestFixture {
  val littleWdl =
    """
      |task t1 {
      |  command {
      |    echo ${Int int_arg}
      |  }
      |}
      |
      |workflow w1 {
      |  call t1
      |}
    """.stripMargin

  val intArgName = "w1.t1.int_arg"

  val workspace = new Workspace("workspaces", "test_workspace", "aBucket", DateTime.now, "testUser", Map.empty)

  val sampleGood = new Entity("sampleGood", "Sample", Map("blah" -> AttributeNumber(1)))
  val sampleMissingValue = new Entity("sampleMissingValue", "Sample", Map.empty)

  val configGood = new MethodConfiguration("config_namespace", "configGood", "Sample",
    Map.empty, Map(intArgName -> AttributeString("this.blah")), Map.empty,
    MethodRepoConfiguration( "method_config_namespace", "test_method", "1"), MethodRepoMethod( "method_namespace", "test_method", "1"))

  val configMissingExpr = new MethodConfiguration("config_namespace", "configMissingExpr", "Sample",
    Map.empty, Map.empty, Map.empty,
    MethodRepoConfiguration( "method_config_namespace", "test_method", "1"), MethodRepoMethod( "method_namespace", "test_method", "1"))

  class ConfigData extends TestData {
    override def save(txn: RawlsTransaction): Unit = {
      workspaceDAO.save(workspace, txn)
      withWorkspaceContext(workspace, txn) { context =>
        entityDAO.save(context, sampleGood, txn)
        entityDAO.save(context, sampleMissingValue, txn)
        methodConfigDAO.save(context, configGood, txn)
        methodConfigDAO.save(context, configMissingExpr, txn)
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
    "get validation errors" in withConfigData { dataSource =>
      dataSource.inTransaction { txn =>
        withWorkspaceContext(workspace, txn) { context =>
          MethodConfigResolver.getValidationErrors(context, configGood, sampleGood, littleWdl, txn).get(intArgName) should be(None) // Valid input
          MethodConfigResolver.getValidationErrors(context, configGood, sampleMissingValue, littleWdl, txn).get(intArgName) shouldNot be(None) // Entity missing value
          MethodConfigResolver.getValidationErrors(context, configMissingExpr, sampleGood, littleWdl, txn).get(intArgName) shouldNot be(None) // Config missing expr
        }
      }
    }
  }
}
