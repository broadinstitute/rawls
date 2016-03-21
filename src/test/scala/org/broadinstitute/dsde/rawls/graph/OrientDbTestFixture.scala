package org.broadinstitute.dsde.rawls.graph

import java.util.UUID

import com.tinkerpop.blueprints.impls.orient.{OrientVertex, OrientGraph}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.db.{TestData, TestDb, DbTestFixture}

import org.broadinstitute.dsde.rawls.TestExecutionContext.testExecutionContext
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProject, RawlsUser}

trait OrientDbTestFixture extends DbTestFixture with OrientTestDb {
  this: org.scalatest.BeforeAndAfterAll with org.scalatest.Suite =>
}

trait OrientTestDb extends TestDb {
  lazy val entityDAO = new GraphEntityDAO()
  lazy val workspaceDAO = new GraphWorkspaceDAO()
  lazy val methodConfigDAO = new GraphMethodConfigurationDAO()
  lazy val authDAO = new GraphAuthDAO()
  lazy val billingDAO = new GraphBillingDAO()
  lazy val submissionDAO = new GraphSubmissionDAO()
  lazy val workflowDAO = new GraphWorkflowDAO(submissionDAO)

  def withCustomTestDatabase(data: TestData)(testCode: DataSource => Any): Unit = {
    val dbName = UUID.randomUUID.toString
    val dataSource = DataSource("memory:" + dbName, "admin", "admin")
    try {
      val graph = new OrientGraph("memory:" + dbName)

      // do this twice to make sure it is idempotent
      VertexSchema.createVertexClasses(graph)
      VertexSchema.createVertexClasses(graph)

      // save the data inside a transaction to cause data to be committed
      dataSource.inTransaction() { txn =>
        data.save(txn)
      }

      testCode(dataSource)
      graph.rollback()
      graph.drop()
      graph.shutdown()
    } catch {
      case t: Throwable => t.printStackTrace; throw t
    }
  }

  import scala.collection.JavaConversions._

  def userInDb(dataSource: DataSource, user: RawlsUser) = dataSource.inTransaction() { txn => txn.withGraph { graph =>
    graph.getVertices.exists(v => {
      v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.User) &&
        v.getProperty[String]("userSubjectId") == user.userSubjectId.value
    })
  }}

  def billingProjectInDb(dataSource: DataSource, project: RawlsBillingProject) = dataSource.inTransaction() { txn => txn.withGraph { graph =>
    graph.getVertices.exists(v => {
      v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.BillingProject) &&
        v.getProperty[String]("projectName") == project.projectName.value
    })
  }}

}
