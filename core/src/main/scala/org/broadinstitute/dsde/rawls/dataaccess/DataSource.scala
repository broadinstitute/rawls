package org.broadinstitute.dsde.rawls.dataaccess

import _root_.slick.basic.DatabaseConfig
import _root_.slick.jdbc.{JdbcProfile, TransactionIsolation}
import com.google.common.base.Throwables
import com.typesafe.scalalogging.LazyLogging
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.{ClassLoaderResourceAccessor, ResourceAccessor}
import liquibase.{Contexts, Liquibase}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, DataAccessComponent, ReadWriteAction}
import sun.security.provider.certpath.SunCertPathBuilderException

import java.sql.SQLTimeoutException
import scala.concurrent.{ExecutionContext, Future}

object DataSource {
  def apply(databaseConfig: DatabaseConfig[JdbcProfile])(implicit executionContext: ExecutionContext): SlickDataSource =
    new SlickDataSource(databaseConfig)
}

object AttributeTempTableType extends Enumeration {
  val Workspace, Entity = Value
}

class SlickDataSource(val databaseConfig: DatabaseConfig[JdbcProfile])(implicit executionContext: ExecutionContext)
    extends LazyLogging {
  val batchSize = databaseConfig.config.getInt("batchSize")
  val fetchSize = databaseConfig.config.getInt("fetchSize")

  val dataAccess = new DataAccessComponent(databaseConfig.profile, batchSize, fetchSize)

  val database = databaseConfig.db

  import dataAccess.driver.api._

  def inTransaction[T](f: (DataAccess) => ReadWriteAction[T],
                       isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): Future[T] =
    database.run(f(dataAccess).transactionally.withTransactionIsolation(isolationLevel))

  def createEntityAttributeTempTable =
    sql"""call createEntityAttributeTempTable()""".as[Boolean]

  def dropEntityAttributeTempTable =
    sql"""call dropEntityAttributeTempTable""".as[Boolean]

  def createWorkspaceAttributeTempTable =
    sql"""call createWorkspaceAttributeTempTable()""".as[Boolean]

  def dropWorkspaceAttributeTempTable =
    sql"""call dropWorkspaceAttributeTempTable""".as[Boolean]

  // creates the ENTITY_ATTRIBUTE_TEMP, WORSKPACE_ATTRIBUTE_TEMP, or both as specified by tempTableTypes  for use by this transaction, executes the transaction
  def inTransactionWithAttrTempTable[T](tempTableTypes: Set[AttributeTempTableType.Value])(
    f: (DataAccess) => ReadWriteAction[T],
    isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): Future[T] = {

    val callerAction = f(dataAccess).transactionally.withTransactionIsolation(isolationLevel)

    val entityTemp = tempTableTypes.contains(AttributeTempTableType.Entity)
    val workspaceTemp = tempTableTypes.contains(AttributeTempTableType.Workspace)

    val callerActionWithTempTables = for {
      _ <- if (entityTemp) dropEntityAttributeTempTable else DBIO.successful()
      _ <- if (workspaceTemp) dropWorkspaceAttributeTempTable else DBIO.successful()
      _ <- if (entityTemp) createEntityAttributeTempTable else DBIO.successful()
      _ <- if (workspaceTemp) createWorkspaceAttributeTempTable else DBIO.successful()
      origResult <- callerAction.asTry
      _ <- if (entityTemp) dropEntityAttributeTempTable else DBIO.successful()
      _ <- if (workspaceTemp) dropWorkspaceAttributeTempTable else DBIO.successful()
    } yield origResult match {
      case scala.util.Success(s)  => s
      case scala.util.Failure(ex) => throw ex
    }

    database.run(callerActionWithTempTables.withPinnedSession).recover { case t: Throwable =>
      logger.error(
        s"Transaction with temporary tables failed for (${tempTableTypes.mkString(",")}). Message: ${t.getMessage}"
      )
      throw t
    }
  }

  def initWithLiquibase(liquibaseChangeLog: String, parameters: Map[String, AnyRef]) = {
    val dbConnection = database.source.createConnection()
    try {
      val liquibaseConnection = new JdbcConnection(dbConnection)
      val resourceAccessor: ResourceAccessor = new ClassLoaderResourceAccessor()
      val liquibase = new Liquibase(liquibaseChangeLog, resourceAccessor, liquibaseConnection)

      parameters.map { case (key, value) => liquibase.setChangeLogParameter(key, value) }
      liquibase.update(new Contexts())

    } catch {
      case e: SQLTimeoutException =>
        val isCertProblem = Throwables.getRootCause(e).isInstanceOf[SunCertPathBuilderException]
        if (isCertProblem) {
          val k = "javax.net.ssl.keyStore"
          if (System.getProperty(k) == null) {
            logger.warn("************")
            logger.warn(
              s"The system property '${k}' is null. This is likely the cause of the database"
                + " connection failure."
            )
            logger.warn("************")
          }
        }
        throw e
    } finally
      dbConnection.close()
  }
}
