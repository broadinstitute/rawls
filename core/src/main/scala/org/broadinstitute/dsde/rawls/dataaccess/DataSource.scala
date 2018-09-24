package org.broadinstitute.dsde.rawls.dataaccess

import java.sql.SQLTimeoutException

import _root_.slick.basic.DatabaseConfig
import _root_.slick.jdbc.{JdbcProfile, TransactionIsolation}
import com.google.common.base.Throwables
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, DataAccessComponent, ReadWriteAction}
import sun.security.provider.certpath.SunCertPathBuilderException
import scala.concurrent.{ExecutionContext, Future}
import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.{ClassLoaderResourceAccessor, ResourceAccessor}
import org.broadinstitute.dsde.rawls.dataaccess.jndi.DirectoryConfig

object DataSource {
  def apply(databaseConfig: DatabaseConfig[JdbcProfile], directoryConfig: DirectoryConfig)(implicit executionContext: ExecutionContext): SlickDataSource = {
    new SlickDataSource(databaseConfig, directoryConfig)
  }
}

class SlickDataSource(val databaseConfig: DatabaseConfig[JdbcProfile], directoryConfig: DirectoryConfig)(implicit executionContext: ExecutionContext) extends LazyLogging {
  val dataAccess = new DataAccessComponent(databaseConfig.profile, databaseConfig.config.getInt("batchSize"), directoryConfig)

  val database = databaseConfig.db

  import dataAccess.driver.api._

  def inTransaction[T](f: (DataAccess) => ReadWriteAction[T], isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead): Future[T] = {
    database.run(f(dataAccess).transactionally.withTransactionIsolation(isolationLevel))
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
    } finally {
      dbConnection.close()
    }
  }
 }
