package org.broadinstitute.dsde.rawls.dataaccess

import java.sql.SQLTimeoutException
import _root_.slick.basic.DatabaseConfig
import _root_.slick.jdbc.{JdbcProfile, TransactionIsolation}
import com.google.common.base.Throwables
import com.typesafe.config.ConfigValueFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, DataAccessComponent, ReadWriteAction}
import sun.security.provider.certpath.SunCertPathBuilderException

import scala.concurrent.{ExecutionContext, Future}
import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.{ClassLoaderResourceAccessor, ResourceAccessor}

object DataSource {
  def apply(databaseConfig: DatabaseConfig[JdbcProfile])(implicit executionContext: ExecutionContext): SlickDataSource = {
    new SlickDataSource(databaseConfig)
  }
}

class SlickDataSource(val initialDatabaseConfig: DatabaseConfig[JdbcProfile])(implicit executionContext: ExecutionContext) extends LazyLogging {
  val dataAccess = new DataAccessComponent(initialDatabaseConfig.profile, initialDatabaseConfig.config.getInt("batchSize"))
  val databaseConfig = DatabaseConfig.forConfig[JdbcProfile]("", initialDatabaseConfig.config.withValue("db.connectionInitSql", ConfigValueFactory.fromAnyRef("call createAttributeTempTables()")))

  val database = databaseConfig.db

  import dataAccess.driver.api._

  def inTransaction[T](f: (DataAccess) => ReadWriteAction[T], isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead): Future[T] = {
    database.run(f(dataAccess).transactionally.withTransactionIsolation(isolationLevel))
  }

  def initWithLiquibase(liquibaseChangeLog: String, parameters: Map[String, AnyRef]) = {
    // use a database specified with the initialDatabaseConfig because the regular databaseConfig assumes
    // a procedure called createTempTables exists but it is liquibase that creates that procedure
    // need to create a new config because each config instance has its own db and closing it is a problem if it is shared
    val initDatabase = DatabaseConfig.forConfig[JdbcProfile]("", initialDatabaseConfig.config).db
    try {
      val dbConnection = initDatabase.source.createConnection()

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
      initDatabase.close()
    }
  }
 }
