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

object DataSource {
  def apply(databaseConfig: DatabaseConfig[JdbcProfile])(implicit executionContext: ExecutionContext): SlickDataSource = {
    new SlickDataSource(databaseConfig)
  }
}

class SlickDataSource(val databaseConfig: DatabaseConfig[JdbcProfile])(implicit executionContext: ExecutionContext) extends LazyLogging {
  val dataAccess = new DataAccessComponent(databaseConfig.profile, databaseConfig.config.getInt("batchSize"))

  val database = databaseConfig.db

  import dataAccess.driver.api._

  def inTransaction[T](f: (DataAccess) => ReadWriteAction[T], isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead): Future[T] = {
     database.run(f(dataAccess).transactionally.withTransactionIsolation(isolationLevel))
  }

  // creates the ENTITY_ATTRIBUTE_TEMP for use by this transaction, executes the transaction, drops the temp table
  def inTransactionWithAttrTempTable[T](f: (DataAccess) => ReadWriteAction[T], isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead): Future[T] = {

    def createTempTable = {
      // TODO: this could move to a stored procedure so we are not defining schema here inside this class
      sql"""create temporary table ENTITY_ATTRIBUTE_TEMP (
           id bigint(20) unsigned NOT NULL AUTO_INCREMENT primary key,
           namespace text NOT NULL,
           name text NOT NULL,
           value_string text,
           value_json longtext,
           value_number double DEFAULT NULL,
           value_boolean bit(1) DEFAULT NULL,
           value_entity_ref bigint(20) unsigned DEFAULT NULL,
           list_index int(11) DEFAULT NULL,
           list_length int(11) DEFAULT NULL,
           owner_id bigint(20) unsigned NOT NULL,
           deleted bit(1) DEFAULT false,
           deleted_date timestamp NULL DEFAULT NULL,
           transaction_id CHAR(36) NOT NULL);""".as[Boolean]
    }

    def dropTempTable = {
      sql"""drop temporary table if exists ENTITY_ATTRIBUTE_TEMP;""".as[Boolean]
    }

    val callerAction = f(dataAccess).transactionally.withTransactionIsolation(isolationLevel)

    // TODO: does this need more complex error-handling?
    val callerActionWithTempTables = (for {
      _ <- dropTempTable
      _ <- createTempTable
      origResult <- callerAction
    } yield {
      origResult
    }).andFinally(dropTempTable)

    database.run(callerActionWithTempTables.withPinnedSession)

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
