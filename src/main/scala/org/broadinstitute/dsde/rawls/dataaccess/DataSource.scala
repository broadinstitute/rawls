package org.broadinstitute.dsde.rawls.dataaccess

import java.sql.SQLTimeoutException
import java.util.concurrent.{Executors, ExecutorService}

import _root_.slick.backend.DatabaseConfig
import _root_.slick.driver.JdbcDriver
import _root_.slick.jdbc.meta.MTable
import com.google.common.base.Throwables
import com.typesafe.config.ConfigValueFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, DataAccess, DataAccessComponent}
import sun.security.provider.certpath.SunCertPathBuilderException

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

import org.broadinstitute.dsde.rawls.util.ScalaConfig._

import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.{ClassLoaderResourceAccessor, ResourceAccessor}

object DataSource {
  def apply(databaseConfig: DatabaseConfig[JdbcDriver])(implicit executionContext: ExecutionContext): SlickDataSource = {
    new SlickDataSource(databaseConfig)
  }
}

class SlickDataSource(initialDatabaseConfig: DatabaseConfig[JdbcDriver])(implicit executionContext: ExecutionContext) extends LazyLogging {
  val dataAccess = new DataAccessComponent(initialDatabaseConfig.driver, initialDatabaseConfig.config.getInt("batchSize"))
  val databaseConfig = DatabaseConfig.forConfig[JdbcDriver]("", initialDatabaseConfig.config)

  val database = databaseConfig.db


  /**
   * Create a special execution context, a fixed thread pool, to run each of our composite database actions. Running
   * each composite action as a runnable within the pool will ensure that-- at most-- the same number of actions are
   * running as there are available connections. Thus there should never be a connection deadlock, as outlined in
   * - https://github.com/slick/slick/issues/1274
   * - https://groups.google.com/d/msg/scalaquery/5MCUnwaJ7U0/NLLMotX9BQAJ
   *
   * Custom future thread pool based on:
   * - http://stackoverflow.com/questions/15285284/how-to-configure-a-fine-tuned-thread-pool-for-futures#comment23278672_15285441
   *
   * Database config parameter defaults based on: (expand the `forConfig` scaladoc for a full list of values)
   * - http://slick.typesafe.com/doc/3.1.0/api/index.html#slick.jdbc.JdbcBackend$DatabaseFactoryDef@forConfig(path:String,config:com.typesafe.config.Config,driver:java.sql.Driver,classLoader:ClassLoader):JdbcBackend.this.Database
   *
   * Reuses the error reporter from the database's executionContext.
   */
  private val actionThreadPool: ExecutorService = {
    val dbNumThreads = databaseConfig.config.getIntOr("db.numThreads", 20)
    val dbMaximumPoolSize = databaseConfig.config.getIntOr("db.maxConnections", dbNumThreads * 5)
    val actionThreadPoolSize = databaseConfig.config.getIntOr("actionThreadPoolSize", dbNumThreads) min dbMaximumPoolSize
    Executors.newFixedThreadPool(actionThreadPoolSize)
  }
  private val actionExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(
    actionThreadPool, database.executor.executionContext.reportFailure)

  import dataAccess.driver.api._

  def inTransaction[T](f: (DataAccess) => ReadWriteAction[T]): Future[T] = {
    //database.run(f(dataAccess).transactionally) <-- https://github.com/slick/slick/issues/1274
    Future(Await.result(database.run(f(dataAccess).transactionally), Duration.Inf))(actionExecutionContext)
  }

  def initWithLiquibase(liquibaseChangeLog: String) = {
    // use a database specified with the initialDatabaseConfig because the regular databaseConfig assumes
    // a procedure called createTempTables exists but it is liquibase that creates that procedure
    // need to create a new config because each config instance has its own db and closing it is a problem if it is shared
    val initDatabase = DatabaseConfig.forConfig[JdbcDriver]("", initialDatabaseConfig.config).db
    try {
      val dbConnection = initDatabase.source.createConnection()

      val liquibaseConnection = new JdbcConnection(dbConnection)
      val resourceAccessor: ResourceAccessor = new ClassLoaderResourceAccessor()
      val liquibase = new Liquibase(liquibaseChangeLog, resourceAccessor, liquibaseConnection)
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
