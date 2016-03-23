package org.broadinstitute.dsde.rawls.dataaccess

import java.util.concurrent.{Executors, ExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.util.concurrent.locks.{Lock, StampedLock, ReadWriteLock, ReentrantReadWriteLock}

import _root_.slick.backend.DatabaseConfig
import _root_.slick.driver.JdbcProfile
import com.orientechnologies.orient.core.exception.OConcurrentModificationException
import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph.THREAD_MODE
import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientGraphFactory}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, DataAccess, DataAccessComponent}
import org.broadinstitute.dsde.rawls.model.WorkspaceName

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

import org.broadinstitute.dsde.rawls.util.ScalaConfig._

import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.{ClassLoaderResourceAccessor, ResourceAccessor}

object DataSource {
  /**
   * We have to use JVM locks because Orient refuses to lock remote databases
   */
  var wsLocks = TrieMap[WorkspaceName, StampedLock]()

  private def getLock(ws: WorkspaceName): StampedLock = {
    wsLocks.getOrElseUpdate(ws, new StampedLock())
  }

  def apply(url: String, user: String, password: String, minPoolSize: Int, maxPoolSize: Int)(implicit executionContext: ExecutionContext) = {
    val factory: OrientGraphFactory = createFactory(url, user, password)
// db pooling disabled: https://broadinstitute.atlassian.net/browse/DSDEEPB-1589
//    factory.setupPool(minPoolSize, maxPoolSize)
    new DataSource(factory)
  }

  def apply(url: String, user: String, password: String)(implicit executionContext: ExecutionContext) = {
    val factory: OrientGraphFactory = createFactory(url, user, password)
    new DataSource(factory)
  }

  def apply(databaseConfig: DatabaseConfig[JdbcProfile])(implicit executionContext: ExecutionContext): SlickDataSource = {
    new SlickDataSource(databaseConfig)
  }

  def createFactory(url: String, user: String, password: String): OrientGraphFactory = {
    val factory = new OrientGraphFactory(url, user, password)
    factory.setThreadMode(THREAD_MODE.MANUAL)
    factory.setAutoStartTx(false)
    factory.setUseClassForEdgeLabel(false)
    factory.setUseLightweightEdges(true)
    factory
  }
}


class SlickDataSource(val databaseConfig: DatabaseConfig[JdbcProfile])(implicit executionContext: ExecutionContext) {
  private val database = databaseConfig.db

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

  val dataAccess = new DataAccessComponent(databaseConfig.driver)
  import dataAccess.driver.api._

  def inTransaction[T](f: (DataAccess) => ReadWriteAction[T]): Future[T] = {
    //database.run(f(dataAccess).transactionally) <-- https://github.com/slick/slick/issues/1274
    Future(Await.result(database.run(f(dataAccess).transactionally), Duration.Inf))(actionExecutionContext)
  }

  def initWithLiquibase(liquibaseChangeLog: String) = {
    val dbConnection = database.source.createConnection()
    val liquibaseConnection = new JdbcConnection(dbConnection)

    try {
      val resourceAccessor: ResourceAccessor = new ClassLoaderResourceAccessor()
      val liquibase = new Liquibase(liquibaseChangeLog, resourceAccessor, liquibaseConnection)
      liquibase.update(new Contexts())
    } finally {
      liquibaseConnection.close()
      dbConnection.close()
    }
  }

  // For testing/migration.  Not for production code!
  def initWithSlick(): Unit = {
    import dataAccess.driver.api._
    import scala.concurrent.duration._
    Await.result(database.run(DBIO.seq(dataAccess.allSchemas.create)), 1.minute)
  }
}

class DataSource(graphFactory: OrientGraphFactory)(implicit executionContext: ExecutionContext) extends LazyLogging {

  private def lockTxn(txn: RawlsTransaction): Unit = {
    txn.readLocks.foreach { readWs =>
      if (DataSource.getLock(readWs).tryReadLock(5, TimeUnit.SECONDS) == 0) {
        throw new RawlsException(s"Could not acquire read lock for $readWs, please try again.")
      }
    }
    txn.writeLocks.foreach { writeWs =>
      if (DataSource.getLock(writeWs).tryWriteLock(30, TimeUnit.SECONDS) == 0) {
        throw new RawlsException(s"Could not acquire write lock for $writeWs, please try again.")
      }
    }
  }

  private def unlockTxn(txn: RawlsTransaction): Unit = {
    txn.writeLocks.foreach { writeWs =>
      DataSource.getLock(writeWs).tryUnlockWrite()
    }
    txn.readLocks.foreach { readWs =>
      DataSource.getLock(readWs).tryUnlockRead()
    }
  }

  def inTransaction[T](readLocks: Set[WorkspaceName] = Set.empty[WorkspaceName],
                       writeLocks: Set[WorkspaceName] = Set.empty[WorkspaceName])(f: RawlsTransaction => T): T = {
    val graph = graphFactory.getTx
    graph.begin()
    val txn: RawlsTransaction = new RawlsTransaction(graph, this, readLocks -- writeLocks, writeLocks )
    try {
      lockTxn(txn)
      val result = f(txn)
      completeTransaction(txn)
      result
    } catch {
      case t: Throwable =>
        graph.rollback()
        throw t
    } finally {
      graph.shutdown()
      unlockTxn(txn)
    }
  }

  /**
   * This function completes or rolls back the transaction in a future at the end of the future resulting from f
   * @param f
   * @tparam T
   */
  def inFutureTransaction[T](readLocks: Set[WorkspaceName] = Set.empty[WorkspaceName],
                             writeLocks: Set[WorkspaceName] = Set.empty[WorkspaceName])(f: RawlsTransaction => Future[T]): Future[T] = {
    val graph = graphFactory.getTx
    graph.begin()
    val txn: RawlsTransaction = new RawlsTransaction(graph, this, readLocks -- writeLocks, writeLocks)
    lockTxn(txn)
    val resultFuture = f(txn)

    resultFuture.transform( { result =>
      graph.makeActive()
      try {
        completeTransaction(txn)
      } catch {
        case t: OConcurrentModificationException =>
          import scala.collection.JavaConversions._
          val v = graph.getVertex(t.getRid)
          val props = v.getPropertyKeys.map(k => k -> v.getProperty(k)).toMap
          throw new RawlsException(s"concurrent modification exception modifying ${props}", t)
        case t: Throwable =>
          throw t
      } finally {
        unlockTxn(txn)
      }
      result
    }, { throwable =>
      graph.makeActive()
      completeTransactionOnException(graph, txn)
      throwable
    })
  }

  def completeTransaction[T](rawlsTransaction: RawlsTransaction): Unit = {
    try {
      if (rawlsTransaction.isRollbackOnly) {
        logger.debug("rolling back transaction marked as rollback only")
        rawlsTransaction.graph.rollback()
      } else {
        rawlsTransaction.graph.commit()
      }
    } finally {
      rawlsTransaction.graph.shutdown()
    }
  }

  def completeTransactionOnException(graph: OrientGraph, rawlsTransaction: RawlsTransaction): Unit = {
    try {
      graph.rollback()
    } finally {
      graph.shutdown()
      unlockTxn(rawlsTransaction)
    }
  }

  def shutdown() = graphFactory.close()
}

class RawlsTransaction(val graph: OrientGraph,
                       dataSource: DataSource,
                       val readLocks: Set[WorkspaceName],
                       val writeLocks: Set[WorkspaceName])(implicit executionContext: ExecutionContext) {
  private val rollbackOnly = new AtomicBoolean(false)

  def withGraph[T](f: Graph => T) = {
    // because transactions are spanning threads we need to make sure the graph is active
    graph.makeActive()
    f(graph)
  }

  /**
   * Allows code running with a connection to rollback the transaction without throwing an exception.
   * All following modifications will be rolled back as well.
   */
  def setRollbackOnly(): Unit = {
    rollbackOnly.set(true)
  }

  def isRollbackOnly = rollbackOnly.get()
}
