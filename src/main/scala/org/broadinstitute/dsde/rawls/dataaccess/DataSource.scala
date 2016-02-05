package org.broadinstitute.dsde.rawls.dataaccess

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.util.concurrent.locks.{Lock, StampedLock, ReadWriteLock, ReentrantReadWriteLock}

import com.tinkerpop.blueprints.Element
import com.tinkerpop.blueprints.impls.orient.OrientElement
import com.orientechnologies.orient.core.exception.OConcurrentModificationException
import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph.THREAD_MODE
import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientGraphFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.WorkspaceName

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

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

  def createFactory(url: String, user: String, password: String): OrientGraphFactory = {
    val factory = new OrientGraphFactory(url, user, password)
    factory.setThreadMode(THREAD_MODE.MANUAL)
    factory.setAutoStartTx(false)
    factory.setUseClassForEdgeLabel(false)
    factory.setUseLightweightEdges(true)
    factory
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
