package org.broadinstitute.dsde.rawls.dataaccess

import java.util.concurrent.atomic.AtomicBoolean

import com.tinkerpop.blueprints.{Element, Graph}
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph.THREAD_MODE
import com.tinkerpop.blueprints.impls.orient.{OrientElement, OrientGraph, OrientGraphFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.concurrent.Future
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}

import scala.concurrent.ExecutionContext.Implicits.global

object DataSource {
  def apply(url: String, user: String, password: String, minPoolSize: Int, maxPoolSize: Int) = {
    val factory: OrientGraphFactory = createFactory(url, user, password)
    factory.setupPool(minPoolSize, maxPoolSize)
    new DataSource(factory)
  }

  def apply(url: String, user: String, password: String) = {
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

class DataSource(graphFactory: OrientGraphFactory) extends LazyLogging {
  def inTransaction[T](f: RawlsTransaction => T): T = {
    val graph = graphFactory.getTx
    try {
      graph.begin()
      val txn: RawlsTransaction = new RawlsTransaction(graph, this)
      val result = f(txn)
      completeTransaction(txn)
      result
    } catch {
      case t: Throwable =>
        graph.rollback()
        throw t
    } finally {
      graph.shutdown()
    }
  }

  /**
   * This function completes or rolls back the transaction in a future at the end of the future resulting from f
   * @param f
   * @tparam T
   */
  def inFutureTransaction[T](f: RawlsTransaction => Future[T]): Future[T] = {
    val graph = graphFactory.getTx
    graph.begin()
    val txn: RawlsTransaction = new RawlsTransaction(graph, this)
    val resultFuture = f(txn)

    resultFuture.transform( { result =>
      completeTransaction(txn)
      result
    }, { throwable =>
      completeTransactionOnException(graph)
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

  def completeTransactionOnException(graph: OrientGraph): Unit = {
    try {
      graph.rollback()
    } finally {
      graph.shutdown()
    }
  }

  def shutdown() = graphFactory.close()
}

class RawlsTransaction(val graph: OrientGraph, dataSource: DataSource) {
  private val rollbackOnly = new AtomicBoolean(false)

  var readLocks = TrieMap[Element, Int]().withDefaultValue(0)
  var writeLocks = TrieMap[Element, Int]().withDefaultValue(0)

  /**
   * Lock handling. Ensures that we don't try to get multiple locks on this elem within the same txn.
   */
  private def acquireReadLock(elem: Element) = {
    if( writeLocks(elem) > 0 ) {
      //Silently "upgrade" read locks to write locks if a write lock exists, since we already
      //have exclusive access.
      acquireWriteLock(elem)
    } else {
      if (readLocks(elem) == 0) {
        elem.asInstanceOf[OrientElement].lock(false)
      }
      readLocks(elem) += 1
    }
  }

  private def releaseReadLock(elem: Element) = {
    if( writeLocks(elem) > 0 ) {
      //Handle our read lock having been silently upgraded.
      releaseWriteLock(elem)
    } else {
      assert(readLocks(elem) > 0, s"Attempting to release nonexistent read lock on $elem")
      readLocks(elem) -= 1
      if (readLocks(elem) == 0) {
        elem.asInstanceOf[OrientElement].unlock()
      }
    }
  }

  private def acquireWriteLock(elem: Element) = {
    assert( readLocks(elem) == 0, s"Attempting to acquire write lock on $elem which already has a read lock" )
    if( writeLocks(elem) == 0 ) {
      elem.asInstanceOf[OrientElement].lock(true)
    }
    writeLocks(elem) += 1
  }

  private def releaseWriteLock(elem: Element) = {
    assert(writeLocks(elem) > 0, s"Attempting to release nonexistent write lock on $elem")
    writeLocks(elem) -= 1
    if( writeLocks(elem) == 0 ) {
      elem.asInstanceOf[OrientElement].unlock()
    }
  }

  def withWriteLock[T](elem: Element) (op: => T): T = {
    try {
      acquireWriteLock(elem)
      op
    } finally {
      releaseWriteLock(elem)
    }
  }

  def withReadLock[T](elem: Element) (op: => T): T = {
    try {
      acquireReadLock(elem)
      op
    } finally {
      releaseReadLock(elem)
    }
  }

  def withLock[T](elem: Element, writeLock: Boolean)(op: => T): T = {
    if( writeLock ) {
      withWriteLock(elem)(op)
    } else {
      withReadLock(elem)(op)
    }
  }

  def isReadLocked(elem: Element): Boolean = { readLocks(elem) > 0 }
  def isWriteLocked(elem: Element): Boolean = { writeLocks(elem) > 0 }

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
