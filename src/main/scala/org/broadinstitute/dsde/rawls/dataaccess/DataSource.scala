package org.broadinstitute.dsde.rawls.dataaccess

import java.util.concurrent.atomic.AtomicBoolean

import com.tinkerpop.blueprints.{Element, Graph}
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph.THREAD_MODE
import com.tinkerpop.blueprints.impls.orient.{OrientElement, OrientGraph, OrientGraphFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.mutable

import scala.util.{Failure, Success, Try}

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
  val rollbackOnly = new AtomicBoolean(false)

  def inTransaction[T](f: RawlsTransaction => T): T = {
    val graph = graphFactory.getTx
    try {
      rollbackOnly.set(false)
      graph.begin()
      val result = f(new RawlsTransaction(graph, this))
      if (rollbackOnly.get) {
        logger.debug("rolling back transaction marked as rollback only")
        graph.rollback()
      } else {
        graph.commit()
      }
      result
    } catch {
      case t: Throwable =>
        graph.rollback()
        throw t
    } finally {
      graph.shutdown()
    }
  }

  def shutdown() = graphFactory.close()
}

class RawlsTransaction(graph: OrientGraph, dataSource: DataSource) {

  var readLocks:mutable.Map[Element, Int] = mutable.Map[Element, Int]().withDefaultValue(0)
  var writeLocks:mutable.Map[Element, Int] = mutable.Map[Element, Int]().withDefaultValue(0)

  /**
   * Lock handling. Ensures that we don't try to get multiple locks on this elem within the same txn.
   */
  def acquireReadLock(elem: Element) = {
    assert( writeLocks(elem) == 0, s"Attempting to acquire read lock on $elem which already has a write lock" )
    if( readLocks(elem) == 0 ) {
      elem.asInstanceOf[OrientElement].lock(false)
    }
    readLocks(elem) += 1
  }

  def releaseReadLock(elem: Element) = {
    assert(readLocks(elem) > 0, s"Attempting to release nonexistent read lock on $elem")
    readLocks(elem) -= 1
    if( readLocks(elem) == 0 ) {
      elem.asInstanceOf[OrientElement].unlock()
    }
  }

  def acquireWriteLock(elem: Element) = {
    assert( readLocks(elem) == 0, s"Attempting to acquire write lock on $elem which already has a read lock" )
    if( writeLocks(elem) == 0 ) {
      elem.asInstanceOf[OrientElement].lock(true)
    }
    writeLocks(elem) += 1
  }

  def releaseWriteLock(elem: Element) = {
    assert(writeLocks(elem) > 0, s"Attempting to release nonexistent write lock on $elem")
    writeLocks(elem) -= 1
    if( writeLocks(elem) == 0 ) {
      elem.asInstanceOf[OrientElement].unlock()
    }
  }

  def withWriteLock[T](elem: Element) (op: => T): T = {
    acquireWriteLock(elem)
    val ret = op
    releaseWriteLock(elem)
    ret
  }

  def withReadLock[T](elem: Element) (op: => T): T = {
    acquireReadLock(elem)
    val ret = op
    releaseReadLock(elem)
    ret
  }

  def isReadLocked(elem: Element): Boolean = {
    readLocks(elem) > 0
  }

  def isWriteLocked(elem: Element): Boolean = {
    writeLocks(elem) > 0
  }

  def withGraph[T](f: Graph => T) = f(graph)

  /**
   * Allows code running with a connection to rollback the transaction without throwing an exception.
   * All following modifications will be rolled back as well.
   */
  def setRollbackOnly(): Unit = {
    dataSource.rollbackOnly.set(true)
  }
}
