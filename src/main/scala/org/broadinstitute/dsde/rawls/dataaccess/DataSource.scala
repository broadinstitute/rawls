package org.broadinstitute.dsde.rawls.dataaccess

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.tinkerpop.blueprints.Element
import com.tinkerpop.blueprints.impls.orient.OrientElement
import com.orientechnologies.orient.core.exception.OConcurrentModificationException
import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph.THREAD_MODE
import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientGraphFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

object DataSource {
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
      }
      result
    }, { throwable =>
      graph.makeActive()
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

  /**
   * We have to use JVM locks because Orient refuses to lock remote databases
   */
  var elemLocks = TrieMap[Element, ReentrantReadWriteLock]()

  private def getElemLock(elem: Element): ReentrantReadWriteLock = {
    elemLocks.getOrElseUpdate(elem, new ReentrantReadWriteLock())
  }

  def withWriteLock[T](elem: Element) (op: => T): T = {
    val wl = getElemLock(elem).writeLock()
    try {
      wl.lock()
      op
    } finally {
      wl.unlock()
    }
  }

  def withReadLock[T](elem: Element) (op: => T): T = {
    val rl = getElemLock(elem).readLock()
    try {
      rl.lock()
      op
    } finally {
      rl.unlock()
    }
  }

  def withLock[T](elem: Element, writeLock: Boolean)(op: => T): T = {
    if( writeLock ) {
      withWriteLock(elem)(op)
    } else {
      withReadLock(elem)(op)
    }
  }

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
