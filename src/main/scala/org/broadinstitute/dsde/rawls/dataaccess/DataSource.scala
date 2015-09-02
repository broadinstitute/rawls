package org.broadinstitute.dsde.rawls.dataaccess

import java.util.concurrent.atomic.AtomicBoolean

import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph.THREAD_MODE
import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientGraphFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.util.{Failure, Success, Try}

object DataSource {
  def apply(url: String, user: String, password: String, minPoolSize: Int, maxPoolSize: Int) = {
    val factory: OrientGraphFactory = createFactory(url, user, password)
    factory.setupPool(minPoolSize, maxPoolSize)
    new DataSource(factory)
  }

  def apply(url: String, user: String, password: String, lwEdges:Boolean=true) = {
    val factory: OrientGraphFactory = createFactory(url, user, password, lwEdges)
    new DataSource(factory)
  }

  def createFactory(url: String, user: String, password: String, lwEdges:Boolean=true): OrientGraphFactory = {
    val factory = new OrientGraphFactory(url, user, password)
    factory.setThreadMode(THREAD_MODE.MANUAL)
    factory.setAutoStartTx(false)
    factory.setUseClassForEdgeLabel(false)
    factory.setUseLightweightEdges(lwEdges)
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
  def withGraph[T](f: Graph => T) = f(graph)

  /**
   * Allows code running with a connection to rollback the transaction without throwing an exception.
   * All following modifications will be rolled back as well.
   */
  def setRollbackOnly(): Unit = {
    dataSource.rollbackOnly.set(true)
  }
}
