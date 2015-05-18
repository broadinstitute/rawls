package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph.THREAD_MODE
import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientGraphFactory}

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
    factory
  }
}

class DataSource(graphFactory: OrientGraphFactory) {
  def inTransaction[T](f: RawlsTransaction => T): T = {
    val graph = graphFactory.getTx
    try {
      graph.begin()
      val result = f(new RawlsTransaction(graph))
      graph.commit()
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

class RawlsTransaction(graph: OrientGraph) {
  def withGraph[T](f: Graph => T) = f(graph)

  /**
   * Allows code running with a connection to rollback the transaction without throwing an exception.
   * Following use of the connection will probably fail.
   */
  def rollback(): Unit = {
    graph.rollback()
  }
}
