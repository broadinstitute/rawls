package org.broadinstitute.dsde.rawls.graph

import java.util.logging.{Logger, LogManager}

import com.tinkerpop.blueprints.impls.orient.{OrientBaseGraph, OrientGraph}
import org.scalatest.{SuiteMixin, FunSuite, BeforeAndAfter}

trait OrientDbTestFixture extends BeforeAndAfter {
  this : org.scalatest.BeforeAndAfter with org.scalatest.Suite =>

  var graph: OrientBaseGraph = _
  val testDbName: String

  def initializeGraph

  before {
    // TODO find a better way to set the log level. Nothing else seems to work.
    LogManager.getLogManager().reset()
    Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME).setLevel(java.util.logging.Level.SEVERE)
    graph = new OrientGraph("memory:"+testDbName)
    graph.setUseLightweightEdges(true) // improves performance, but can't put properties on edges
    initializeGraph
  }

  after {
    graph.drop
    graph.shutdown
  }
}
