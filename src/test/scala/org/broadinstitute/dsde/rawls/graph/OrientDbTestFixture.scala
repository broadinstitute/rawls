package org.broadinstitute.dsde.rawls.graph

import java.util.logging.{LogManager, Logger}

import com.tinkerpop.blueprints.impls.orient.{OrientBaseGraph, OrientGraph}
import org.scalatest.BeforeAndAfterAll

trait OrientDbTestFixture extends BeforeAndAfterAll {
  this : org.scalatest.BeforeAndAfterAll with org.scalatest.Suite =>

  var graph: OrientBaseGraph = _
  val testDbName: String

  override def beforeAll {
    // TODO find a better way to set the log level. Nothing else seems to work.
    LogManager.getLogManager().reset()
    Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME).setLevel(java.util.logging.Level.SEVERE)
    graph = new OrientGraph("memory:"+testDbName)
  }

  override def afterAll {
    graph.drop
    graph.shutdown
  }
}
