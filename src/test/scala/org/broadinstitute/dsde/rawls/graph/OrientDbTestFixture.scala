package org.broadinstitute.dsde.rawls.graph

import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.{LogManager, Logger}

import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, RawlsTransaction}
import org.scalatest.BeforeAndAfterAll

trait OrientDbTestFixture extends BeforeAndAfterAll {
  this : org.scalatest.BeforeAndAfterAll with org.scalatest.Suite =>

  val testDbName: String
  lazy val dataSource = DataSource("memory:"+testDbName, "admin", "admin")
  lazy val graph = new OrientGraph("memory:"+testDbName)
  lazy val txn: RawlsTransaction = new RawlsTransaction(graph, dataSource)

  override def beforeAll {
    // TODO find a better way to set the log level. Nothing else seems to work.
    LogManager.getLogManager().reset()
    Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME).setLevel(java.util.logging.Level.SEVERE)
    graph.begin()
  }

  override def afterAll: Unit = {
    graph.rollback()
    graph.drop()
    graph.shutdown()
  }
}
