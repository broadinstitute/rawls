package org.broadinstitute.dsde.rawls.graph

import java.util.logging.{LogManager, Logger}

import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable.HashMap

trait OrientDbTestFixture extends BeforeAndAfterAll {
  this : org.scalatest.BeforeAndAfterAll with org.scalatest.Suite =>

  val testDbName: String
  lazy val dataSource = DataSource("memory:"+testDbName, "admin", "admin")
  lazy val graph = new OrientGraph("memory:"+testDbName)
  lazy val txn: RawlsTransaction = new RawlsTransaction(graph, dataSource)
  lazy val workspaceDAO: GraphWorkspaceDAO = new GraphWorkspaceDAO()
  lazy val entityDAO: GraphEntityDAO = new GraphEntityDAO()
  lazy val configDAO: GraphMethodConfigurationDAO = new GraphMethodConfigurationDAO()

  val tg_workspace = new Workspace("workspaces", "test_workspace", DateTime.now, "testUser", new HashMap[String, Attribute]() )

  val tg_sample1 = new Entity("sample1", "Sample", Map( "type" -> AttributeString("normal") ), WorkspaceName("workspaces", "test_workspace") )
  val tg_sample2 = new Entity("sample2", "Sample", Map( "type" -> AttributeString("tumor") ), WorkspaceName("workspaces", "test_workspace") )
  val tg_sample3 = new Entity("sample3", "Sample", Map( "type" -> AttributeString("tumor") ), WorkspaceName("workspaces", "test_workspace") )

  val tg_pair1 = new Entity("pair1", "Pair",
    Map( "case" -> AttributeReferenceSingle("Sample", "sample2"),
      "control" -> AttributeReferenceSingle("Sample", "sample1") ),
    WorkspaceName("workspaces", "test_workspace") )
  val tg_pair2 = new Entity("pair2", "Pair",
    Map( "case" -> AttributeReferenceSingle("Sample", "sample3"),
      "control" -> AttributeReferenceSingle("Sample", "sample1") ),
    WorkspaceName("workspaces", "test_workspace") )

  val tg_sset1 = new Entity("sset1", "SampleSet",
    Map( "samples" -> AttributeReferenceList( List(AttributeReferenceSingle("Sample", "sample1"),
      AttributeReferenceSingle("Sample", "sample2"),
      AttributeReferenceSingle("Sample", "sample3"))) ),
    WorkspaceName("workspaces", "test_workspace") )
  val tg_sset2 = new Entity("sset2", "SampleSet",
    Map( "samples" -> AttributeReferenceList( List(AttributeReferenceSingle("Sample", "sample2"))) ),
    WorkspaceName("workspaces", "test_workspace") )

  val tg_ps1 = new Entity("ps1", "PairSet",
    Map( "pairs" -> AttributeReferenceList( List(AttributeReferenceSingle("Pair", "pair1"),
      AttributeReferenceSingle("Pair", "pair2"))) ),
    WorkspaceName("workspaces", "test_workspace") )

  val tg_indiv1 = new Entity("indiv1", "Individual",
    Map( "sset" -> AttributeReferenceSingle("SampleSet", "sset1") ),
    WorkspaceName("workspaces", "test_workspace") )

  def initializeTestGraph(tx:RawlsTransaction = txn): Unit = {
    workspaceDAO.save(tg_workspace, tx)
    entityDAO.save("workspaces", "test_workspace", tg_sample1, tx)
    entityDAO.save("workspaces", "test_workspace", tg_sample2, tx)
    entityDAO.save("workspaces", "test_workspace", tg_sample3, tx)
    entityDAO.save("workspaces", "test_workspace", tg_pair1, tx)
    entityDAO.save("workspaces", "test_workspace", tg_pair2, tx)
    entityDAO.save("workspaces", "test_workspace", tg_sset1, tx)
    entityDAO.save("workspaces", "test_workspace", tg_sset2, tx)
    entityDAO.save("workspaces", "test_workspace", tg_ps1, tx)
    entityDAO.save("workspaces", "test_workspace", tg_indiv1, tx)
  }

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
