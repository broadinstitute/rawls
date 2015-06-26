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

  /**
   * Creates a small default graph. Note that these variables will be inaccessible from outside tests, so you need to remember the entity names, etc.
   */
  def initializeTestGraph(tx:RawlsTransaction = txn): Unit = {
    val workspace = new Workspace("workspaces", "test_workspace", DateTime.now, "testUser", new HashMap[String, Attribute]() )

    val sample1 = new Entity("sample1", "Sample", Map( "type" -> AttributeString("normal") ), WorkspaceName("workspaces", "test_workspace") )
    val sample2 = new Entity("sample2", "Sample", Map( "type" -> AttributeString("tumor") ), WorkspaceName("workspaces", "test_workspace") )
    val sample3 = new Entity("sample3", "Sample", Map( "type" -> AttributeString("tumor") ), WorkspaceName("workspaces", "test_workspace") )

    val pair1 = new Entity("pair1", "Pair",
      Map( "case" -> AttributeReferenceSingle("Sample", "sample2"),
        "control" -> AttributeReferenceSingle("Sample", "sample1") ),
      WorkspaceName("workspaces", "test_workspace") )
    val pair2 = new Entity("pair2", "Pair",
      Map( "case" -> AttributeReferenceSingle("Sample", "sample3"),
        "control" -> AttributeReferenceSingle("Sample", "sample1") ),
      WorkspaceName("workspaces", "test_workspace") )

    val sset1 = new Entity("sset1", "SampleSet",
      Map( "samples" -> AttributeReferenceList( List(AttributeReferenceSingle("Sample", "sample1"),
        AttributeReferenceSingle("Sample", "sample2"),
        AttributeReferenceSingle("Sample", "sample3"))) ),
      WorkspaceName("workspaces", "test_workspace") )
    val sset2 = new Entity("sset2", "SampleSet",
      Map( "samples" -> AttributeReferenceList( List(AttributeReferenceSingle("Sample", "sample2"))) ),
      WorkspaceName("workspaces", "test_workspace") )

    val ps1 = new Entity("ps1", "PairSet",
      Map( "pairs" -> AttributeReferenceList( List(AttributeReferenceSingle("Pair", "pair1"),
        AttributeReferenceSingle("Pair", "pair2"))) ),
      WorkspaceName("workspaces", "test_workspace") )

    workspaceDAO.save(workspace, tx)
    entityDAO.save("workspaces", "test_workspace", sample1, tx)
    entityDAO.save("workspaces", "test_workspace", sample2, tx)
    entityDAO.save("workspaces", "test_workspace", sample3, tx)
    entityDAO.save("workspaces", "test_workspace", pair1, tx)
    entityDAO.save("workspaces", "test_workspace", pair2, tx)
    entityDAO.save("workspaces", "test_workspace", sset1, tx)
    entityDAO.save("workspaces", "test_workspace", sset2, tx)
    entityDAO.save("workspaces", "test_workspace", ps1, tx)
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
