package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.Direction
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

class GraphEntityDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {
  lazy val dao: GraphEntityDAO = new GraphEntityDAO()


  "GraphEntityDAO" should "get entity types" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Set("PairSet", "Individual", "Sample", "Aliquot", "SampleSet", "Pair")) {
        dao.getEntityTypes(testData.workspace.namespace, testData.workspace.name, txn).toSet
      }
    }
  }

  it should "list all entities of all entity types" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Set(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7, testData.sset1, testData.sset2, testData.sset3, testData.sset4, /*testData.sset_empty,*/ testData.aliquot1, testData.aliquot2, testData.pair1, testData.pair2, testData.ps1, testData.indiv1, testData.aliquot1, testData.aliquot2)) {
        dao.listEntitiesAllTypes(testData.workspace.namespace, testData.workspace.name, txn).toSet
      }
    }
  }

  class BugTestData() extends TestData {
    val wsName = WorkspaceName("myNamespace2", "myWorkspace2")
    val workspace = new Workspace(wsName.namespace, wsName.name, DateTime.now, "testUser", Map.empty )

    val sample1 = new Entity("sample1", "Sample",
      Map(
        "aliquot" -> AttributeReferenceSingle("Aliquot", "aliquot1")),
      wsName )

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty, wsName)

    override def save(txn:RawlsTransaction): Unit = {
      workspaceDAO.save(workspace, txn)
      entityDAO.save(workspace.namespace, workspace.name, aliquot1, txn)
      entityDAO.save(workspace.namespace, workspace.name, sample1, txn)
    }
  }
  val bugData = new BugTestData

  // TODO re-enable this when case sensitivity is fixed
  ignore should "get an entity with attribute ref name same as an entity, but different case" in withCustomTestDatabase(new BugTestData) { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Some(bugData.sample1)) {
        dao.get(bugData.workspace.namespace, bugData.workspace.name, "Sample", "sample1", txn)
      }
    }
  }

  it should "get an entity" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Some(testData.pair1)) {
        dao.get(testData.workspace.namespace, testData.workspace.name, "Pair", "pair1", txn)
      }
      assertResult(Some(testData.sample1)) {
        dao.get(testData.workspace.namespace, testData.workspace.name, "Sample", "sample1", txn)
      }
      assertResult(Some(testData.sset1)) {
        dao.get(testData.workspace.namespace, testData.workspace.name, "SampleSet", "sset1", txn)
      }
    }
  }

  it should "return None when an entity does not exist" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(None) {
        dao.get(testData.workspace.namespace, testData.workspace.name, "pair", "fnord", txn)
      }
      assertResult(None) {
        dao.get(testData.workspace.namespace, testData.workspace.name, "fnord", "pair1", txn)
      }
    }
  }

  it should "return None when a parent workspace does not exist" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(None) {
        dao.get(testData.workspace.namespace, "fnord", "pair", "pair1", txn)
      }
    }
  }

  it should "save a new entity" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      val pair2 = Entity("pair2", "Pair",
        Map(
          "case" -> AttributeReferenceSingle("Sample", "sample3"),
          "control" -> AttributeReferenceSingle("Sample", "sample1")),
        testData.wsName)
      dao.save(testData.workspace.namespace, testData.workspace.name, pair2, txn)
      assert {
        txn.withGraph { graph =>
          graph.getVertices("_entityType", "Pair")
            .filter(v => v.getProperty[String]("_name") == "pair2")
            .headOption.isDefined
        }
      }
    }
  }

  it should "clone all entities from a workspace containing cycles" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      lazy val daoCycles: GraphEntityDAO = new GraphEntityDAO()
      lazy val workspaceDaoOriginal: GraphWorkspaceDAO = new GraphWorkspaceDAO()
      lazy val workspaceDaoClone: GraphWorkspaceDAO = new GraphWorkspaceDAO()

      val workspaceOriginal = Workspace(
        namespace = testData.wsName.namespace + "Original",
        name = testData.wsName.name + "Original",
        createdDate = DateTime.now(),
        createdBy = "Joe Biden",
        Map.empty
      )

      val workspaceClone = Workspace(
        namespace = testData.wsName.namespace + "Clone",
        name = testData.wsName.name + "Clone",
        createdDate = DateTime.now(),
        createdBy = "Joe Biden",
        Map.empty
      )

      val c1 = Entity("c1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle1" -> AttributeReferenceSingle("samples", "c2")), WorkspaceName(testData.workspace.namespace, testData.workspace.name))
      val c2 = Entity("c2", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle2" -> AttributeReferenceSingle("samples", "c3")), WorkspaceName(testData.workspace.namespace, testData.workspace.name))
      var c3 = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3)), WorkspaceName(testData.workspace.namespace, testData.workspace.name))

      workspaceDaoOriginal.save(workspaceOriginal, txn)
      workspaceDaoClone.save(workspaceClone, txn)

      daoCycles.save(workspaceOriginal.namespace, workspaceOriginal.name, c3, txn)
      daoCycles.save(workspaceOriginal.namespace, workspaceOriginal.name, c2, txn)
      daoCycles.save(workspaceOriginal.namespace, workspaceOriginal.name, c1, txn)

      c3 = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle3" -> AttributeReferenceSingle("samples", "c1")), WorkspaceName(testData.workspace.namespace, testData.workspace.name))

      daoCycles.save(workspaceOriginal.namespace, workspaceOriginal.name, c3, txn)
      daoCycles.cloneAllEntities(workspaceOriginal.namespace, workspaceClone.namespace, workspaceOriginal.name, workspaceClone.name, txn)

      assertResult(dao.listEntitiesAllTypes(workspaceOriginal.namespace, workspaceOriginal.name, txn).map(_.copy(workspaceName = WorkspaceName(workspaceClone.namespace, workspaceClone.name))).toSet) {
        dao.listEntitiesAllTypes(workspaceClone.namespace, workspaceClone.name, txn).toSet
      }
    }
  }

  it should "save updates to an existing entity" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      // flip case / control and add property, remove a property
      val pair1Updated = Entity("pair1", "Pair",
        Map(
          "isItAPair" -> AttributeBoolean(true),
          "case" -> AttributeReferenceSingle("Sample", "sample2"),
          "control" -> AttributeReferenceSingle("Sample", "sample1")),
        testData.wsName)
      dao.save(testData.workspace.namespace, testData.workspace.name, pair1Updated, txn)
      txn.withGraph { graph =>
        val fetched = graph.getVertices("_entityType", "Pair").filter(v => v.getProperty[String]("_name") == "pair1").head
        assert {
          fetched.getPropertyKeys.contains("isItAPair")
        }
        // TODO check edges?
      }

      val pair1UpdatedAgain = Entity("pair1", "Pair",
        Map(
          "case" -> AttributeReferenceSingle("Sample", "sample2"),
          "control" -> AttributeReferenceSingle("Sample", "sample1")),
        testData.wsName)
      dao.save(testData.workspace.namespace, testData.workspace.name, pair1UpdatedAgain, txn)
      txn.withGraph { graph =>
        val fetched = graph.getVertices("_entityType", "Pair").filter(v => v.getProperty[String]("_name") == "pair1").head
        assert {
          !fetched.getPropertyKeys.contains("isItAPair")
        }
      }
    }
  }

  it should "throw an exception when trying to save an entity to a nonexistent workspace" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      val foo = Entity("foo", "bar", Map.empty, testData.wsName)
      intercept[IllegalArgumentException] {
        dao.save("fnord", "dronf", foo, txn)
      }
    }
  }

  it should "throw an exception if trying to save reserved-keyword attributes" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      val bar = Entity("boo", "far", Map("_entityType" -> AttributeString("myAmazingClazzzz")), testData.wsName)
      intercept[IllegalArgumentException] {
        dao.save(testData.workspace.namespace, testData.workspace.name, bar, txn)
      }
    }
  }

  it should "throw an exception if trying to save invalid references" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      val baz = Entity("wig", "wug", Map("edgeToNowhere" -> AttributeReferenceSingle("sample", "notTheSampleYoureLookingFor")), testData.wsName)
      intercept[IllegalArgumentException] {
        dao.save(testData.workspace.namespace, testData.workspace.name, baz, txn)
      }
    }
  }

  it should "delete an entity" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      dao.delete(testData.workspace.namespace, testData.workspace.name, "Pair", "pair2", txn)
      assert {
        txn.withGraph { graph =>
          graph.getVertices("_entityType", "Pair")
            .filter(v => v.getProperty[String]("_name") == "pair2")
            .headOption.isEmpty
        }
      }
    }
  }

  it should "list entities" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Set(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7)) {
        dao.list(testData.workspace.namespace, testData.workspace.name, "Sample", txn).toSet
      }
    }
  }

  it should "add cycles to entity graph" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      val sample1Copy = Entity("sample1", "Sample",
        Map(
          "type" -> AttributeString("normal"),
          "whatsit" -> AttributeNumber(100),
          "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
          "aliquot" -> AttributeReferenceSingle("Aliquot", "aliquot1"),
          "cycle" -> AttributeReferenceSingle("SampleSet", "sset1")),
        testData.wsName)
      dao.save(testData.workspace.namespace, testData.workspace.name, sample1Copy, txn)
      val sample5Copy = Entity("sample5", "Sample",
        Map(
          "type" -> AttributeString("tumor"),
          "whatsit" -> AttributeNumber(100),
          "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
          "cycle" -> AttributeReferenceSingle("SampleSet", "sset4")),
        testData.wsName)
      dao.save(testData.workspace.namespace, testData.workspace.name, sample5Copy, txn)
      val sample7Copy = Entity("sample7", "Sample",
        Map(
          "type" -> AttributeString("tumor"),
          "whatsit" -> AttributeNumber(100),
          "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
          "cycle" -> AttributeReferenceSingle("Sample", "sample6")),
        testData.wsName)
      dao.save(testData.workspace.namespace, testData.workspace.name, sample7Copy, txn)
      val sample6Copy = Entity("sample6", "Sample",
        Map(
          "type" -> AttributeString("tumor"),
          "whatsit" -> AttributeNumber(100),
          "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
          "cycle" -> AttributeReferenceSingle("SampleSet", "sset3")),
        testData.wsName)
      dao.save(testData.workspace.namespace, testData.workspace.name, sample6Copy, txn)
      val entitiesWithCycles = List("sample1", "sample5", "sample7", "sample6")
      txn.withGraph { graph =>
        val fetched = graph.getVertices().filter(v => entitiesWithCycles.contains(v.getProperty[String]("_name"))).head.getEdges(Direction.OUT, "cycle")
        assert {
          fetched.size == 1
        }
      }
    }
  }

  it should "rename an entity" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      dao.rename(testData.workspace.namespace, testData.workspace.name, "Pair", "pair1", "amazingPair", txn)
      txn.withGraph { graph =>
        val pairNames = graph.getVertices("_entityType", "Pair").map(_.getProperty[String]("_name")).toList
        assert {
          pairNames.contains("amazingPair")
        }
        assert {
          !pairNames.contains("pair1")
        }
      }
    }
  }

  /* Test case tests for cycles, cycles contained within cycles, cycles existing below other cycles, invalid
   * entity names being supplied, and multiple disjoint subtrees
   */
  it should "get entity subtrees from a list of entities" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Set(testData.sset3, testData.sample1, testData.sset2, testData.aliquot1, testData.sample6, testData.sset1, testData.sample2, testData.sample3, testData.sample5)) {
        dao.getEntitySubtrees(testData.workspace.namespace, testData.workspace.name, "SampleSet", List("sset1", "sset2", "sset3", "sampleSetDOESNTEXIST"), txn).toSet
      }
    }
  }

  val x1 = Entity("x1", "SampleSet", Map.empty, testData.wsName)

  val workspace2 = Workspace(
    namespace = testData.wsName.namespace + "2",
    name = testData.wsName.name + "2",
    createdDate = DateTime.now(),
    createdBy = "Joe Biden",
    Map.empty
  )

  it should "copy entities without a conflict" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      new GraphWorkspaceDAO().save(workspace2, txn)
      dao.save(workspace2.namespace, workspace2.name, x1, txn)

      assertResult(Seq.empty) {
        dao.getCopyConflicts(testData.workspace.namespace, testData.workspace.name, Seq(x1), txn)
      }

      assertResult(Seq.empty) {
        dao.copyEntities(testData.workspace.namespace, testData.workspace.name, workspace2.namespace, workspace2.name, "SampleSet", Seq("x1"), txn)
      }

      //verify it was actually copied into the workspace
      assert(dao.list(testData.workspace.namespace, testData.workspace.name, "SampleSet", txn).toList.contains(x1))
    }
  }

  it should "copy entities with a conflict" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      assertResult(Set(testData.sample1)) {
        dao.getCopyConflicts(testData.workspace.namespace, testData.workspace.name, Seq(testData.sample1), txn).toSet
      }

      assertResult(Set(testData.sample1, testData.aliquot1)) {
        dao.copyEntities(testData.workspace.namespace, testData.workspace.name, testData.workspace.namespace, testData.workspace.name, "Sample", Seq("sample1"), txn).toSet
      }

      //verify that it wasn't copied into the workspace again
      assert(dao.list(testData.workspace.namespace, testData.workspace.name, "Sample", txn).toList.filter(entity => entity == testData.sample1).size == 1)
    }
  }
}